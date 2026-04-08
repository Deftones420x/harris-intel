"""
Harris County Address Enrichment Engine
Multi-strategy lookup with confidence scoring.

Strategy priority:
1. Legal description match (subdivision + lot + block) - HIGH confidence
2. Doc-type-aware name match (grantee for LP, grantor for others) - MEDIUM/LOW
3. Fuzzy normalized name match - LOW
4. No match - output HCAD lookup URL for manual verification
"""

import re
import logging
import urllib.parse
from typing import Optional

log = logging.getLogger(__name__)


def parse_legal_description(legal: str) -> dict:
    """
    Parse clerk legal description into components.
    Examples:
      "Desc: FOXWOOD Sec: 4 Lot: 15 Block: 4"
      "Desc: SUNDOWN GLEN Sec: 6 Lot: 23 Block: 6"
    """
    result = {"subdivision": "", "section": "", "lot": "", "block": "",
              "abstract": "", "tract": ""}
    if not legal:
        return result

    legal = legal.upper().strip()

    m = re.search(r'DESC[:\s]+([A-Z0-9 &\-\\']+?)(?:\s+SEC[:\s]|\s+LOT[:\s]|\s+BLK[:\s]|\s+BLOCK[:\s]|\s+TR[:\s]|\s+ABST[:\s]|$)', legal)
    if m:
        result["subdivision"] = m.group(1).strip()

    for key, pattern in [
        ("section", r'SEC(?:TION)?[:\s]+(\w+)'),
        ("lot",     r'LOT[:\s]+(\w+)'),
        ("block",   r'(?:BLK|BLOCK)[:\s]+(\w+)'),
        ("abstract",r'ABST(?:RACT)?[:\s]+(\w+)'),
        ("tract",   r'(?:TR|TRACT)[:\s]+(\w+)'),
    ]:
        m2 = re.search(pattern, legal)
        if m2:
            result[key] = m2.group(1).strip()

    return result


def legal_match_key(parsed: dict) -> Optional[str]:
    sub = parsed.get("subdivision", "").strip()
    sec = parsed.get("section", "").strip()
    lot = parsed.get("lot", "").strip()
    blk = parsed.get("block", "").strip()
    if sub and lot and blk:
        return f"{sub}|{sec}|{lot}|{blk}"
    if sub and lot:
        return f"{sub}||{lot}|"
    return None


STRIP_SUFFIXES = [
    r'\bINC\\.?\b', r'\bINCORPORATED\b',
    r'\bLLC\\.?\b', r'\bL\\.L\\.C\\.?\b',
    r'\bLP\\.?\b',  r'\bL\\.P\\.?\b',
    r'\bLTD\\.?\b', r'\bLIMITED\b',
    r'\bCORP\\.?\b', r'\bCORPORATION\b',
    r'\bTRUST\b', r'\bTRUSTEE\b',
    r'\bATTN\\.?\b', r'\bC/O\b',
    r'\bETAL\b', r'\bET AL\b', r'\bET UX\b',
    r'\bJR\\.?\b', r'\bSR\\.?\b',
    r'\bASSOC(?:IATION)?\b',
    r'\bCOMMUNITY\b', r'\bHOMEOWNERS?\b',
]

STRIP_RE = re.compile('|'.join(STRIP_SUFFIXES), re.IGNORECASE)


def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.upper().strip()
    n = re.sub(r"[',\\.]", " ", n)
    n = STRIP_RE.sub(" ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def name_variants(name: str) -> list:
    n = normalize_name(name)
    if not n:
        return []
    parts = n.split()
    variants = {n}
    if len(parts) >= 2:
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
    return list(variants)


def get_match_targets(rec: dict) -> list:
    cat     = rec.get("cat", "")
    grantor = (rec.get("owner") or rec.get("grantor") or "").strip()
    grantee = (rec.get("grantee") or "").strip()

    if cat == "lp":
        return [t for t in [grantee, grantor] if t]
    elif cat in ("jud", "lien", "tax"):
        return [t for t in [grantor, grantee] if t]
    elif cat == "fc":
        return [t for t in [grantee, grantor] if t]
    else:
        return [t for t in [grantor, grantee] if t]


class EnrichmentEngine:
    def __init__(self):
        self.legal_lookup = {}
        self.name_lookup  = {}
        self.acct_lookup  = {}

    def _add(self, lookup, key, parcel):
        if not key:
            return
        if key not in lookup:
            lookup[key] = []
        lookup[key].append(parcel)

    def build_from_hcad(self, owners_records: list, acct_map: dict,
                         parcel_tieback: dict = None):
        log.info("Building enrichment indexes...")
        for rec in owners_records:
            acct = rec.get("acct", "").strip()
            name = rec.get("name", "").strip()
            aka  = rec.get("aka", "").strip()
            if not acct:
                continue
            parcel = acct_map.get(acct)
            if not parcel:
                continue

            self.acct_lookup[acct] = parcel

            if parcel_tieback and acct in parcel_tieback:
                pt  = parcel_tieback[acct]
                key = legal_match_key(pt)
                self._add(self.legal_lookup, key, parcel)

            for raw in [name, aka]:
                for v in name_variants(raw):
                    if v and len(v) > 2:
                        self._add(self.name_lookup, v, parcel)

        log.info(f"  Name index:  {len(self.name_lookup)} keys")
        log.info(f"  Legal index: {len(self.legal_lookup)} keys")
        log.info(f"  Acct index:  {len(self.acct_lookup)} keys")

    def build_parcel_tieback(self, parcel_tieback_records: list) -> dict:
        """
        Parse parcel_tieback.txt records into legal description lookup.
        Returns acct -> {subdivision, section, lot, block}
        """
        result = {}
        for rec in parcel_tieback_records:
            r    = {k.strip().lower(): str(v).strip() if v else ""
                    for k, v in rec.items()}
            acct = r.get("acct", "").strip()
            if not acct:
                continue
            result[acct] = {
                "subdivision": r.get("subdv_cd", "") or r.get("subdivision", ""),
                "section":     r.get("section",  "") or r.get("sec", ""),
                "lot":         r.get("lot",       "") or r.get("lot_nbr", ""),
                "block":       r.get("block",     "") or r.get("block_nbr", ""),
            }
        return result

    def enrich(self, rec: dict) -> dict:
        legal_raw   = rec.get("legal", "")
        search_name = (rec.get("grantee") if rec.get("cat") == "lp"
                       else rec.get("owner") or "")
        rec["hcad_url"] = self._hcad_url(search_name)

        # Strategy 1: Legal description match
        parsed    = parse_legal_description(legal_raw)
        legal_key = legal_match_key(parsed)

        if legal_key and legal_key in self.legal_lookup:
            matches = self.legal_lookup[legal_key]
            if len(matches) == 1:
                return self._apply(rec, matches[0], "HIGH",
                                   "Legal desc — unique")
            # Multiple — try to narrow with name
            targets = get_match_targets(rec)
            for target in targets:
                for v in name_variants(target):
                    for m in matches:
                        if v and v in m.get("_owner_norm", ""):
                            return self._apply(rec, m, "HIGH",
                                               "Legal + name — unique")
            return self._apply(rec, matches[0], "MEDIUM",
                               f"Legal desc — {len(matches)} parcels, took first")

        # Strategy 2: Name match
        targets = get_match_targets(rec)
        for target in targets:
            for v in name_variants(target):
                if not v or len(v) < 3:
                    continue
                if v in self.name_lookup:
                    matches = self.name_lookup[v]
                    conf    = "MEDIUM" if len(matches) == 1 else "LOW"
                    reason  = (f"Name — unique: {v}" if len(matches) == 1
                               else f"Name — {len(matches)} duplicates: {v}")
                    return self._apply(rec, matches[0], conf, reason)

        # No match
        rec.update({
            "match_confidence": "NONE",
            "match_reason":     "No match found",
            "prop_address": "", "prop_city": "",
            "prop_state":  "TX", "prop_zip": "",
            "mail_address": "", "mail_city": "",
            "mail_state":  "TX", "mail_zip": "",
        })
        return rec

    def _apply(self, rec, parcel, confidence, reason):
        rec["match_confidence"] = confidence
        rec["match_reason"]     = reason
        rec["prop_address"]     = parcel.get("site_addr", "")
        rec["prop_city"]        = parcel.get("site_city", "Houston")
        rec["prop_state"]       = parcel.get("site_state", "TX")
        rec["prop_zip"]         = parcel.get("site_zip", "")
        rec["mail_address"]     = parcel.get("mail_addr", "")
        rec["mail_city"]        = parcel.get("mail_city", "")
        rec["mail_state"]       = parcel.get("mail_state", "TX")
        rec["mail_zip"]         = parcel.get("mail_zip", "")
        if not rec["mail_address"] and rec["prop_address"]:
            rec["mail_address"] = rec["prop_address"]
            rec["mail_city"]    = rec["prop_city"]
            rec["mail_state"]   = rec["prop_state"]
            rec["mail_zip"]     = rec["prop_zip"]
        return rec

    def _hcad_url(self, name: str) -> str:
        base = "https://hcad.org/property-search/real-property/search-by-owner-name/"
        if name:
            return base + "?name=" + urllib.parse.quote(str(name)[:50])
        return base
