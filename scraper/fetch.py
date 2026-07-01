"""
Harris County Motivated Seller Lead Scraper - Final Production Version
- Scrapes RP.aspx via Playwright using confirmed exact field IDs
- Parses results table using RP-YYYY-NNNNN file number pattern
- Enriches with HCAD owners.txt + real_acct.txt from Google Drive
- Exports records.json + GHL CSV
"""

import asyncio
import json
import csv
import io
import os
import re
import sys
import time
import zipfile
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from enrichment import EnrichmentEngine
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
except ImportError:
    DBF = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

CLERK_URL       = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"
GDRIVE_ZIP_ID   = "1wV4EW-uxasZUjkc_wxOr-UjVKUTK5-qh"  # Real_Account_Owner.zip
FILE_NUMBER_RE  = re.compile(r'^RP-\d{4}-\d+$')

# Harris County actual instrument codes from Codes.aspx
DOC_TYPES = {
    "L/P":    ("lp",      "Lis Pendens"),
    "NOTICE": ("fc",      "Notice of Foreclosure / Trustee Sale"),
    "TRSALE": ("fc",      "Trustee Sale"),
    "LIEN":   ("lien",    "Lien"),
    "T/L":    ("lien",    "Federal Tax Lien"),
    "JUDGE":  ("jud",     "Judgment"),
    "A/J":    ("jud",     "Abstract of Judgment"),
    "PROB":   ("probate", "Probate"),
    "DEED":   ("tax",     "Deed (Sheriff/Trustee/Tax)"),
    "BNKRCY": ("lien",    "Bankruptcy"),
    "LEVY":   ("lien",    "Notice of Levy"),
    "REL":    ("lp",      "Release"),
}

# ─── Scoring model ────────────────────────────────────────────────────────────
# Weighted motivation model. Each distress signal contributes points that reflect
# how strongly it predicts a motivated *individual* seller. Institutional owners
# (LLC/corp/HOA) are penalized because they rarely sell under duress. The old
# model gave a flat +10 per flag and treated "LLC/corp owner" as positive, which
# pushed 80% of leads to "Hot" and made the tiers meaningless.
DISTRESS_POINTS = {
    "Foreclosure auction scheduled": 32,
    "Pre-foreclosure":               32,
    "Lis pendens":                   28,
    "Tax lien":                      25,
    "Probate / estate":              25,
    "Bankruptcy":                    18,
    "Judgment lien":                 18,
    "Mechanic lien":                 12,
}
SCORE_ABSENTEE_OWNER = 12   # mailing address != property address (out-of-area owner)
SCORE_HAS_ADDRESS    = 10   # actionable/skip-traceable — a lead we can't reach is worth little
SCORE_HAS_OWNER      = 8    # a named individual owner (not blank, not institutional)
SCORE_NEW_WEEK       = 8    # freshly filed within the lookback window
SCORE_AMOUNT_100K    = 12
SCORE_AMOUNT_50K     = 6
SCORE_STACK_PER_SRC  = 18   # same property appears on N distress lists (N-1 extra * this)
SCORE_STACK_MAX      = 36
PENALTY_INSTITUTIONAL = -25  # LLC / corp / HOA / government owner — not a motivated seller

SCORE_HOT  = 70   # tier thresholds (unchanged, but now meaningful)
SCORE_WARM = 50
SCORE_ACTIVE = 30

# Bug 1: foreclosure-notice doc types scraped over a wider window so upcoming
# trustee sales are captured with grantor names (then HCAD supplies addresses).
FC_NOTICE_CODES         = {"NOTICE", "TRSALE"}
FC_NOTICE_LOOKBACK_DAYS = 45

# Bug 5: single source of truth. The dashboard/ copy is what GitHub Pages
# serves; the old data/ mirror and the daily GHL CSVs were pure repo bloat
# (the dashboard exports GHL client-side on demand).
OUTPUT_DIRS = [Path("dashboard")]


# ─── HCAD Parcel Lookup ───────────────────────────────────────────────────────
class HCADParcelLookup:
    """
    Builds two lookups from HCAD bulk data:
    1. acct_to_addr: acct# → {site_addr, mail_addr, ...} from real_acct.txt
    2. name_to_acct: normalized owner name → acct# from owners.txt
    
    owners.txt columns (tab-delimited):
        acct | ln_num | name | aka | pct_own
    
    real_acct.txt columns (tab-delimited):
        acct | yr | mailto_addr | mail_addr_2 | mail_city | mail_state | 
        mail_zip | mail_country | ... | site_addr | ...
    """

    def __init__(self):
        self.name_lookup: dict = {}   # normalized name → parcel dict
        self.acct_lookup: dict = {}   # acct# → parcel dict

    def _normalize(self, name: str) -> str:
        return re.sub(r"\s+", " ", (name or "").strip().upper())

    def _name_variants(self, name: str) -> list:
        n = self._normalize(name)
        parts = n.split()
        variants = [n]
        if len(parts) >= 2:
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return variants

    def _download_gdrive(self) -> Optional[bytes]:
        """Download zip from Google Drive handling large file warning."""
        log.info("Downloading HCAD zip from Google Drive...")
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        # Try usercontent direct download with confirm=t
        try:
            url = (f"https://drive.usercontent.google.com/download"
                   f"?id={GDRIVE_ZIP_ID}&export=download&confirm=t")
            r = session.get(url, timeout=300)
            if r.status_code == 200 and r.content[:2] == b'PK':
                log.info(f"  ✓ {len(r.content)/1e6:.1f} MB downloaded")
                return r.content
        except Exception as e:
            log.warning(f"  usercontent failed: {e}")

        # Fallback: get page, extract uuid, confirm
        try:
            r = session.get(
                f"https://drive.google.com/uc?export=download&id={GDRIVE_ZIP_ID}",
                timeout=60)
            m = re.search(r'name="uuid" value="([^"]+)"', r.text)
            if m:
                uuid = m.group(1)
                r2 = session.get(
                    f"https://drive.usercontent.google.com/download"
                    f"?id={GDRIVE_ZIP_ID}&export=download&confirm=t&uuid={uuid}",
                    timeout=300)
                if r2.status_code == 200 and r2.content[:2] == b'PK':
                    log.info(f"  ✓ {len(r2.content)/1e6:.1f} MB (uuid confirm)")
                    return r2.content
        except Exception as e:
            log.warning(f"  uuid confirm failed: {e}")

        log.warning("  Could not download from Google Drive")
        return None

    def _parse_txt_from_zip(self, zip_bytes: bytes,
                             filename_hint: str) -> Optional[io.StringIO]:
        """Extract a specific txt file from zip by name hint."""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                names = z.namelist()
                log.info(f"  Zip contents: {names}")
                match = next(
                    (n for n in names
                     if filename_hint.lower() in n.lower()), None)
                if not match:
                    # Try any txt file
                    match = next(
                        (n for n in names
                         if n.lower().endswith(".txt")), None)
                if match:
                    log.info(f"  Reading: {match}")
                    with z.open(match) as f:
                        return io.StringIO(
                            f.read().decode("latin-1", errors="replace"))
        except Exception as e:
            log.warning(f"  Zip read error: {e}")
        return None

    def _load_real_acct(self, zip_bytes: bytes) -> dict:
        """
        Parse real_acct.txt → acct_to_parcel dict.
        Tab-delimited. Columns confirmed from screenshot:
        acct, yr, mailto_addr, mail_addr_2, mail_city, mail_state, mail_zip,
        mail_country, ...many cols..., site_addr, ...
        """
        acct_map = {}
        sio = self._parse_txt_from_zip(zip_bytes, "real_acct")
        if not sio:
            return acct_map

        reader = csv.DictReader(sio, delimiter="\t")
        # Normalize fieldnames
        raw_fields = reader.fieldnames or []
        fields = [f.strip().lower() for f in raw_fields]
        log.info(f"  real_acct fields: {fields[:20]}")

        count = 0
        for row in reader:
            try:
                r = {k.strip().lower(): (v or "").strip()
                     for k, v in row.items() if k}
                acct = r.get("acct", "").strip()
                if not acct:
                    continue

                # Mailing address - confirmed field names from log
                mail_addr  = r.get("mail_addr_1") or r.get("mailto") or ""
                mail_city  = r.get("mail_city", "")
                mail_state = r.get("mail_state", "TX")
                mail_zip   = r.get("mail_zip", "")

                # Site/property address - build from components
                # site_addr_1 = street number + name, site_addr_2 = city, site_addr_3 = zip
                site_addr  = r.get("site_addr_1", "").strip()
                site_city  = r.get("site_addr_2", "") or "HOUSTON"
                site_state = "TX"
                site_zip   = r.get("site_addr_3", "")
                
                # Filter out "0" addresses (no street number)
                if site_addr and site_addr.startswith("0 "):
                    # Build from street components instead
                    num = r.get("str_num", "").strip()
                    if num and num != "0":
                        parts = [num, r.get("str_pfx",""), r.get("str",""),
                                 r.get("str_sfx",""), r.get("str_sfx_dir",""),
                                 r.get("str_unit","")]
                        site_addr = " ".join(p for p in parts if p).strip()
                    else:
                        site_addr = ""
                elif not site_addr:
                    num = r.get("str_num", "").strip()
                    if num and num != "0":
                        parts = [num, r.get("str_pfx",""), r.get("str",""),
                                 r.get("str_sfx",""), r.get("str_sfx_dir",""),
                                 r.get("str_unit","")]
                        site_addr = " ".join(p for p in parts if p).strip()
                if not site_addr:
                    parts = [
                        r.get("str_num",""), r.get("str_pfx",""),
                        r.get("str",""), r.get("str_sfx",""),
                        r.get("str_sfx_dir",""), r.get("str_unit","")
                    ]
                    site_addr = " ".join(p for p in parts if p).strip()

                acct_map[acct] = {
                    "mail_addr":  mail_addr,
                    "mail_city":  mail_city,
                    "mail_state": mail_state,
                    "mail_zip":   mail_zip,
                    "site_addr":  site_addr,
                    "site_city":  site_city,
                    "site_state": site_state,
                    "site_zip":   site_zip,
                }
                count += 1
            except Exception:
                continue

        log.info(f"  Loaded {count} real_acct records")
        return acct_map

    def _load_owners(self, zip_bytes: bytes, acct_map: dict):
        """
        Parse owners.txt → build name_lookup.
        Tab-delimited. Columns confirmed from screenshot:
        acct | ln_num | name | aka | pct_own
        """
        sio = self._parse_txt_from_zip(zip_bytes, "owner")
        if not sio:
            return

        reader = csv.DictReader(sio, delimiter="\t")
        raw_fields = reader.fieldnames or []
        fields = [f.strip().lower() for f in raw_fields]
        log.info(f"  owners fields: {fields[:10]}")

        count = 0
        for row in reader:
            try:
                r = {k.strip().lower(): (v or "").strip()
                     for k, v in row.items() if k}
                acct = r.get("acct", "").strip()
                name = r.get("name", "").strip()
                aka  = r.get("aka", "").strip()

                if not acct or not name:
                    continue

                parcel = acct_map.get(acct)
                if not parcel:
                    continue

                # Build name lookup
                for n in [name, aka]:
                    if n:
                        for variant in self._name_variants(n):
                            self.name_lookup[variant] = parcel

                self.acct_lookup[acct] = parcel
                count += 1
            except Exception:
                continue

        log.info(f"  Loaded {count} owner records → "
                 f"{len(self.name_lookup)} name keys")

    def load(self):
        zip_bytes = self._download_gdrive()
        if not zip_bytes:
            log.warning("HCAD unavailable — no address enrichment")
            return

        log.info("Parsing HCAD data files...")
        acct_map = self._load_real_acct(zip_bytes)
        if not acct_map:
            log.warning("  real_acct parse returned 0 records")
            return

        # Parse parcel_tieback for legal description matching
        parcel_tieback = self._load_parcel_tieback(zip_bytes)

        # Build enrichment engine
        self.engine = EnrichmentEngine()
        owners = self._get_owners_raw(zip_bytes)
        self.engine.build_from_hcad(owners, acct_map, parcel_tieback)
        self._load_owners(zip_bytes, acct_map)
    def _load_parcel_tieback(self, zip_bytes: bytes) -> dict:
        """
        Parse parcel_tieback.txt for legal description matching.
        Confirmed columns: acct | tp | dscr | related_acct | pct
        The dscr field contains text like:
          "FOXWOOD SEC 4 LT 15 BLK 4"
          "SUNDOWN GLEN SEC 6 LT 23 BLK 6"
        We parse this using the same legal description parser.
        """
        from enrichment import parse_legal_description, legal_match_key
        result = {}
        sio = self._parse_txt_from_zip(zip_bytes, "parcel_tieback")
        if not sio:
            return result
        reader = csv.DictReader(sio, delimiter="\t")
        fields = [f.strip().lower() for f in (reader.fieldnames or [])]
        log.info(f"  parcel_tieback fields: {fields[:10]}")
        parsed_count = 0
        for row in reader:
            try:
                r    = {k.strip().lower(): (v or "").strip()
                        for k, v in row.items() if k}
                acct = r.get("acct", "").strip()
                dscr = r.get("dscr", "").strip()
                if not acct or not dscr:
                    continue
                # Parse the dscr text field into components
                # dscr format: "FOXWOOD SEC 4 LT 15 BLK 4"
                # Need to normalize to match clerk format
                dscr_normalized = dscr.upper()
                # Replace common abbreviations to match our parser
                dscr_normalized = dscr_normalized.replace(" LT ", " LOT ").replace(" LTS ", " LOT ")
                dscr_normalized = dscr_normalized.replace(" BLK ", " BLOCK ").replace(" BK ", " BLOCK ")
                # Add "Desc:" prefix so our parser picks it up
                # Log first 5 dscr values to understand format
                if parsed_count == 0 and len(result) < 10:
                    log.info(f"  dscr sample: {repr(dscr)}")
                dscr_for_parse = "Desc: " + dscr_normalized
                parsed = parse_legal_description(dscr_for_parse)
                key = legal_match_key(parsed)
                if key:
                    result[acct] = parsed
                    parsed_count += 1
                else:
                    # Store raw dscr for fallback matching
                    result[acct] = {
                        "subdivision": dscr_normalized.split()[0] if dscr_normalized else "",
                        "section": "", "lot": "", "block": "",
                        "dscr_raw": dscr_normalized
                    }
            except Exception:
                continue
        log.info(f"  Parcel tieback: {len(result)} records, {parsed_count} with full legal key")
        return result

    def _get_owners_raw(self, zip_bytes: bytes) -> list:
        records = []
        sio = self._parse_txt_from_zip(zip_bytes, "owner")
        if not sio:
            return records
        reader = csv.DictReader(sio, delimiter="\t")
        for row in reader:
            records.append(dict(row))
        return records

    def find(self, name: str) -> Optional[dict]:
        if not name:
            return None
        for v in self._name_variants(name):
            if v in self.name_lookup:
                return self.name_lookup[v]
        return None


# ─── Scoring ──────────────────────────────────────────────────────────────────
INSTITUTIONAL_TOKENS = (
    "LLC", "L L C", "INC", "CORP", "LTD", " LP", "L P", "TRUST", "TRUSTEE",
    "ASSOC", "ASSN", "HOMEOWNER", "COMMUNITY", "PARTNERS", "PROPERTIES",
    "HOLDINGS", "CAPITAL", "INVESTMENT", "REALTY", "BANK", "MORTGAGE",
    "FEDERAL", "NATIONAL", "COMPANY", " CO ", "FUND", "GROUP", "VENTURES",
    # Government / public plaintiffs — these appear as grantor on tax & judgment
    # filings but are never the motivated seller.
    "COUNTY", "CITY OF", "STATE OF", "DISTRICT", "AUTHORITY", "MUNICIPAL",
    " ISD", "UNITED STATES", " USA", "DEPARTMENT", "COMMISSION", "UNIVERSITY",
)


def is_institutional(name: str) -> bool:
    """True when the owner looks like a company/HOA/trust rather than a person."""
    n = f" {(name or '').upper().strip()} "
    return any(tok in n for tok in INSTITUTIONAL_TOKENS)


def is_new(rec: dict, today: datetime, window_days: int = 7) -> bool:
    """Filed within the last `window_days`. Future dates (foreclosure sale
    dates) are NOT new — the old code flagged them because (today - future) < 0."""
    filed = (rec.get("filed") or "")[:10]
    try:
        dt = datetime.strptime(filed, "%Y-%m-%d")
    except Exception:
        return False
    delta = (today - dt).days
    return 0 <= delta <= window_days


def build_flags(rec: dict, today: datetime) -> list:
    """Human-readable badges. Distress badges must match DISTRESS_POINTS keys."""
    flags = []
    try:
        cat      = rec.get("cat", "")
        doc_type = (rec.get("doc_type") or "").upper()
        owner    = (rec.get("owner") or "")

        if cat == "lp" and "REL" not in doc_type:
            flags.append("Lis pendens")
        if cat == "fc":
            if rec.get("source") == "FRCL" or "SALE" in doc_type:
                flags.append("Foreclosure auction scheduled")
            else:
                flags.append("Pre-foreclosure")
        if cat == "jud":
            flags.append("Judgment lien")
        if doc_type in ("T/L", "LEVY") or "TAX" in doc_type:
            flags.append("Tax lien")
        if cat == "lien" and doc_type == "LIEN":
            flags.append("Mechanic lien")
        if "BNKR" in doc_type or "BANKR" in doc_type:
            flags.append("Bankruptcy")
        if cat == "probate":
            flags.append("Probate / estate")

        # Owner-type signal (affects score in compute_score, shown for context)
        if is_institutional(owner):
            flags.append("Institutional owner")

        # Absentee: owner mails somewhere other than the property they own.
        prop = (rec.get("prop_address") or "").strip().upper()
        mail = (rec.get("mail_address") or "").strip().upper()
        if prop and mail and prop != mail and not is_institutional(owner):
            flags.append("Absentee owner")

        if is_new(rec, today):
            flags.append("New this week")
    except Exception:
        pass
    return flags


def compute_score(rec: dict, flags: list) -> int:
    """Weighted motivation score, 0-100. See DISTRESS_POINTS for rationale.
    Also writes rec['score_breakdown'] for transparency in the dashboard."""
    try:
        breakdown = {}

        # Distress signals — take the single strongest plus partial credit for
        # additional distinct signals, so genuinely stacked distress ranks high
        # without every extra badge blindly adding a flat amount.
        distress = [(f, DISTRESS_POINTS[f]) for f in flags if f in DISTRESS_POINTS]
        distress.sort(key=lambda x: x[1], reverse=True)
        for i, (name, pts) in enumerate(distress):
            add = pts if i == 0 else round(pts * 0.5)
            if add:
                breakdown[name] = add

        if "Absentee owner" in flags:
            breakdown["Absentee owner"] = SCORE_ABSENTEE_OWNER
        if "New this week" in flags:
            breakdown["New this week"] = SCORE_NEW_WEEK
        if rec.get("prop_address"):
            breakdown["Has address"] = SCORE_HAS_ADDRESS
        owner = (rec.get("owner") or "").strip()
        if owner and "Institutional owner" not in flags:
            breakdown["Named owner"] = SCORE_HAS_OWNER

        amount = 0.0
        try:
            amount = float(str(rec.get("amount") or "0")
                           .replace(",", "").replace("$", ""))
        except Exception:
            pass
        if amount >= 100_000:
            breakdown["Debt ≥ $100k"] = SCORE_AMOUNT_100K
        elif amount >= 50_000:
            breakdown["Debt ≥ $50k"] = SCORE_AMOUNT_50K

        # Stacking: same property on multiple distress lists (set in enrich_records)
        extra_sources = max(0, int(rec.get("stack_count", 1)) - 1)
        if extra_sources:
            breakdown["Stacked lists"] = min(
                extra_sources * SCORE_STACK_PER_SRC, SCORE_STACK_MAX)

        # Penalty: institutional owners are rarely motivated sellers
        if "Institutional owner" in flags:
            breakdown["Institutional owner"] = PENALTY_INSTITUTIONAL

        score = sum(breakdown.values())
        rec["score_breakdown"] = breakdown
        return max(0, min(score, 100))
    except Exception:
        return 0


# ─── Harris County Clerk Scraper ──────────────────────────────────────────────
class HarrisClerkScraper:
    """
    Playwright scraper for RP.aspx.
    
    Confirmed from debug logs:
    - Field IDs: ctl00_ContentPlaceHolder1_txtInstrument / txtFrom / txtTo
    - Results: 137 tables per page, main table has header row with
      "File Number | File Date | Type Vol Page | Names | Legal | Pgs | Film Code"
    - File numbers match pattern: RP-YYYY-NNNNN
    - Names cell has lines: "Grantor : NAME" and "Grantee : NAME"
    """

    BASE_URL = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back  = days_back
        self.today      = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.records: list = []
        # Per-source row counts + page counts for the sanity gate (Bug 4).
        self.per_source: dict = {}   # doc_code -> row count
        self.page_counts: dict = {}  # doc_code -> pages fetched

    async def _wait_overlay_hidden(self, page, timeout: int = 20000):
        """ASP.NET UpdatePanel renders a loading overlay (div#overlay) that
        intercepts clicks between pages and causes 'Element is not attached to
        the DOM' errors — which silently stopped pagination at page 1. Wait for
        it to reach the hidden state before interacting."""
        for sel in ("#overlay", "div#overlay", "#ctl00_UpdateProgress1",
                    ".updateProgress", ".overlay"):
            try:
                el = await page.query_selector(sel)
                if el:
                    await page.wait_for_selector(sel, state="hidden",
                                                 timeout=timeout)
            except Exception:
                pass

    def _parse_results_page(self, html: str, doc_code: str,
                            cat: str, cat_label: str) -> list:
        soup = BeautifulSoup(html, "lxml")
        rows_out = []

        # Harris County renders 137+ tables per page.
        # Strategy: find ALL tds whose text matches RP-YYYY-NNNNN,
        # then walk up to the parent tr and extract sibling cells.
        # This is more robust than finding the "header table" which
        # may not be detected correctly.
        
        # First try: find header table approach
        target_table = None
        for tbl in soup.find_all("table"):
            txt = tbl.get_text(" ", strip=True)
            if ("File Number" in txt and "File Date" in txt
                    and FILE_NUMBER_RE.search(txt)):
                target_table = tbl
                break

        if target_table:
            all_trs = target_table.find_all("tr")
        else:
            # Fallback: collect all trs from the entire page
            all_trs = soup.find_all("tr")

        for tr in all_trs:
            try:
                cells = tr.find_all("td")
                if len(cells) < 5:
                    continue

                # Find file number — debug showed it can be at offset 1 (empty first cell)
                # e.g. Row cells: ['', 'RP-2026-130137', '04/07/2026', 'L/P', ...]
                file_num = ''
                file_idx = -1
                for _ci, _cell in enumerate(cells):
                    _txt = _cell.get_text(strip=True)
                    if FILE_NUMBER_RE.match(_txt):
                        file_num = _txt
                        file_idx = _ci
                        break

                if not file_num or file_idx < 0:
                    continue

                def _gc(offset):
                    idx = file_idx + offset
                    return cells[idx] if idx < len(cells) else None

                # Cell +1: File Date
                raw_date = _gc(1).get_text(strip=True) if _gc(1) else ''
                filed_iso = ''
                for fmt in ('%m/%d/%Y', '%Y-%m-%d'):
                    try:
                        filed_iso = datetime.strptime(
                            raw_date[:10], fmt).strftime('%Y-%m-%d')
                        break
                    except Exception:
                        pass

                # Cell +2: Instrument type
                type_cell = _gc(2)
                type_link = type_cell.find('a') if type_cell else None
                rec_type  = type_link.get_text(strip=True) if type_link else doc_code

                # Cell +3: Names — names are in nested sub-tables inside this cell
                # Structure: outer td contains inner tables with Grantor/Grantee rows
                name_cell = _gc(3)
                grantor   = ''
                grantees  = []
                if name_cell:
                    # Get all text, including from nested tables
                    full_text = name_cell.get_text(' ', strip=True)
                    
                    # Try nested table approach first — each row has label + name
                    inner_rows = name_cell.find_all('tr')
                    for irow in inner_rows:
                        icells = irow.find_all('td')
                        if len(icells) >= 2:
                            label = icells[0].get_text(strip=True).lower()
                            name  = icells[1].get_text(strip=True)
                            if 'grantor' in label and name:
                                grantor = name
                            elif 'grantee' in label and name:
                                grantees.append(name)
                    
                    # Fallback: parse flat text
                    # Debug showed: "Grantor:MELCHOR GRACIELAGrantee:TREVINO"
                    # Split on Grantor/Grantee keywords
                    if not grantor:
                        # Insert newlines before keywords
                        tagged = re.sub(r'(Grantor|Grantee)', r'\n\1', full_text, flags=re.IGNORECASE)
                        for line2 in tagged.split('\n'):
                            line2 = line2.strip()
                            if not line2:
                                continue
                            low2 = line2.lower()
                            if low2.startswith('grantor'):
                                val = re.sub(r'^grantor\s*[:\s]\s*', '', line2, flags=re.IGNORECASE).strip()
                                if val:
                                    grantor = val
                            elif low2.startswith('grantee'):
                                val = re.sub(r'^grantee\s*[:\s]\s*', '', line2, flags=re.IGNORECASE).strip()
                                if val:
                                    grantees.append(val)
                # Cell +4: Legal description
                legal_cell = _gc(4)
                legal_raw_text = legal_cell.get_text(' ', strip=True) if legal_cell else ''
                # Clean up legal desc — strip any leaked Grantor/Grantee text
                import re as _re
                legal = _re.sub(r'^(Grantor|Grantee)\s*[:\s]\s*', '', legal_raw_text, flags=_re.IGNORECASE).strip()

                # Last cell: Film Code link
                clerk_url = self.BASE_URL
                a = cells[-1].find('a', href=True)

                if a:
                    href = a.get("href", "")
                    clerk_url = (href if href.startswith("http")
                                 else "https://www.cclerk.hctx.net" + href)

                rows_out.append({
                    "doc_num":   file_num,
                    "doc_type":  rec_type or doc_code,
                    "filed":     filed_iso or raw_date,
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     grantor,
                    "grantee":   ", ".join(grantees),
                    "contact":   (grantees[0] if (cat == "lp" and grantees) else grantor),
                    "amount":    None,
                    "legal":     legal,
                    "clerk_url": clerk_url,
                })

            except Exception as e:
                log.warning(f"Row parse error: {e}")
                continue

        return rows_out

    async def _search_one(self, page, doc_code: str,
                          cat: str, cat_label: str,
                          lookback_days: int = None):
        log.info(f"  → {doc_code} ({cat_label})")

        for attempt in range(3):
            try:
                await page.goto(self.BASE_URL,
                                wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)
                break
            except PWTimeout:
                log.warning(f"    Timeout (attempt {attempt+1})")
                if attempt == 2:
                    return

        try:
            # BUG 1: foreclosure notices (NOTICE/TRSALE) get a wider lookback so
            # we capture upcoming trustee sales while the notice is fresh. TX
            # Property Code §51.002 requires the notice filed >=21 days before
            # the sale, so ~45 days reliably covers the next auction cycle.
            start = (self.today - timedelta(days=lookback_days)
                     if lookback_days else self.start_date)
            date_from = start.strftime("%m/%d/%Y")
            date_to   = self.today.strftime("%m/%d/%Y")

            # Confirmed exact field IDs from debug log
            await page.fill(
                '#ctl00_ContentPlaceHolder1_txtInstrument', doc_code)
            await page.fill(
                '#ctl00_ContentPlaceHolder1_txtFrom', date_from)
            await page.fill(
                '#ctl00_ContentPlaceHolder1_txtTo', date_to)

            log.info(f"    Filled: {doc_code} | {date_from} → {date_to}")

            await page.click('input[value="Search"]')
            await page.wait_for_load_state("domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)

            # BUG 4 FIX: paginate through ALL result pages. The old loop clicked
            # NEXT then waited only for 'domcontentloaded' — but the results grid
            # updates via an ASP.NET UpdatePanel (XHR postback), not a full nav,
            # so it frequently read stale HTML or clicked a detached NEXT and
            # silently stopped at page 1 (the exactly-200-rows signature). We now
            # wait for the loading overlay to hide and confirm the page's first
            # document number actually changed before parsing the next page.
            page_num   = 0
            row_count  = 0
            seen_first = set()
            MAX_PAGES  = 80
            while page_num < MAX_PAGES:
                page_num += 1
                await self._wait_overlay_hidden(page)
                html = await page.content()
                rows = self._parse_results_page(html, doc_code, cat, cat_label)
                first_doc = rows[0]["doc_num"] if rows else None
                log.info(f"    Page {page_num}: {len(rows)} rows"
                         f" (first={first_doc})")
                self.records.extend(rows)
                row_count += len(rows)

                # Stop if this page repeats a page we've already parsed (the
                # postback didn't advance) — prevents infinite loops / dupes.
                if first_doc and first_doc in seen_first:
                    log.info("    Page did not advance — stopping pagination")
                    break
                if first_doc:
                    seen_first.add(first_doc)

                next_btn = await page.query_selector(
                    'input[value="NEXT"]:not([disabled]), '
                    'input[value="Next"]:not([disabled]), '
                    'a:has-text("NEXT"), a:has-text("Next")')
                if not next_btn:
                    break

                # Click NEXT, retrying once if the element detaches mid-postback.
                clicked = False
                for _try in range(2):
                    try:
                        await next_btn.click(timeout=10000)
                        clicked = True
                        break
                    except Exception as e:
                        log.info(f"    NEXT click retry ({e})")
                        await self._wait_overlay_hidden(page)
                        next_btn = await page.query_selector(
                            'input[value="NEXT"]:not([disabled]), '
                            'input[value="Next"]:not([disabled]), '
                            'a:has-text("NEXT"), a:has-text("Next")')
                        if not next_btn:
                            break
                if not clicked:
                    break

                # Wait for the grid to actually change to the next page.
                await self._wait_overlay_hidden(page)
                try:
                    await page.wait_for_function(
                        """(prev) => {
                            const m = document.body.innerText.match(/RP-\\d{4}-\\d+/);
                            return m && m[0] !== prev;
                        }""",
                        arg=first_doc, timeout=15000)
                except Exception:
                    # No change detected in time — likely the last page.
                    await page.wait_for_timeout(1000)

            self.per_source[doc_code]  = row_count
            self.page_counts[doc_code] = page_num
            log.info(f"    {doc_code}: {row_count} rows across {page_num} pages")

        except Exception as e:
            log.warning(f"  Error on {doc_code}: {e}")
            # Record what we got so the sanity gate can see a shortfall.
            self.per_source.setdefault(doc_code, len(
                [r for r in self.records if r.get('cat') == cat]))

    async def scrape_all(self) -> list:
        log.info(f"Scraping: {self.start_date.date()} → {self.today.date()}")
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"),
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()
            page.set_default_timeout(30000)

            for doc_code, (cat, cat_label) in DOC_TYPES.items():
                try:
                    # Foreclosure notices use a wider window so upcoming trustee
                    # sales are captured with grantor names (Bug 1). This is the
                    # enrichment source for foreclosures — the FRCL portal is
                    # kept only as a supplementary (nameless) auction calendar.
                    lb = FC_NOTICE_LOOKBACK_DAYS if doc_code in FC_NOTICE_CODES else None
                    await self._search_one(page, doc_code, cat, cat_label,
                                           lookback_days=lb)
                except Exception as e:
                    log.error(f"Fatal error on {doc_code}: {e}")


            # Also scrape dedicated Foreclosure portal (FRCL_R.aspx)
            try:
                await self._scrape_frcl(page)
            except Exception as e:
                log.error(f"FRCL scrape error: {e}")

            await browser.close()

        log.info(f"Total clerk records: {len(self.records)}")
        return self.records

    async def _scrape_frcl(self, page) -> None:
        """
        Scrape the dedicated Foreclosure portal (FRCL_R.aspx).
        Searches by Sale Date for current + next month (upcoming auctions)
        and by File Date for last 30 days (recently filed).
        """
        FRCL_URL = "https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx"
        log.info("  → FRCL_R (Foreclosure Portal)")

        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }

        # Determine months to cover: prev month, current, next
        now = self.today
        months_to_search = []
        # Previous month (covers 30-day lookback)
        if now.month == 1:
            months_to_search.append((now.year - 1, 12))
        else:
            months_to_search.append((now.year, now.month - 1))
        # Current month
        months_to_search.append((now.year, now.month))
        # Next month (upcoming auctions)
        if now.month == 12:
            months_to_search.append((now.year + 1, 1))
        else:
            months_to_search.append((now.year, now.month + 1))

        frcl_count = 0
        for year, month in months_to_search:
            for search_type in ["Sale Date", "File Date"]:
                try:
                    await page.goto(FRCL_URL, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(2500)

                    # Debug: dump all input/select elements on first visit
                    if frcl_count == 0 and search_type == "Sale Date":
                        inputs = await page.eval_on_selector_all(
                            "input, select",
                            "els => els.map(e => ({tag: e.tagName, id: e.id, name: e.name, type: e.type || '', value: e.value || ''}))"
                        )
                        log.info(f"    FRCL form elements: {inputs[:15]}")

                    # Find and click the appropriate radio button
                    # Try multiple selector strategies
                    radio_clicked = False
                    for radio_sel in [
                        f"input[type=radio][value*='{search_type[:4]}']",
                        f"input[type=radio][id*='Sale']" if "Sale" in search_type else f"input[type=radio][id*='File']",
                        "input[type=radio]",
                    ]:
                        radios = page.locator(radio_sel)
                        count = await radios.count()
                        if count > 0:
                            if search_type == "Sale Date":
                                await radios.first.click()
                            else:
                                # Click second radio for File Date
                                idx = 1 if count > 1 else 0
                                await radios.nth(idx).click()
                            radio_clicked = True
                            await page.wait_for_timeout(500)
                            break

                    # Select year
                    year_sels = page.locator("select")
                    sel_count = await year_sels.count()
                    for si in range(sel_count):
                        sel = year_sels.nth(si)
                        sel_id = await sel.get_attribute("id") or ""
                        if "year" in sel_id.lower() or "yr" in sel_id.lower():
                            try:
                                await sel.select_option(str(year))
                                await page.wait_for_timeout(300)
                                break
                            except Exception:
                                pass
                    else:
                        # Try first select
                        if sel_count > 0:
                            try:
                                await year_sels.first.select_option(str(year))
                                await page.wait_for_timeout(300)
                            except Exception:
                                pass

                    # Select month
                    for si in range(sel_count):
                        sel = year_sels.nth(si)
                        sel_id = await sel.get_attribute("id") or ""
                        if "month" in sel_id.lower() or "mon" in sel_id.lower():
                            try:
                                await sel.select_option(month_names[month])
                                await page.wait_for_timeout(300)
                                break
                            except Exception:
                                # Try numeric value
                                try:
                                    await sel.select_option(str(month))
                                    await page.wait_for_timeout(300)
                                except Exception:
                                    pass
                            break
                    else:
                        # Try second select
                        if sel_count > 1:
                            try:
                                await year_sels.nth(1).select_option(month_names[month])
                                await page.wait_for_timeout(300)
                            except Exception:
                                pass

                    # Click search button
                    for btn_sel in [
                        "input[value='Search']",
                        "input[type=submit]",
                        "input[type=button][value*='Search']",
                        "button:has-text('Search')",
                    ]:
                        btn = page.locator(btn_sel)
                        if await btn.count() > 0:
                            await btn.first.click()
                            await page.wait_for_load_state("domcontentloaded", timeout=20000)
                            await page.wait_for_timeout(2500)
                            break

                    # Paginate - FRCL uses page number links in a table at top
                    # Debug showed: row 0 = "12345678910..." = page number links
                    # Each page shows 38 records; click each page number
                    html = await page.content()
                    month_rows = []
                    first_rows = self._parse_frcl_page(html, year, month, search_type)
                    month_rows.extend(first_rows)

                    # Find how many pages exist from pagination row
                    from bs4 import BeautifulSoup as _BS4
                    _soup = _BS4(html, "lxml")
                    page_links = []
                    for _a in _soup.find_all("a"):
                        txt = _a.get_text(strip=True)
                        if txt.isdigit() and int(txt) > 1:
                            page_links.append(int(txt))
                    max_page = max(page_links) if page_links else 1
                    log.info(f"      FRCL pages detected: {max_page}")

                    # Click each page number
                    for pg in range(2, min(max_page + 1, 25)):
                        try:
                            pg_link = page.locator(f"a:text-is('{pg}')")
                            if await pg_link.count() == 0:
                                # Try broader selector
                                pg_link = page.locator(f"a").filter(has_text=str(pg))
                            if await pg_link.count() > 0:
                                await pg_link.first.click()
                                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                                await page.wait_for_timeout(1500)
                                pg_html = await page.content()
                                pg_rows = self._parse_frcl_page(pg_html, year, month, search_type)
                                month_rows.extend(pg_rows)
                                if len(pg_rows) == 0:
                                    break
                            else:
                                break
                        except Exception as pg_e:
                            log.warning(f"      FRCL page {pg} error: {pg_e}")
                            break

                    frcl_count += len(month_rows)
                    self.records.extend(month_rows)
                    log.info(f"    FRCL [{search_type}] {month_names[month]} {year}: {len(month_rows)} foreclosures ({max_page} pages)")

                except Exception as e:
                    log.warning(f"    FRCL [{search_type}] {year}-{month:02d} error: {e}")
                    continue

        self.per_source["FRCL"] = frcl_count
        log.info(f"  FRCL total: {frcl_count} foreclosure postings")

    def _parse_frcl_page(self, html: str, year: int, month: int, search_type: str = "Sale Date") -> list:
        """
        Parse FRCL results page.
        The page renders a table with columns like:
          Document ID | Grantor Name | Sale Date | Property Address | ...
        """
        from bs4 import BeautifulSoup
        import re as _re
        soup = BeautifulSoup(html, "lxml")
        rows_out = []
        seen = set()

        # Log table count and sizes for debug
        tables = soup.find_all("table")
        log.info(f"    FRCL parser: {len(tables)} tables on page")

        # Find the results table — look for one with multiple rows of data
        # and content that looks like document records
        best_table = None
        best_score = 0
        for tbl in tables:
            trs = tbl.find_all("tr")
            if len(trs) < 2:
                continue
            txt = tbl.get_text(" ")
            # Score by presence of date patterns and name-like content
            score = 0
            if _re.search(r'\d{1,2}/\d{1,2}/\d{4}', txt):
                score += 3
            if len(trs) > 5:
                score += len(trs)
            if "Grantor" in txt or "Trustee" in txt or "Mortgage" in txt:
                score += 2
            if score > best_score:
                best_score = score
                best_table = tbl

        if best_table is None:
            # Dump page text snippet for debugging
            page_txt = soup.get_text(" ")[:500]
            log.info(f"    FRCL no table found. Page snippet: {page_txt[:200]}")
            return rows_out

        log.info(f"    FRCL best table: {len(best_table.find_all('tr'))} rows, score={best_score}")

        # Log first few rows for debugging
        for ri, tr in enumerate(best_table.find_all("tr")[:5]):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            log.info(f"    FRCL row {ri}: {cells[:6]}")

        # Parse all rows
        header_map = {}
        for ri, tr in enumerate(best_table.find_all("tr")):
            cells_raw = tr.find_all(["td", "th"])
            cells = [c.get_text(strip=True) for c in cells_raw]

            if not cells or not any(cells):
                continue

            # Detect header row
            if ri == 0 or any(h in " ".join(cells).upper() for h in
                              ["DOCUMENT", "GRANTOR", "SALE DATE", "FILE DATE", "ADDRESS", "INSTRUMENT"]):
                for ci, c in enumerate(cells):
                    header_map[ci] = c.upper().strip()
                continue

            if len(cells) < 2:
                continue

            # Try to extract fields using header map or positional guessing
            def get_field(*keys):
                for ci, hdr in header_map.items():
                    for k in keys:
                        if k in hdr and ci < len(cells):
                            return cells[ci]
                return ""

            # Confirmed FRCL layout from debug:
            # col 0: blank, col 1: Doc ID, col 2: Sale Date, col 3: File Date, col 4: Pgs
            # Grantor name not available in list view — must open individual document
            doc_num  = cells[1] if len(cells) > 1 else cells[0] if cells else ""
            sale_dt  = cells[2] if len(cells) > 2 else f"{year}-{month:02d}-01"
            file_dt  = cells[3] if len(cells) > 3 else ""
            grantor  = ""  # Not available in list view
            address  = ""  # Not available in list view — HCAD match will find it

            # Skip obvious non-data rows
            if not doc_num or len(doc_num) < 2:
                continue
            if doc_num.upper() in ["DOCUMENT ID", "DOC ID", "FILE NUMBER", ""]:
                continue
            # Deduplicate — a posting shows up under both the Sale-Date and
            # File-Date searches (and across adjacent months), so key on the
            # document number alone.
            if doc_num in seen:
                continue
            seen.add(doc_num)

            def _parse_frcl_date(s):
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
                    try:
                        return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
                    except Exception:
                        pass
                return ""

            # BUG 2 FIX: the foreclosure *sale* date is often 1-3 months in the
            # FUTURE. Storing it in `filed` produced future-dated leads and made
            # build_flags tag them "New this week" (today - future <= 7). Keep
            # `filed` = actual file date only; expose the auction date separately
            # via `frcl_sale_date`.
            sale_iso = _parse_frcl_date(sale_dt)
            file_iso = _parse_frcl_date(file_dt)

            rows_out.append({
                "doc_num":           doc_num,
                "doc_type":          "FRCL",
                "filed":             file_iso,          # past file date, may be ""
                "cat":               "fc",
                "cat_label":         "Foreclosure Sale",
                "owner":             grantor,
                "grantee":           "",
                "contact":           grantor,
                "amount":            None,
                "legal":             address,
                "clerk_url":         f"https://www.cclerk.hctx.net/applications/websearch/FRCL_R.aspx",
                "frcl_sale_date":    sale_iso,          # auction date (may be future)
                "frcl_search_type":  search_type,
                "source":            "FRCL",
            })

        return rows_out

def _prop_key(rec: dict):
    """Normalized (street, zip5) key for stacking the same property together."""
    addr = re.sub(r"\s+", " ", (rec.get("prop_address") or "").strip().upper())
    zp   = (rec.get("prop_zip") or "").strip()[:5]
    if not addr:
        return None
    return (addr, zp)


def apply_stacking(records: list) -> None:
    """Annotate each record with how many *distinct* distress lists its property
    appears on, and the set of source categories. Same property showing up as
    both a tax lien and a foreclosure is a much stronger lead than either alone."""
    groups = {}
    for r in records:
        k = _prop_key(r)
        if k:
            groups.setdefault(k, []).append(r)
    for r in records:
        r["stack_count"] = 1
        r["sources"] = sorted({(r.get("cat") or "").upper()} - {""}) or \
                       [("FRCL" if r.get("source") == "FRCL" else "RP")]
    for k, grp in groups.items():
        cats = sorted({(x.get("cat") or "").upper() for x in grp} - {""})
        if len(cats) <= 1:
            continue
        for x in grp:
            x["stack_count"] = len(cats)
            x["sources"] = cats


# ─── Enrich & Score ───────────────────────────────────────────────────────────
def enrich_records(raw: list, parcel: HCADParcelLookup) -> list:
    today    = datetime.now()
    seen     = set()
    enriched = []
    engine   = getattr(parcel, "engine", None)
    conf_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "NONE": 0}

    # Phase 1: dedup + address enrichment
    for rec in raw:
        try:
            key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
            if key in seen:
                continue
            seen.add(key)

            if engine:
                rec = engine.enrich(rec)
            else:
                p = parcel.find(rec.get("owner", ""))
                rec["prop_address"] = p["site_addr"]  if p else ""
                rec["prop_city"]    = p["site_city"]  if p else "Houston"
                rec["prop_state"]   = "TX"
                rec["prop_zip"]     = p["site_zip"]   if p else ""
                rec["mail_address"] = p["mail_addr"]  if p else ""
                rec["mail_city"]    = p["mail_city"]  if p else ""
                rec["mail_state"]   = p["mail_state"] if p else "TX"
                rec["mail_zip"]     = p["mail_zip"]   if p else ""
                rec["match_confidence"] = "MEDIUM" if p else "NONE"
                rec["hcad_url"] = ""

            conf_counts[rec.get("match_confidence", "NONE")] += 1
            enriched.append(rec)
        except Exception as e:
            log.warning(f"Enrich error: {e}")

    # Phase 2: stacking (needs all enriched addresses first)
    apply_stacking(enriched)

    # Phase 3: flags + score
    for rec in enriched:
        try:
            flags        = build_flags(rec, today)
            rec["flags"] = flags
            rec["score"] = compute_score(rec, flags)
        except Exception as e:
            log.warning(f"Enrich error: {e}")

    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)
    with_addr = sum(1 for r in enriched if r.get("prop_address"))
    log.info(f"Enriched {len(enriched)} unique records | {with_addr} with address")
    log.info(f"  Confidence: HIGH={conf_counts['HIGH']} "
             f"MEDIUM={conf_counts['MEDIUM']} "
             f"LOW={conf_counts['LOW']} "
             f"NONE={conf_counts['NONE']}")
    return enriched

# ─── New-lead detection (Bug 6) ───────────────────────────────────────────────
# Persist every doc number ever seen. Each run, anything not already in the file
# is stamped is_new=True so the dashboard can surface a "New" tab + badge.
SEEN_FILE = OUTPUT_DIRS[0] / "seen_doc_nums.json"


def stamp_new_leads(records: list) -> int:
    """Mark records first seen on this run, then persist the seen set.
    On the very first run (no seen file yet) nothing is marked new — we just
    seed the file — to avoid a useless 'everything is new' batch."""
    try:
        seen = set(json.loads(SEEN_FILE.read_text()))
        first_run = False
    except Exception:
        seen = set()
        first_run = True

    new_count = 0
    for r in records:
        dn = (r.get("doc_num") or "").strip()
        is_new = bool(dn) and not first_run and dn not in seen
        r["is_new"] = is_new
        if is_new:
            new_count += 1

    all_docs = seen | {(r.get("doc_num") or "").strip()
                       for r in records if (r.get("doc_num") or "").strip()}
    try:
        SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        SEEN_FILE.write_text(json.dumps(sorted(all_docs)))
    except Exception as e:
        log.warning(f"Could not write {SEEN_FILE}: {e}")

    log.info(f"New leads this run: {new_count} "
             f"(seen set now {len(all_docs)} docs)"
             + (" [first run — seeded, none marked new]" if first_run else ""))
    return new_count


# ─── Save ─────────────────────────────────────────────────────────────────────
def save_records(records: list, today: datetime, days_back: int,
                 new_count: int = 0):
    payload = {
        "fetched_at":   today.isoformat(),
        "source":       "Harris County Clerk / HCAD",
        "date_range": {
            "from": (today - timedelta(days=days_back)).strftime("%Y-%m-%d"),
            "to":   today.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "new_count":    new_count,
        "records":      records,
    }
    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        (d / "records.json").write_text(
            json.dumps(payload, indent=2, default=str))
        log.info(f"Saved → {d}/records.json")


# Bug 5: export_ghl_csv() was removed. It wrote a dated GHL CSV into both output
# dirs every run, and the workflow committed them — 150+ MB of accumulated bloat.
# The dashboard already builds the identical GHL export client-side on demand.


# ─── Sanity gate (Bug 4) ──────────────────────────────────────────────────────
# Core sources that must return data on a healthy run. If any is empty or a doc
# type caps at exactly the page size (the "stopped at page 1" signature), the run
# is a silent partial failure and must NOT overwrite good data / must exit non-0.
GATE_PAGE_SIZE   = 200   # RP results page size — an exact multiple with no more
GATE_MIN_TOTAL   = 400   # implausibly low overall record count
GATE_REQUIRED    = ["L/P", "FRCL"]  # doc codes that should never be empty


def sanity_gate(per_source: dict, page_counts: dict, total: int) -> list:
    """Return a list of human-readable problems. Empty list == healthy run."""
    problems = []
    for code in GATE_REQUIRED:
        if per_source.get(code, 0) == 0:
            problems.append(f"required source '{code}' returned 0 rows")
    for code, cnt in per_source.items():
        # The page-1 cap signature: a doc type that returned exactly one full
        # page of GATE_PAGE_SIZE rows and fetched no further pages — the
        # ASP.NET overlay silently stopped pagination. (Large genuine counts
        # that merely happen to be multiples of 200 are NOT flagged.)
        pages = page_counts.get(code)
        if pages is not None and pages <= 1 and cnt and cnt % GATE_PAGE_SIZE == 0:
            problems.append(
                f"'{code}' stopped at {pages} page of {cnt} rows — "
                f"pagination truncated (page-1 cap signature)")
    if total < GATE_MIN_TOTAL:
        problems.append(f"total records {total} < floor {GATE_MIN_TOTAL}")
    return problems


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main():
    today     = datetime.now()
    days_back = int(os.environ.get("DAYS_BACK", "7"))

    log.info("=" * 60)
    log.info("Harris County Motivated Seller Scraper")
    log.info(f"Date range: last {days_back} days")
    log.info("=" * 60)

    log.info("\n[1/3] Loading HCAD parcel data...")
    parcel = HCADParcelLookup()
    try:
        parcel.load()
    except Exception as e:
        log.warning(f"HCAD load error: {e}")

    log.info("\n[2/3] Scraping Harris County Clerk...")
    raw = []
    scraper = HarrisClerkScraper(days_back=days_back)
    try:
        raw = await scraper.scrape_all()
    except Exception as e:
        log.error(f"Scraper error: {e}")

    log.info("\n[3/3] Enriching and scoring...")
    records = enrich_records(raw or [], parcel)

    # ── Sanity gate (Bug 4): fail loudly on silent partial scrapes ────────────
    log.info("\nPer-source row counts:")
    for code, cnt in sorted(scraper.per_source.items()):
        log.info(f"  {code:8} {cnt:6}  ({scraper.page_counts.get(code, '?')} pages)")
    problems = sanity_gate(scraper.per_source, scraper.page_counts, len(records))
    if problems:
        log.error("✗ SANITY GATE FAILED — not overwriting good data:")
        for p in problems:
            log.error(f"    - {p}")
        if os.environ.get("SKIP_SANITY_GATE") == "1":
            log.warning("SKIP_SANITY_GATE=1 set — saving anyway.")
        else:
            log.error("Exiting non-zero. Set SKIP_SANITY_GATE=1 to override.")
            sys.exit(1)
    else:
        log.info("✓ Sanity gate passed.")

    # New-lead detection (Bug 6) — only after the gate passes, so a partial
    # scrape can't poison the seen set.
    new_count = stamp_new_leads(records)

    log.info("\nSaving outputs...")
    save_records(records, today, days_back, new_count)

    log.info(f"\n{'='*60}")
    log.info(f"✓ COMPLETE")
    log.info(f"  Total records:    {len(records)}")
    log.info(f"  With address:     {sum(1 for r in records if r.get('prop_address'))}")
    log.info(f"  Hot leads (≥70):  {sum(1 for r in records if r.get('score',0) >= 70)}")
    log.info(f"  Warm leads (≥50): {sum(1 for r in records if 50 <= r.get('score',0) < 70)}")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
