"""
DealMachine enrichment (Phase 1) — enrich records that already have a property
address with owner contact info (phones/emails), owner name, mailing address,
and valuation/equity.

Contract (confirmed against https://api.docs.dealmachine.com):
  Base: https://api.v2.dealmachine.com/v1   Auth: Authorization: Bearer <key>
  POST /v1/enrichment/address
    body: { "data": [ {street,city,state,zip} | {full_address} ],
            "contact_audience": "owners" }
    resp: { data:[ {matched, full_address, address, city, state, zip,
                    estimated_value, contacts:[{first_name,last_name,full_name,
                    phones:[...], emails:[...]}], ...} ],
            totals:{submitted,matched,unmatched},
            credits:{used,properties,people,deduplicated,licensed} }
    Misses are free.
  GET /v1/usage -> { credits:{ included, used, remaining, overage, breakdown } }
  Limits: 60 req/min, 5000 req/day per org (429 + Retry-After).

Security: the API key is read ONLY from os.environ["DEALMACHINE_API_KEY"]. It is
never logged, printed, or written to disk. If absent, enrichment is skipped.
"""

import os
import re
import json
import time
import logging
from datetime import datetime
from pathlib import Path

import requests

log = logging.getLogger(__name__)

BASE_URL       = "https://api.v2.dealmachine.com/v1"
CACHE_VERSION  = 2         # bump to re-fetch cached entries for new fields
                           # (re-access within a billing cycle is free per docs)
BATCH_SIZE     = 25        # addresses per request (well under any body limit)
REQUEST_SPACING= 1.2       # seconds between requests -> ~50/min, under the 60/min cap
MAX_REQUESTS   = 120       # hard cap on requests per run (<< 5000/day)
MAX_ADDRESSES  = MAX_REQUESTS * BATCH_SIZE   # bound on properties enriched per run
HTTP_TIMEOUT   = 30


# ─── helpers ──────────────────────────────────────────────────────────────────
def _addr_key(rec: dict) -> str:
    """Stable cache key: normalized street + zip5. Same property → same key."""
    street = re.sub(r"\s+", " ", (rec.get("prop_address") or "").strip().upper())
    zp     = (rec.get("prop_zip") or "").strip()[:5]
    if not street:
        return ""
    return f"{street}|{zp}"


def _extract_phones(contact: dict) -> list:
    """Phone items are loosely typed in the spec — handle str or object, and
    look for a DNC flag under several possible names."""
    out = []
    for p in (contact.get("phones") or []):
        if isinstance(p, str):
            out.append({"number": p, "dnc": None})
            continue
        if isinstance(p, dict):
            num = (p.get("phone") or p.get("number") or p.get("phone_number")
                   or p.get("value") or "")
            dnc = None
            for k in ("dnc", "do_not_call", "is_dnc", "on_dnc", "dnc_flag", "is_do_not_call"):
                if k in p:
                    dnc = bool(p[k]); break
            if num:
                out.append({"number": str(num), "dnc": dnc})
    return out


def _extract_emails(contact: dict) -> list:
    out = []
    for e in (contact.get("emails") or []):
        if isinstance(e, str):
            out.append(e)
        elif isinstance(e, dict):
            val = e.get("email") or e.get("address") or e.get("value")
            if val:
                out.append(str(val))
    return out


def _first(*vals):
    for v in vals:
        if v not in (None, ""):
            return v
    return ""


def _parse_property(prop: dict) -> dict:
    """Turn one DealMachine property record into the dm_* fields we store.
    Independent fields: a match with no phone/email is still a success."""
    contacts = prop.get("contacts") or []
    phones, emails, owner = [], [], ""
    for c in contacts:
        phones.extend(_extract_phones(c))
        emails.extend(_extract_emails(c))
        if not owner:
            owner = _first(c.get("full_name"),
                           (c.get("first_name", "") + " " + c.get("last_name", "")).strip())
    # dedupe, keep first 3 phones / 3 emails
    seen_n, uphones = set(), []
    for p in phones:
        if p["number"] and p["number"] not in seen_n:
            seen_n.add(p["number"]); uphones.append(p)
    uemails = list(dict.fromkeys([e for e in emails if e]))

    # Mailing address / equity are not strictly typed in the address-endpoint
    # schema; capture them defensively if the response includes them.
    mailing = _first(prop.get("mailing_address"), prop.get("owner_mailing_address"),
                     prop.get("mail_address"))
    est_val = _first(prop.get("estimated_value"), prop.get("calculated_total_value"),
                     prop.get("market_value"), prop.get("assessed_total_value"))
    equity  = _first(prop.get("equity_percent"), prop.get("equity_percentage"),
                     prop.get("estimated_equity_percent"), prop.get("equity"))
    equity_d= _first(prop.get("equity_dollars"), prop.get("estimated_equity"),
                     prop.get("equity_amount"))

    # Capture EVERY top-level scalar field DealMachine returns (Change 2/4), so
    # value/equity/sale/owner-type/absentee/vacancy/tax indicators are all kept
    # without hard-coding a schema. Nested objects/arrays (contacts, input) and
    # noise are skipped.
    _skip = {"contacts", "input", "match_failure", "match_warning"}
    dm_data = {}
    for k, v in (prop.items() if isinstance(prop, dict) else []):
        if k in _skip:
            continue
        if isinstance(v, (str, int, float, bool)) and v not in ("", None):
            dm_data[k] = v

    return {
        "cache_v":            CACHE_VERSION,
        "dm_enriched":        True,
        "dm_matched":         bool(prop.get("matched", True)),
        "dm_owner_name":      owner or "",
        "dm_phone":           uphones[:3],          # [{number, dnc}]
        "dm_email":           uemails[:3],
        "dm_mailing_address": mailing or "",
        "dm_property_id":     prop.get("dm_property_id", ""),
        "dm_estimated_value": est_val if est_val != "" else None,
        "dm_equity_percent":  equity if equity != "" else None,
        "dm_equity_dollars":  equity_d if equity_d != "" else None,
        "dm_sale_price":      _first(prop.get("sale_price"), prop.get("last_sale_price")) or None,
        "dm_sale_date":       _first(prop.get("sale_date"), prop.get("last_sale_date")) or "",
        "dm_data":            dm_data,             # all scalar fields returned
        "dm_raw":             prop,                # FULL response payload — so no
                                                   # future field addition ever
                                                   # needs a re-call
    }


# ─── API calls ────────────────────────────────────────────────────────────────
def _headers(key: str) -> dict:
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}


def _check_usage(key: str) -> "int | None":
    """Return remaining credits, or None if it couldn't be determined."""
    try:
        r = requests.get(f"{BASE_URL}/usage", headers=_headers(key), timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            log.warning(f"DealMachine /usage returned {r.status_code}; proceeding cautiously")
            return None
        cr = (r.json() or {}).get("credits", {}) or {}
        remaining = cr.get("remaining")
        log.info(f"DealMachine credits — included={cr.get('included')} "
                 f"used={cr.get('used')} remaining={remaining}")
        return remaining
    except Exception as e:
        log.warning(f"DealMachine /usage check failed: {e}")
        return None


def _enrich_batch(key: str, batch: list) -> dict:
    """POST one batch of records to /enrichment/address. Returns
    {addr_key: dm_fields} for matches and credits_used. Honors 429 backoff."""
    payload = {
        "data": [{
            "street": r.get("prop_address", ""),
            "city":   r.get("prop_city", ""),
            "state":  r.get("prop_state", "TX"),
            "zip":    (r.get("prop_zip") or "")[:5],
        } for r in batch],
        "contact_audience": "owners",
    }
    for attempt in range(3):
        try:
            resp = requests.post(f"{BASE_URL}/enrichment/address",
                                 headers=_headers(key), json=payload, timeout=HTTP_TIMEOUT)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "5"))
                log.warning(f"DealMachine 429 rate-limited; sleeping {wait}s")
                time.sleep(min(wait, 30)); continue
            if resp.status_code != 200:
                log.warning(f"DealMachine enrich returned {resp.status_code}")
                return {"results": {}, "credits": 0, "matched": 0, "misses": len(batch)}
            body = resp.json() or {}
            data = body.get("data") or []
            credits = ((body.get("credits") or {}).get("used")) or 0
            results, matched = {}, 0
            for i, prop in enumerate(data):
                if i >= len(batch):
                    break
                k = _addr_key(batch[i])
                if prop.get("matched", False) or prop.get("contacts") or prop.get("dm_property_id"):
                    results[k] = _parse_property(prop)
                    matched += 1
                else:
                    results[k] = {"dm_enriched": True, "dm_matched": False}  # cache the miss (free)
            return {"results": results, "credits": credits,
                    "matched": matched, "misses": len(batch) - matched}
        except Exception as e:
            log.warning(f"DealMachine batch error (attempt {attempt+1}): {e}")
            time.sleep(2)
    return {"results": {}, "credits": 0, "matched": 0, "misses": len(batch)}


# ─── main entry ───────────────────────────────────────────────────────────────
def enrich_addressed(records: list, cache_path: Path) -> dict:
    """Phase 1: enrich records that already have a property address. Applies the
    cache to every addressed record, sends only uncached addresses, and never
    blanks an existing field. Returns a stats dict for the run report."""
    stats = {"considered": 0, "from_cache": 0, "newly_enriched": 0,
             "tier_contact": 0, "tier_address_only": 0, "tier_miss": 0,
             "credits_used": 0, "requests": 0, "skipped": False,
             "credits_before": None, "credits_after": None, "aborted": False}

    key = os.environ.get("DEALMACHINE_API_KEY")
    if not key:
        log.info("DealMachine key not present, skipping enrichment")
        stats["skipped"] = "no_key"
        return stats

    # Load cache (addr_key -> dm_fields). Missing/corrupt -> empty.
    try:
        cache = json.loads(Path(cache_path).read_text())
        if not isinstance(cache, dict):
            cache = {}
    except Exception:
        cache = {}

    addressed = [r for r in records if (r.get("prop_address") or "").strip()]
    stats["considered"] = len(addressed)
    if not addressed:
        return stats

    # Credit balance BEFORE anything (Step 1).
    log.info("DealMachine: checking credit balance BEFORE backfill...")
    credits_before = _check_usage(key)
    stats["credits_before"] = credits_before
    can_call = not (credits_before is not None and credits_before <= 0)
    if not can_call:
        log.warning("DealMachine credits exhausted — skipping calls (apply cache only)")
        stats["skipped"] = "no_credits"

    # Determine which unique addresses need a call: not cached, OR cached under
    # an older schema version (re-fetch to backfill new fields).
    todo, todo_keys = [], set()
    for r in addressed:
        k = _addr_key(r)
        if not k or k in todo_keys:
            continue
        cached = cache.get(k)
        if cached and cached.get("cache_v") == CACHE_VERSION:
            continue
        todo_keys.add(k); todo.append(r)
    stale = sum(1 for k in todo_keys if k in cache)
    if todo:
        log.info(f"DealMachine: {len(todo)} addresses to fetch "
                 f"({stale} stale re-fetch, {len(todo)-stale} brand new)")

    def _persist():
        try:
            Path(cache_path).write_text(json.dumps(cache))
        except Exception as e:
            log.warning(f"Could not write DealMachine cache: {e}")

    if can_call and todo:
        todo = todo[:MAX_ADDRESSES]
        # ── CANARY (Step 2): prove re-access is FREE on the first few records
        # before committing to all of them. Abort if the credit counter moves.
        CANARY_N = 3
        canary = todo[:CANARY_N]
        log.info(f"DealMachine CANARY: enriching first {len(canary)} records to "
                 f"test credit cost before the full backfill...")
        out = _enrich_batch(key, canary)
        cache.update(out["results"]); _persist()
        stats["requests"]      += 1
        stats["credits_used"]  += out["credits"]
        stats["newly_enriched"]+= len(out["results"])
        credits_after_canary = _check_usage(key)
        delta = (credits_before - credits_after_canary) \
            if (credits_before is not None and credits_after_canary is not None) else None
        log.info(f"DealMachine CANARY result: credits_before={credits_before} "
                 f"credits_after={credits_after_canary} delta={delta} "
                 f"batch_credits_used={out['credits']}")
        charged = (out["credits"] and out["credits"] > 0) or (delta is not None and delta > 0)
        if charged:
            log.error(f"✗ DealMachine CANARY: credits MOVED (delta={delta}, "
                      f"batch={out['credits']}) — ABORTING. NOT re-fetching the "
                      f"remaining {len(todo)-len(canary)} records.")
            stats["aborted"] = True
        elif os.environ.get("DM_CANARY_ONLY") in ("1", "true", "True"):
            log.info("✓ DealMachine CANARY: 0 credits. DM_CANARY_ONLY set — "
                     "stopping after the canary as requested.")
        else:
            log.info("✓ DealMachine CANARY: 0 credits — re-access is free; "
                     "proceeding with the full backfill.")
            rest = todo[CANARY_N:]
            for start in range(0, len(rest), BATCH_SIZE):
                if stats["requests"] >= MAX_REQUESTS:
                    log.warning(f"Hit MAX_REQUESTS={MAX_REQUESTS} — stopping")
                    break
                batch = rest[start:start + BATCH_SIZE]
                out = _enrich_batch(key, batch)
                cache.update(out["results"])
                stats["requests"]      += 1
                stats["credits_used"]  += out["credits"]
                stats["newly_enriched"]+= len(out["results"])
                time.sleep(REQUEST_SPACING)   # ~50 req/min, under the 60/min cap
            _persist()
        stats["credits_after"] = _check_usage(key)

    # Apply cache to every addressed record. Never blank existing good fields.
    for r in addressed:
        dm = cache.get(_addr_key(r))
        if not dm:
            continue
        if _addr_key(r) in cache:
            stats["from_cache"] += 1
        if not dm.get("dm_matched", False):
            stats["tier_miss"] += 1
            continue
        # Fill dm_* (only when present; never blank an existing good field).
        for k in ("dm_enriched", "dm_matched", "dm_owner_name", "dm_phone",
                  "dm_email", "dm_mailing_address", "dm_property_id",
                  "dm_estimated_value", "dm_equity_percent", "dm_equity_dollars",
                  "dm_sale_price", "dm_sale_date", "dm_data"):
            v = dm.get(k)
            if v not in (None, "", [], {}):
                r[k] = v
        r["dm_enriched_date"] = datetime.now().strftime("%Y-%m-%d")
        # upgrade OUR mailing address only if we didn't have one
        if dm.get("dm_mailing_address") and not (r.get("mail_address") or "").strip():
            r["mail_address"] = dm["dm_mailing_address"]

        has_contact = bool(dm.get("dm_phone")) or bool(dm.get("dm_email"))
        if has_contact:
            stats["tier_contact"] += 1
        else:
            stats["tier_address_only"] += 1

    # Discovery: log the full set of scalar fields DealMachine actually returned,
    # so we can see exactly which value/equity/indicator columns exist.
    field_union = set()
    for dm in cache.values():
        field_union.update((dm.get("dm_data") or {}).keys())
    if field_union:
        log.info(f"DealMachine returned fields ({len(field_union)}): "
                 f"{sorted(field_union)}")

    with_value = sum(1 for r in records if r.get("dm_estimated_value") is not None)
    with_equity = sum(1 for r in records
                      if r.get("dm_equity_percent") is not None
                      or r.get("dm_equity_dollars") is not None)
    log.info(
        f"DealMachine Phase 1: considered={stats['considered']} "
        f"newly_enriched={stats['newly_enriched']} "
        f"| tiers: contact={stats['tier_contact']} "
        f"address_only={stats['tier_address_only']} miss={stats['tier_miss']} "
        f"| value_populated={with_value} equity_populated={with_equity} "
        f"| requests={stats['requests']} credits_used={stats['credits_used']} "
        f"credits_before={stats['credits_before']} credits_after={stats['credits_after']} "
        f"aborted={stats['aborted']}")
    return stats
