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

SCORE_BASE        = 30
SCORE_PER_FLAG    = 10
SCORE_LP_FC_COMBO = 20
SCORE_AMOUNT_100K = 15
SCORE_AMOUNT_50K  = 10
SCORE_NEW_WEEK    = 5
SCORE_HAS_ADDRESS = 5

OUTPUT_DIRS = [Path("dashboard"), Path("data")]


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

                # Mailing address
                mail_addr  = r.get("mailto_addr") or r.get("mail_addr_1") or ""
                mail_city  = r.get("mail_city", "")
                mail_state = r.get("mail_state", "TX")
                mail_zip   = r.get("mail_zip", "")

                # Site/property address
                site_addr  = r.get("site_addr", "")
                site_city  = r.get("site_city", "HOUSTON")
                site_state = r.get("site_state", "TX")
                site_zip   = r.get("site_zip", "")

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
        if acct_map:
            self._load_owners(zip_bytes, acct_map)
        else:
            log.warning("  real_acct parse returned 0 records")

    def find(self, name: str) -> Optional[dict]:
        if not name:
            return None
        for v in self._name_variants(name):
            if v in self.name_lookup:
                return self.name_lookup[v]
        return None


# ─── Scoring ──────────────────────────────────────────────────────────────────
def build_flags(rec: dict, today: datetime) -> list:
    flags = []
    try:
        cat      = rec.get("cat", "")
        doc_type = rec.get("doc_type", "").upper()
        owner    = (rec.get("owner") or "").upper()
        filed    = rec.get("filed") or ""

        if cat == "lp" and "REL" not in doc_type:
            flags.append("Lis pendens")
        if cat == "fc":
            flags.append("Pre-foreclosure")
        if cat == "jud":
            flags.append("Judgment lien")
        if doc_type in ("T/L", "LEVY"):
            flags.append("Tax lien")
        if cat == "lien" and doc_type == "LIEN":
            flags.append("Mechanic lien")
        if cat == "probate":
            flags.append("Probate / estate")
        if any(x in owner for x in
               ("LLC", " LP", "INC", "CORP", "LTD", "TRUST", "ASSOC")):
            flags.append("LLC / corp owner")
        try:
            dt = datetime.strptime(filed[:10], "%Y-%m-%d")
            if (today - dt).days <= 7:
                flags.append("New this week")
        except Exception:
            pass
    except Exception:
        pass
    return flags


def compute_score(rec: dict, flags: list) -> int:
    try:
        score = SCORE_BASE + len(flags) * SCORE_PER_FLAG
        cat = rec.get("cat", "")
        if "Lis pendens" in flags and "Pre-foreclosure" in flags:
            score += SCORE_LP_FC_COMBO
        elif cat in ("lp", "fc") and len(flags) >= 2:
            score += SCORE_LP_FC_COMBO
        amount = 0
        try:
            amount = float(str(rec.get("amount") or "0")
                           .replace(",", "").replace("$", ""))
        except Exception:
            pass
        if amount >= 100_000:
            score += SCORE_AMOUNT_100K
        elif amount >= 50_000:
            score += SCORE_AMOUNT_50K
        if "New this week" in flags:
            score += SCORE_NEW_WEEK
        if rec.get("prop_address"):
            score += SCORE_HAS_ADDRESS
        return min(score, 100)
    except Exception:
        return SCORE_BASE


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

    def _parse_results_page(self, html: str, doc_code: str,
                            cat: str, cat_label: str) -> list:
        soup = BeautifulSoup(html, "lxml")
        rows_out = []

        # Find the main results table — has "File Number" and "Film Code" headers
        # and contains RP-YYYY-NNNNN file numbers
        target_table = None
        for tbl in soup.find_all("table"):
            txt = tbl.get_text(" ", strip=True)
            if ("File Number" in txt and "File Date" in txt
                    and FILE_NUMBER_RE.search(txt)):
                target_table = tbl
                break

        if not target_table:
            return []

        # Each data row is a <tr> with tds containing file number,
        # date, type, names nested table, legal nested table, pgs, film link
        for tr in target_table.find_all("tr"):
            try:
                cells = tr.find_all("td")
                if len(cells) < 5:
                    continue

                # Cell 0: File Number
                file_num = cells[0].get_text(strip=True)
                if not FILE_NUMBER_RE.match(file_num):
                    continue

                # Cell 1: File Date
                raw_date  = cells[1].get_text(strip=True)
                filed_iso = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                    try:
                        filed_iso = datetime.strptime(
                            raw_date[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass

                # Cell 2: Instrument type link
                type_link = cells[2].find("a")
                rec_type  = (type_link.get_text(strip=True)
                             if type_link else doc_code)

                # Cell 3: Names — "Grantor : NAME\nGrantee : NAME"
                grantor  = ""
                grantees = []
                for line in cells[3].get_text("\n").split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    low = line.lower()
                    if low.startswith("grantor"):
                        grantor = re.sub(
                            r'^grantor\s*:\s*', '', line,
                            flags=re.IGNORECASE).strip()
                    elif low.startswith("grantee"):
                        g = re.sub(
                            r'^grantee\s*:\s*', '', line,
                            flags=re.IGNORECASE).strip()
                        if g:
                            grantees.append(g)

                # Cell 4: Legal description
                legal = cells[4].get_text(" ", strip=True) if len(cells) > 4 else ""

                # Last cell: Film Code link
                clerk_url = self.BASE_URL
                a = cells[-1].find("a", href=True)
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
                    "amount":    None,
                    "legal":     legal,
                    "clerk_url": clerk_url,
                })

            except Exception as e:
                log.warning(f"Row parse error: {e}")
                continue

        return rows_out

    async def _search_one(self, page, doc_code: str,
                          cat: str, cat_label: str):
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
            date_from = self.start_date.strftime("%m/%d/%Y")
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

            page_num = 0
            while True:
                page_num += 1
                html = await page.content()
                rows = self._parse_results_page(html, doc_code, cat, cat_label)
                log.info(f"    Page {page_num}: {len(rows)} rows")
                self.records.extend(rows)

                next_btn = await page.query_selector(
                    'input[value="NEXT"], input[value="Next"], '
                    'a:has-text("NEXT"), a:has-text("Next")')
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                await page.wait_for_timeout(1000)

        except Exception as e:
            log.warning(f"  Error on {doc_code}: {e}")

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
                    await self._search_one(page, doc_code, cat, cat_label)
                except Exception as e:
                    log.error(f"Fatal error on {doc_code}: {e}")

            await browser.close()

        log.info(f"Total clerk records: {len(self.records)}")
        return self.records


# ─── Enrich & Score ───────────────────────────────────────────────────────────
def enrich_records(raw: list, parcel: HCADParcelLookup) -> list:
    today    = datetime.now()
    seen     = set()
    enriched = []

    for rec in raw:
        try:
            key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
            if key in seen:
                continue
            seen.add(key)

            p = parcel.find(rec.get("owner", ""))
            rec["prop_address"] = p["site_addr"]  if p else ""
            rec["prop_city"]    = p["site_city"]  if p else "Houston"
            rec["prop_state"]   = p.get("site_state", "TX") if p else "TX"
            rec["prop_zip"]     = p["site_zip"]   if p else ""
            rec["mail_address"] = p["mail_addr"]  if p else ""
            rec["mail_city"]    = p["mail_city"]  if p else ""
            rec["mail_state"]   = p["mail_state"] if p else "TX"
            rec["mail_zip"]     = p["mail_zip"]   if p else ""

            if not rec["mail_address"] and rec["prop_address"]:
                rec["mail_address"] = rec["prop_address"]
                rec["mail_city"]    = rec["prop_city"]
                rec["mail_state"]   = rec["prop_state"]
                rec["mail_zip"]     = rec["prop_zip"]

            flags        = build_flags(rec, today)
            rec["flags"] = flags
            rec["score"] = compute_score(rec, flags)
            enriched.append(rec)
        except Exception as e:
            log.warning(f"Enrich error: {e}")

    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)
    log.info(f"Enriched {len(enriched)} unique records "
             f"({sum(1 for r in enriched if r.get('prop_address'))} with address)")
    return enriched


# ─── Save ─────────────────────────────────────────────────────────────────────
def save_records(records: list, today: datetime, days_back: int):
    payload = {
        "fetched_at":   today.isoformat(),
        "source":       "Harris County Clerk / HCAD",
        "date_range": {
            "from": (today - timedelta(days=days_back)).strftime("%Y-%m-%d"),
            "to":   today.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        (d / "records.json").write_text(
            json.dumps(payload, indent=2, default=str))
        log.info(f"Saved → {d}/records.json")


def export_ghl_csv(records: list, today: datetime):
    cols = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    rows = []
    for r in records:
        try:
            parts  = (r.get("owner") or "").strip().split()
            amount = r.get("amount")
            rows.append({
                "First Name":             parts[0] if parts else "",
                "Last Name":              " ".join(parts[1:]) if len(parts) > 1 else "",
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "TX"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "TX"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       f"${amount:,.2f}" if amount else "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Harris County Clerk",
                "Public Records URL":     r.get("clerk_url", ""),
            })
        except Exception:
            continue

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"ghl_export_{today.strftime('%Y%m%d')}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        log.info(f"GHL CSV → {path} ({len(rows)} rows)")


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
    try:
        raw = await HarrisClerkScraper(days_back=days_back).scrape_all()
    except Exception as e:
        log.error(f"Scraper error: {e}")

    log.info("\n[3/3] Enriching and scoring...")
    records = enrich_records(raw or [], parcel)

    log.info("\nSaving outputs...")
    save_records(records, today, days_back)
    export_ghl_csv(records, today)

    log.info(f"\n{'='*60}")
    log.info(f"✓ COMPLETE")
    log.info(f"  Total records:    {len(records)}")
    log.info(f"  With address:     {sum(1 for r in records if r.get('prop_address'))}")
    log.info(f"  Hot leads (≥70):  {sum(1 for r in records if r.get('score',0) >= 70)}")
    log.info(f"  Warm leads (≥50): {sum(1 for r in records if 50 <= r.get('score',0) < 70)}")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
