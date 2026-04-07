"""
Harris County Motivated Seller Lead Scraper
Pulls from Harris County Clerk portal + HCAD bulk parcel data
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
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# Playwright for JS-heavy clerk portal
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# DBF reader for HCAD parcel bulk data
try:
    from dbfread import DBF
except ImportError:
    DBF = None

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─── Constants ───────────────────────────────────────────────────────────────
CLERK_URL = "https://www.cclerk.hctx.net/PublicRecords.aspx"
HCAD_BULK_BASE = "https://pdata.hcad.org"
HCAD_PARCEL_URL = "https://pdata.hcad.org/download/2024.aspx"  # fallback
HCAD_DBF_URLS = [
    "https://pdata.hcad.org/Desc/2024/account_info.zip",
    "https://pdata.hcad.org/Desc/2024/building_res.zip",
]
HCAD_REAL_ACCT_URL = "https://pdata.hcad.org/download/2024/account_info.zip"

# Document type codes → category mapping
DOC_TYPES = {
    # Lis Pendens
    "LP":      ("lp",      "Lis Pendens"),
    "RELLP":   ("lp",      "Release Lis Pendens"),
    # Foreclosure
    "NOFC":    ("fc",      "Notice of Foreclosure"),
    "NOF":     ("fc",      "Notice of Foreclosure"),
    # Tax
    "TAXDEED": ("tax",     "Tax Deed"),
    "TAXLIEN": ("tax",     "Tax Lien"),
    # Judgments
    "JUD":     ("jud",     "Judgment"),
    "CCJ":     ("jud",     "Certified Judgment"),
    "DRJUD":   ("jud",     "Domestic Relations Judgment"),
    # Liens – tax / federal
    "LNCORPTX":("lien",   "Corp Tax Lien"),
    "LNIRS":   ("lien",    "IRS Lien"),
    "LNFED":   ("lien",    "Federal Lien"),
    # Liens – general
    "LN":      ("lien",    "Lien"),
    "LNMECH":  ("lien",    "Mechanic Lien"),
    "LNHOA":   ("lien",    "HOA Lien"),
    # Medicaid
    "MEDLN":   ("lien",    "Medicaid Lien"),
    # Probate
    "PRO":     ("probate", "Probate"),
    "PROB":    ("probate", "Probate"),
    # Notices
    "NOC":     ("notice",  "Notice of Commencement"),
}

# Score weights
SCORE_BASE = 30
SCORE_PER_FLAG = 10
SCORE_LP_FC_COMBO = 20
SCORE_AMOUNT_100K = 15
SCORE_AMOUNT_50K = 10
SCORE_NEW_WEEK = 5
SCORE_HAS_ADDRESS = 5

OUTPUT_DIRS = [
    Path("dashboard"),
    Path("data"),
]

# ─── Retry helper ─────────────────────────────────────────────────────────────
def retry(func, attempts=3, delay=2, *args, **kwargs):
    for i in range(attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log.warning(f"Attempt {i+1}/{attempts} failed: {e}")
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    return None


# ─── HCAD Parcel Data ─────────────────────────────────────────────────────────
class HCADParcelLookup:
    """
    Downloads HCAD bulk parcel data and builds owner-name → address lookup.
    Tries multiple known HCAD bulk download endpoints.
    """

    KNOWN_URLS = [
        # Direct CSVs / zips from HCAD public data portal
        "https://pdata.hcad.org/download/2024/real_acct.zip",
        "https://pdata.hcad.org/Desc/2024/real_acct.zip",
        "https://pdata.hcad.org/download/2024/account_info.zip",
        "https://pdata.hcad.org/Desc/2024/account_info.zip",
    ]

    def __init__(self):
        self.lookup: dict[str, dict] = {}   # owner_key → parcel record
        self.acct_lookup: dict[str, dict] = {}  # account# → parcel record

    def _normalize_name(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.strip().upper())

    def _name_variants(self, full_name: str) -> list[str]:
        n = self._normalize_name(full_name)
        parts = n.split()
        variants = [n]
        if len(parts) >= 2:
            # LAST FIRST
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            # LAST, FIRST
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return variants

    def _try_download_zip(self, url: str) -> Optional[bytes]:
        """Download a ZIP file, return raw bytes or None."""
        try:
            log.info(f"Trying HCAD URL: {url}")
            r = requests.get(url, timeout=120, stream=True)
            if r.status_code == 200 and len(r.content) > 1000:
                log.info(f"  ✓ Downloaded {len(r.content)/1e6:.1f} MB")
                return r.content
        except Exception as e:
            log.warning(f"  ✗ {e}")
        return None

    def _parse_dbf_from_zip(self, zip_bytes: bytes) -> list[dict]:
        """Extract and parse first .dbf found in a zip."""
        if DBF is None:
            log.warning("dbfread not installed – skipping DBF parse")
            return []
        records = []
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            dbf_files = [n for n in z.namelist() if n.lower().endswith(".dbf")]
            if not dbf_files:
                log.warning("No .dbf found in zip")
                return []
            dbf_name = dbf_files[0]
            log.info(f"  Parsing DBF: {dbf_name}")
            with z.open(dbf_name) as f:
                raw = f.read()
            # write to temp, parse
            tmp = Path("/tmp/_hcad_temp.dbf")
            tmp.write_bytes(raw)
            try:
                for rec in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                    records.append(dict(rec))
            except Exception as e:
                log.warning(f"DBF parse error: {e}")
        return records

    def _parse_csv_from_zip(self, zip_bytes: bytes) -> list[dict]:
        """Extract and parse first .csv or .txt found in a zip."""
        records = []
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            csv_files = [n for n in z.namelist()
                         if n.lower().endswith((".csv", ".txt"))]
            if not csv_files:
                return []
            csv_name = csv_files[0]
            log.info(f"  Parsing CSV: {csv_name}")
            with z.open(csv_name) as f:
                text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                records.append(dict(row))
        return records

    def _ingest_records(self, raw_records: list[dict]):
        """Normalize field names and build lookups."""
        ingested = 0
        for rec in raw_records:
            # Normalize keys to upper
            r = {k.upper().strip(): str(v).strip() if v else "" for k, v in rec.items()}

            owner = (r.get("OWNER") or r.get("OWN1") or r.get("OWNERNAME") or "").strip().upper()
            if not owner:
                continue

            site_addr  = r.get("SITE_ADDR") or r.get("SITEADDR") or r.get("STREET_ADDR") or ""
            site_city  = r.get("SITE_CITY") or r.get("SITECITY") or "HOUSTON"
            site_zip   = r.get("SITE_ZIP")  or r.get("SITEZIP")  or r.get("ZIP4") or ""
            mail_addr  = r.get("ADDR_1")    or r.get("MAILADR1") or r.get("MAIL_ADDR") or site_addr
            mail_city  = r.get("CITY")      or r.get("MAILCITY") or r.get("MAIL_CITY") or site_city
            mail_state = r.get("STATE")     or r.get("MAILSTATE") or "TX"
            mail_zip   = r.get("ZIP")       or r.get("MAILZIP")  or r.get("MAIL_ZIP")  or site_zip
            acct_num   = r.get("ACCT")      or r.get("ACCOUNT")  or ""

            parcel = {
                "site_addr":  site_addr,
                "site_city":  site_city,
                "site_state": "TX",
                "site_zip":   site_zip,
                "mail_addr":  mail_addr,
                "mail_city":  mail_city,
                "mail_state": mail_state,
                "mail_zip":   mail_zip,
            }

            for variant in self._name_variants(owner):
                self.lookup[variant] = parcel

            if acct_num:
                self.acct_lookup[acct_num] = parcel

            ingested += 1

        log.info(f"  Ingested {ingested} parcel records → {len(self.lookup)} name keys")

    def load(self):
        """Try each known HCAD URL until one works."""
        for url in self.KNOWN_URLS:
            raw = self._try_download_zip(url)
            if not raw:
                continue
            # Try DBF first, then CSV
            records = self._parse_dbf_from_zip(raw)
            if not records:
                records = self._parse_csv_from_zip(raw)
            if records:
                self._ingest_records(records)
                return
        log.warning("Could not load HCAD bulk parcel data – address enrichment disabled")

    def find(self, owner_name: str) -> Optional[dict]:
        if not owner_name:
            return None
        for variant in self._name_variants(owner_name):
            if variant in self.lookup:
                return self.lookup[variant]
        return None


# ─── Scoring ──────────────────────────────────────────────────────────────────
def build_flags(record: dict, today: datetime) -> list[str]:
    flags = []
    cat = record.get("cat", "")
    doc_type = record.get("doc_type", "").upper()
    owner = (record.get("owner") or "").upper()
    amount = record.get("amount") or 0
    filed_str = record.get("filed") or ""

    if cat == "lp" and doc_type != "RELLP":
        flags.append("Lis pendens")
    if cat == "fc":
        flags.append("Pre-foreclosure")
    if cat == "jud":
        flags.append("Judgment lien")
    if cat in ("tax", "lien") and doc_type in ("TAXDEED", "LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")
    if doc_type in ("LNMECH",):
        flags.append("Mechanic lien")
    if cat == "probate":
        flags.append("Probate / estate")
    if any(x in owner for x in ("LLC", "LP ", "INC", "CORP", "LTD", "TRUST")):
        flags.append("LLC / corp owner")

    # New this week
    try:
        filed_dt = datetime.strptime(filed_str[:10], "%Y-%m-%d")
        if (today - filed_dt).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    return flags


def compute_score(record: dict, flags: list[str], today: datetime) -> int:
    score = SCORE_BASE
    score += len(flags) * SCORE_PER_FLAG

    # LP + FC combo bonus
    cat = record.get("cat", "")
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += SCORE_LP_FC_COMBO
    elif cat in ("lp", "fc") and len(flags) >= 2:
        score += SCORE_LP_FC_COMBO

    amount = 0
    try:
        amount = float(str(record.get("amount") or "0").replace(",", "").replace("$", ""))
    except Exception:
        pass

    if amount >= 100_000:
        score += SCORE_AMOUNT_100K
    elif amount >= 50_000:
        score += SCORE_AMOUNT_50K

    if "New this week" in flags:
        score += SCORE_NEW_WEEK

    if record.get("prop_address"):
        score += SCORE_HAS_ADDRESS

    return min(score, 100)


# ─── Harris County Clerk Scraper ──────────────────────────────────────────────
class HarrisClerkScraper:
    """
    Uses Playwright to scrape the Harris County Clerk public records portal.
    Handles ASP.NET __doPostBack, pagination, and search by doc type.
    """

    BASE_URL = "https://www.cclerk.hctx.net/PublicRecords.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back = days_back
        self.today = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.records: list[dict] = []

    async def _search_doc_type(self, page, doc_code: str, cat: str, cat_label: str):
        """Search for one document type and collect all result rows."""
        log.info(f"  Searching: {doc_code} ({cat_label})")

        # Navigate fresh each doc type to avoid stale state
        for attempt in range(3):
            try:
                await page.goto(self.BASE_URL, wait_until="networkidle", timeout=30000)
                break
            except PWTimeout:
                log.warning(f"  Timeout loading page (attempt {attempt+1})")
                if attempt == 2:
                    return

        try:
            # Select document type – look for a dropdown or input
            # Harris County Clerk uses a search form with instrument type
            await page.wait_for_selector("form", timeout=10000)

            # Try to find instrument type selector
            selectors_to_try = [
                'select[name*="InstrType"]',
                'select[name*="DocType"]',
                'select[id*="InstrType"]',
                'select[id*="DocType"]',
                'input[name*="InstrType"]',
            ]
            instr_sel = None
            for sel in selectors_to_try:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        instr_sel = sel
                        break
                except Exception:
                    pass

            if instr_sel and "select" in instr_sel:
                await page.select_option(instr_sel, doc_code)

            # Date range
            date_from = self.start_date.strftime("%m/%d/%Y")
            date_to   = self.today.strftime("%m/%d/%Y")

            date_fields = [
                ('input[name*="DateFrom"]', date_from),
                ('input[name*="StartDate"]', date_from),
                ('input[name*="DateTo"]', date_to),
                ('input[name*="EndDate"]', date_to),
            ]
            for sel, val in date_fields:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.fill(val)
                except Exception:
                    pass

            # Submit search
            submit_selectors = [
                'input[type="submit"]',
                'button[type="submit"]',
                'input[value*="Search"]',
                'button:has-text("Search")',
            ]
            for sel in submit_selectors:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=20000)
                        break
                except Exception:
                    pass

            # Scrape results (handle pagination)
            page_num = 0
            while True:
                page_num += 1
                html = await page.content()
                rows = self._parse_results_table(html, doc_code, cat, cat_label)
                log.info(f"    Page {page_num}: {len(rows)} rows")
                self.records.extend(rows)

                # Try to go to next page
                next_btn = await page.query_selector('a:has-text("Next"), input[value="Next >"]')
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15000)

        except Exception as e:
            log.warning(f"  Error scraping {doc_code}: {e}")

    def _parse_results_table(self, html: str, doc_code: str, cat: str, cat_label: str) -> list[dict]:
        """Parse the HTML results table from the clerk portal."""
        soup = BeautifulSoup(html, "lxml")
        rows_out = []

        # Find results table – Harris Clerk uses GridView-style tables
        tables = soup.find_all("table")
        result_table = None
        for tbl in tables:
            headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            if any(h in headers for h in ["instrument", "doc", "filed", "grantor", "grantee", "date"]):
                result_table = tbl
                break

        if not result_table:
            return []

        headers = [th.get_text(strip=True) for th in result_table.find_all("th")]

        def col(cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name.lower() in h.lower() and i < len(cells):
                        return cells[i].get_text(strip=True)
            return ""

        for tr in result_table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if not cells or all(not c.get_text(strip=True) for c in cells):
                continue

            # Extract doc number and link
            doc_num = col(cells, "instrument", "doc number", "doc#", "document")
            filed   = col(cells, "filed", "date", "record date")
            grantor = col(cells, "grantor", "owner", "from")
            grantee = col(cells, "grantee", "to")
            legal   = col(cells, "legal", "description", "legal desc")
            amount  = col(cells, "amount", "consideration", "value")

            # Normalize filed date to ISO
            filed_iso = ""
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    filed_iso = datetime.strptime(filed[:10], fmt).strftime("%Y-%m-%d")
                    break
                except Exception:
                    pass

            # Build clerk direct URL
            clerk_url = self.BASE_URL
            link_tag = None
            for cell in cells:
                a = cell.find("a", href=True)
                if a:
                    link_tag = a
                    break
            if link_tag:
                href = link_tag.get("href", "")
                if href.startswith("http"):
                    clerk_url = href
                elif href:
                    from urllib.parse import urljoin
                    clerk_url = urljoin(self.BASE_URL, href)

            if not doc_num and not grantor:
                continue

            rows_out.append({
                "doc_num":   doc_num,
                "doc_type":  doc_code,
                "filed":     filed_iso or filed,
                "cat":       cat,
                "cat_label": cat_label,
                "owner":     grantor,
                "grantee":   grantee,
                "amount":    self._parse_amount(amount),
                "legal":     legal,
                "clerk_url": clerk_url,
            })

        return rows_out

    def _parse_amount(self, raw: str) -> Optional[float]:
        if not raw:
            return None
        cleaned = re.sub(r"[^\d.]", "", raw)
        try:
            v = float(cleaned)
            return v if v > 0 else None
        except Exception:
            return None

    async def scrape_all(self) -> list[dict]:
        """Run playwright, iterate all doc types, return raw records."""
        log.info(f"Starting Harris Clerk scrape: {self.start_date.date()} → {self.today.date()}")

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await ctx.new_page()
            page.set_default_timeout(30000)

            for doc_code, (cat, cat_label) in DOC_TYPES.items():
                try:
                    await self._search_doc_type(page, doc_code, cat, cat_label)
                except Exception as e:
                    log.error(f"Fatal error on {doc_code}: {e}")

            await browser.close()

        log.info(f"Total raw clerk records: {len(self.records)}")
        return self.records


# ─── Alternative: Requests-based scraper (fallback / supplement) ──────────────
class HarrisClerkRequestsScraper:
    """
    Requests+BeautifulSoup scraper as fallback / supplement to Playwright.
    Handles ASP.NET __doPostBack for the Harris County Clerk.
    """

    SESSION_URL = "https://www.cclerk.hctx.net/PublicRecords.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back = days_back
        self.today = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.records: list[dict] = []

    def _get_viewstate(self, soup: BeautifulSoup) -> dict:
        """Extract ASP.NET hidden fields."""
        fields = {}
        for field_name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                           "__VIEWSTATEENCRYPTED"]:
            tag = soup.find("input", {"name": field_name})
            if tag:
                fields[field_name] = tag.get("value", "")
        return fields

    def _initial_load(self) -> Optional[BeautifulSoup]:
        for attempt in range(3):
            try:
                r = self.session.get(self.SESSION_URL, timeout=30)
                r.raise_for_status()
                return BeautifulSoup(r.text, "lxml")
            except Exception as e:
                log.warning(f"Initial load attempt {attempt+1}: {e}")
                time.sleep(2)
        return None

    def _post_search(self, soup: BeautifulSoup, doc_code: str) -> Optional[BeautifulSoup]:
        """POST a search form for a specific doc type."""
        vs = self._get_viewstate(soup)

        # Find all input/select fields and build base payload
        form = soup.find("form")
        if not form:
            return None

        payload = {}
        # Collect all inputs
        for inp in form.find_all(["input", "select"]):
            name = inp.get("name")
            if not name:
                continue
            if inp.name == "select":
                selected = inp.find("option", selected=True)
                payload[name] = selected.get("value", "") if selected else ""
            else:
                itype = inp.get("type", "text").lower()
                if itype in ("text", "hidden"):
                    payload[name] = inp.get("value", "")
                elif itype == "submit":
                    pass  # handled separately

        # Override with our search params
        payload.update(vs)

        # Set doc type (try common field names)
        for fn in ["ctl00$ContentPlaceHolder1$ddlInstrType",
                   "ddlInstrType", "InstrType", "DocType"]:
            if fn in payload:
                payload[fn] = doc_code

        # Set date range
        date_from = self.start_date.strftime("%m/%d/%Y")
        date_to   = self.today.strftime("%m/%d/%Y")
        for fn in ["ctl00$ContentPlaceHolder1$txtDateFrom",
                   "txtDateFrom", "DateFrom", "StartDate"]:
            if fn in payload:
                payload[fn] = date_from
        for fn in ["ctl00$ContentPlaceHolder1$txtDateTo",
                   "txtDateTo", "DateTo", "EndDate"]:
            if fn in payload:
                payload[fn] = date_to

        # Add search button event
        payload["__EVENTTARGET"] = ""
        payload["__EVENTARGUMENT"] = ""

        # Find submit button
        submit = form.find("input", {"type": "submit"})
        if submit and submit.get("name"):
            payload[submit["name"]] = submit.get("value", "Search")

        try:
            r = self.session.post(self.SESSION_URL, data=payload, timeout=60)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:
            log.warning(f"POST search failed for {doc_code}: {e}")
            return None

    def _parse_table(self, soup: BeautifulSoup, doc_code: str, cat: str, cat_label: str) -> list[dict]:
        """Reuse same parse logic as Playwright scraper."""
        html = str(soup)
        scraper = HarrisClerkScraper()
        return scraper._parse_results_table(html, doc_code, cat, cat_label)

    def scrape_all(self) -> list[dict]:
        log.info("Starting requests-based clerk scrape (fallback)")
        soup = self._initial_load()
        if not soup:
            log.error("Could not load clerk portal")
            return []

        for doc_code, (cat, cat_label) in DOC_TYPES.items():
            log.info(f"  Searching: {doc_code}")
            try:
                result_soup = self._post_search(soup, doc_code)
                if result_soup:
                    rows = self._parse_table(result_soup, doc_code, cat, cat_label)
                    log.info(f"    Found {len(rows)} rows")
                    self.records.extend(rows)
                    # Reload base soup for next search
                    soup = self._initial_load() or soup
            except Exception as e:
                log.error(f"Error on {doc_code}: {e}")

        log.info(f"Total requests records: {len(self.records)}")
        return self.records


# ─── Record Enrichment & Dedup ────────────────────────────────────────────────
def enrich_records(raw_records: list[dict], parcel: HCADParcelLookup) -> list[dict]:
    """
    Enrich raw clerk records with parcel address data and compute scores.
    Deduplicates by (doc_num, doc_type).
    """
    today = datetime.now()
    seen = set()
    enriched = []

    for rec in raw_records:
        key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
        if key in seen:
            continue
        seen.add(key)

        # Look up parcel data by owner name
        owner = rec.get("owner", "")
        p = parcel.find(owner) if owner else None

        rec["prop_address"] = p["site_addr"]  if p else ""
        rec["prop_city"]    = p["site_city"]  if p else "Houston"
        rec["prop_state"]   = p["site_state"] if p else "TX"
        rec["prop_zip"]     = p["site_zip"]   if p else ""
        rec["mail_address"] = p["mail_addr"]  if p else ""
        rec["mail_city"]    = p["mail_city"]  if p else ""
        rec["mail_state"]   = p["mail_state"] if p else "TX"
        rec["mail_zip"]     = p["mail_zip"]   if p else ""

        # Fallback: use prop address for mail if no separate mail
        if not rec["mail_address"] and rec["prop_address"]:
            rec["mail_address"] = rec["prop_address"]
            rec["mail_city"]    = rec["prop_city"]
            rec["mail_state"]   = rec["prop_state"]
            rec["mail_zip"]     = rec["prop_zip"]

        flags = build_flags(rec, today)
        score = compute_score(rec, flags, today)

        rec["flags"] = flags
        rec["score"] = score

        enriched.append(rec)

    # Sort: highest score first
    enriched.sort(key=lambda r: r["score"], reverse=True)
    log.info(f"Enriched {len(enriched)} unique records")
    return enriched


# ─── Output ───────────────────────────────────────────────────────────────────
def save_records(records: list[dict], today: datetime, days_back: int):
    """Save to dashboard/records.json and data/records.json."""
    with_address = sum(1 for r in records if r.get("prop_address"))

    payload = {
        "fetched_at":  today.isoformat(),
        "source":      "Harris County Clerk / HCAD",
        "date_range":  {
            "from": (today - timedelta(days=days_back)).strftime("%Y-%m-%d"),
            "to":   today.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        out_path = d / "records.json"
        out_path.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Saved {len(records)} records → {out_path}")


def export_ghl_csv(records: list[dict], today: datetime):
    """Export GHL-compatible CSV."""
    columns = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    rows = []
    for r in records:
        owner = r.get("owner", "")
        parts = owner.strip().split()
        first = parts[0] if parts else ""
        last  = " ".join(parts[1:]) if len(parts) > 1 else ""

        amount = r.get("amount")
        amount_str = f"${amount:,.2f}" if amount else ""

        rows.append({
            "First Name":             first,
            "Last Name":              last,
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
            "Amount/Debt Owed":       amount_str,
            "Seller Score":           r.get("score", 0),
            "Motivated Seller Flags": "; ".join(r.get("flags", [])),
            "Source":                 "Harris County Clerk",
            "Public Records URL":     r.get("clerk_url", ""),
        })

    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        csv_path = d / f"ghl_export_{today.strftime('%Y%m%d')}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"GHL CSV → {csv_path}")


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main():
    today     = datetime.now()
    days_back = int(os.environ.get("DAYS_BACK", "7"))

    log.info("=" * 60)
    log.info("Harris County Motivated Seller Scraper")
    log.info(f"Date range: last {days_back} days")
    log.info("=" * 60)

    # 1. Load HCAD parcel data
    log.info("\n[1/3] Loading HCAD parcel data...")
    parcel = HCADParcelLookup()
    parcel.load()

    # 2. Scrape clerk portal
    log.info("\n[2/3] Scraping Harris County Clerk portal...")
    raw_records = []

    # Try Playwright first
    try:
        playwright_scraper = HarrisClerkScraper(days_back=days_back)
        raw_records = await playwright_scraper.scrape_all()
    except Exception as e:
        log.warning(f"Playwright scraper failed: {e} — falling back to requests")

    # Fallback / supplement with requests scraper
    if not raw_records:
        try:
            req_scraper = HarrisClerkRequestsScraper(days_back=days_back)
            raw_records = req_scraper.scrape_all()
        except Exception as e:
            log.error(f"Requests scraper also failed: {e}")

    if not raw_records:
        log.warning("No records scraped. Writing empty output.")
        raw_records = []

    # 3. Enrich + score
    log.info("\n[3/3] Enriching and scoring records...")
    records = enrich_records(raw_records, parcel)

    # 4. Save
    log.info("\nSaving outputs...")
    save_records(records, today, days_back)
    export_ghl_csv(records, today)

    log.info("\n✓ Done!")
    log.info(f"  Total records:        {len(records)}")
    log.info(f"  With property addr:   {sum(1 for r in records if r.get('prop_address'))}")
    log.info(f"  Score ≥ 70 (hot):     {sum(1 for r in records if r.get('score', 0) >= 70)}")
    log.info(f"  Score ≥ 50 (warm):    {sum(1 for r in records if 50 <= r.get('score', 0) < 70)}")


if __name__ == "__main__":
    asyncio.run(main())
