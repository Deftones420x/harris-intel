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

CLERK_URL = "https://www.cclerk.hctx.net/PublicRecords.aspx"

DOC_TYPES = {
    "LP":      ("lp",      "Lis Pendens"),
    "RELLP":   ("lp",      "Release Lis Pendens"),
    "NOFC":    ("fc",      "Notice of Foreclosure"),
    "NOF":     ("fc",      "Notice of Foreclosure"),
    "TAXDEED": ("tax",     "Tax Deed"),
    "TAXLIEN": ("tax",     "Tax Lien"),
    "JUD":     ("jud",     "Judgment"),
    "CCJ":     ("jud",     "Certified Judgment"),
    "DRJUD":   ("jud",     "Domestic Relations Judgment"),
    "LNCORPTX":("lien",   "Corp Tax Lien"),
    "LNIRS":   ("lien",    "IRS Lien"),
    "LNFED":   ("lien",    "Federal Lien"),
    "LN":      ("lien",    "Lien"),
    "LNMECH":  ("lien",    "Mechanic Lien"),
    "LNHOA":   ("lien",    "HOA Lien"),
    "MEDLN":   ("lien",    "Medicaid Lien"),
    "PRO":     ("probate", "Probate"),
    "PROB":    ("probate", "Probate"),
    "NOC":     ("notice",  "Notice of Commencement"),
}

SCORE_BASE        = 30
SCORE_PER_FLAG    = 10
SCORE_LP_FC_COMBO = 20
SCORE_AMOUNT_100K = 15
SCORE_AMOUNT_50K  = 10
SCORE_NEW_WEEK    = 5
SCORE_HAS_ADDRESS = 5

OUTPUT_DIRS = [Path("dashboard"), Path("data")]


class HCADParcelLookup:
    KNOWN_URLS = [
        "https://pdata.hcad.org/download/2024/real_acct.zip",
        "https://pdata.hcad.org/Desc/2024/real_acct.zip",
        "https://pdata.hcad.org/download/2024/account_info.zip",
        "https://pdata.hcad.org/Desc/2024/account_info.zip",
    ]

    def __init__(self):
        self.lookup: dict = {}
        self.acct_lookup: dict = {}

    def _normalize_name(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.strip().upper())

    def _name_variants(self, full_name: str) -> list:
        n = self._normalize_name(full_name)
        parts = n.split()
        variants = [n]
        if len(parts) >= 2:
            variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return variants

    def _try_download_zip(self, url: str) -> Optional[bytes]:
        try:
            log.info(f"Trying HCAD URL: {url}")
            r = requests.get(url, timeout=120, stream=True)
            if r.status_code == 200 and len(r.content) > 10000:
                if r.content[:2] == b'PK':
                    log.info(f"  ✓ Downloaded {len(r.content)/1e6:.1f} MB")
                    return r.content
                else:
                    log.warning(f"  ✗ Not a zip file (got HTML/text)")
                    return None
        except Exception as e:
            log.warning(f"  ✗ {e}")
        return None

    def _parse_dbf_from_zip(self, zip_bytes: bytes) -> list:
        if DBF is None:
            return []
        records = []
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                dbf_files = [n for n in z.namelist() if n.lower().endswith(".dbf")]
                if not dbf_files:
                    return []
                with z.open(dbf_files[0]) as f:
                    raw = f.read()
            tmp = Path("/tmp/_hcad_temp.dbf")
            tmp.write_bytes(raw)
            for rec in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                records.append(dict(rec))
        except Exception as e:
            log.warning(f"DBF parse error: {e}")
        return records

    def _parse_csv_from_zip(self, zip_bytes: bytes) -> list:
        records = []
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                csv_files = [n for n in z.namelist()
                             if n.lower().endswith((".csv", ".txt"))]
                if not csv_files:
                    return []
                with z.open(csv_files[0]) as f:
                    text = f.read().decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                records.append(dict(row))
        except Exception as e:
            log.warning(f"CSV parse error: {e}")
        return records

    def _ingest_records(self, raw_records: list):
        ingested = 0
        for rec in raw_records:
            try:
                r = {k.upper().strip(): str(v).strip() if v else ""
                     for k, v in rec.items()}
                owner = (r.get("OWNER") or r.get("OWN1") or
                         r.get("OWNERNAME") or "").strip().upper()
                if not owner:
                    continue
                site_addr  = r.get("SITE_ADDR") or r.get("SITEADDR") or ""
                site_city  = r.get("SITE_CITY") or r.get("SITECITY") or "HOUSTON"
                site_zip   = r.get("SITE_ZIP")  or r.get("SITEZIP")  or ""
                mail_addr  = r.get("ADDR_1")    or r.get("MAILADR1") or site_addr
                mail_city  = r.get("CITY")      or r.get("MAILCITY") or site_city
                mail_state = r.get("STATE")     or "TX"
                mail_zip   = r.get("ZIP")       or r.get("MAILZIP")  or site_zip
                acct_num   = r.get("ACCT")      or r.get("ACCOUNT")  or ""
                parcel = {
                    "site_addr": site_addr, "site_city": site_city,
                    "site_state": "TX",     "site_zip": site_zip,
                    "mail_addr": mail_addr, "mail_city": mail_city,
                    "mail_state": mail_state, "mail_zip": mail_zip,
                }
                for variant in self._name_variants(owner):
                    self.lookup[variant] = parcel
                if acct_num:
                    self.acct_lookup[acct_num] = parcel
                ingested += 1
            except Exception:
                continue
        log.info(f"  Ingested {ingested} records → {len(self.lookup)} name keys")

    def load(self):
        for url in self.KNOWN_URLS:
            try:
                raw = self._try_download_zip(url)
                if not raw:
                    continue
                records = []
                try:
                    records = self._parse_dbf_from_zip(raw)
                except Exception as e:
                    log.warning(f"  DBF failed: {e}")
                if not records:
                    try:
                        records = self._parse_csv_from_zip(raw)
                    except Exception as e:
                        log.warning(f"  CSV failed: {e}")
                if records:
                    self._ingest_records(records)
                    return
            except Exception as e:
                log.warning(f"  Failed {url}: {e}")
                continue
        log.warning("HCAD parcel data unavailable — running without address enrichment")

    def find(self, owner_name: str) -> Optional[dict]:
        if not owner_name:
            return None
        try:
            for variant in self._name_variants(owner_name):
                if variant in self.lookup:
                    return self.lookup[variant]
        except Exception:
            pass
        return None


def build_flags(record: dict, today: datetime) -> list:
    flags = []
    try:
        cat       = record.get("cat", "")
        doc_type  = record.get("doc_type", "").upper()
        owner     = (record.get("owner") or "").upper()
        filed_str = record.get("filed") or ""
        if cat == "lp" and doc_type != "RELLP":
            flags.append("Lis pendens")
        if cat == "fc":
            flags.append("Pre-foreclosure")
        if cat == "jud":
            flags.append("Judgment lien")
        if doc_type in ("TAXDEED", "LNCORPTX", "LNIRS", "LNFED"):
            flags.append("Tax lien")
        if doc_type == "LNMECH":
            flags.append("Mechanic lien")
        if cat == "probate":
            flags.append("Probate / estate")
        if any(x in owner for x in ("LLC", "LP ", "INC", "CORP", "LTD", "TRUST")):
            flags.append("LLC / corp owner")
        try:
            filed_dt = datetime.strptime(filed_str[:10], "%Y-%m-%d")
            if (today - filed_dt).days <= 7:
                flags.append("New this week")
        except Exception:
            pass
    except Exception:
        pass
    return flags


def compute_score(record: dict, flags: list, today: datetime) -> int:
    try:
        score = SCORE_BASE + len(flags) * SCORE_PER_FLAG
        cat = record.get("cat", "")
        if "Lis pendens" in flags and "Pre-foreclosure" in flags:
            score += SCORE_LP_FC_COMBO
        elif cat in ("lp", "fc") and len(flags) >= 2:
            score += SCORE_LP_FC_COMBO
        amount = 0
        try:
            amount = float(str(record.get("amount") or "0")
                           .replace(",", "").replace("$", ""))
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
    except Exception:
        return SCORE_BASE


class HarrisClerkScraper:
    BASE_URL = "https://www.cclerk.hctx.net/PublicRecords.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back  = days_back
        self.today      = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.records: list = []

    async def _search_doc_type(self, page, doc_code: str, cat: str, cat_label: str):
        log.info(f"  Searching: {doc_code} ({cat_label})")
        for attempt in range(3):
            try:
                await page.goto(self.BASE_URL, wait_until="networkidle", timeout=30000)
                break
            except PWTimeout:
                if attempt == 2:
                    return
        try:
            await page.wait_for_selector("form", timeout=10000)
            for sel in ['select[name*="InstrType"]', 'select[name*="DocType"]',
                        'select[id*="InstrType"]',   'select[id*="DocType"]']:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await page.select_option(sel, doc_code)
                        break
                except Exception:
                    pass

            date_from = self.start_date.strftime("%m/%d/%Y")
            date_to   = self.today.strftime("%m/%d/%Y")
            for sel, val in [
                ('input[name*="DateFrom"]', date_from),
                ('input[name*="StartDate"]', date_from),
                ('input[name*="DateTo"]', date_to),
                ('input[name*="EndDate"]', date_to),
            ]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.fill(val)
                except Exception:
                    pass

            for sel in ['input[type="submit"]', 'button[type="submit"]',
                        'input[value*="Search"]', 'button:has-text("Search")']:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=20000)
                        break
                except Exception:
                    pass

            page_num = 0
            while True:
                page_num += 1
                html = await page.content()
                rows = self._parse_results_table(html, doc_code, cat, cat_label)
                log.info(f"    Page {page_num}: {len(rows)} rows")
                self.records.extend(rows)
                next_btn = await page.query_selector(
                    'a:has-text("Next"), input[value="Next >"]')
                if not next_btn:
                    break
                await next_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            log.warning(f"  Error on {doc_code}: {e}")

    def _parse_results_table(self, html: str, doc_code: str,
                             cat: str, cat_label: str) -> list:
        soup = BeautifulSoup(html, "lxml")
        rows_out = []
        result_table = None
        for tbl in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            if any(h in headers for h in
                   ["instrument", "doc", "filed", "grantor", "grantee", "date"]):
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
            try:
                cells = tr.find_all(["td", "th"])
                if not cells or all(not c.get_text(strip=True) for c in cells):
                    continue
                doc_num = col(cells, "instrument", "doc number", "doc#", "document")
                filed   = col(cells, "filed", "date", "record date")
                grantor = col(cells, "grantor", "owner", "from")
                grantee = col(cells, "grantee", "to")
                legal   = col(cells, "legal", "description")
                amount  = col(cells, "amount", "consideration", "value")
                filed_iso = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                    try:
                        filed_iso = datetime.strptime(filed[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass
                clerk_url = self.BASE_URL
                for cell in cells:
                    a = cell.find("a", href=True)
                    if a:
                        href = a.get("href", "")
                        if href.startswith("http"):
                            clerk_url = href
                        elif href:
                            from urllib.parse import urljoin
                            clerk_url = urljoin(self.BASE_URL, href)
                        break
                if not doc_num and not grantor:
                    continue
                rows_out.append({
                    "doc_num": doc_num, "doc_type": doc_code,
                    "filed": filed_iso or filed,
                    "cat": cat, "cat_label": cat_label,
                    "owner": grantor, "grantee": grantee,
                    "amount": self._parse_amount(amount),
                    "legal": legal, "clerk_url": clerk_url,
                })
            except Exception:
                continue
        return rows_out

    def _parse_amount(self, raw: str) -> Optional[float]:
        try:
            cleaned = re.sub(r"[^\d.]", "", raw)
            v = float(cleaned)
            return v if v > 0 else None
        except Exception:
            return None

    async def scrape_all(self) -> list:
        log.info(f"Playwright scrape: {self.start_date.date()} → {self.today.date()}")
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
        log.info(f"Total Playwright records: {len(self.records)}")
        return self.records


class HarrisClerkRequestsScraper:
    SESSION_URL = "https://www.cclerk.hctx.net/PublicRecords.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back  = days_back
        self.today      = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.session    = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        })
        self.records: list = []

    def _get_viewstate(self, soup) -> dict:
        fields = {}
        for fn in ["__VIEWSTATE", "__VIEWSTATEGENERATOR",
                   "__EVENTVALIDATION", "__VIEWSTATEENCRYPTED"]:
            tag = soup.find("input", {"name": fn})
            if tag:
                fields[fn] = tag.get("value", "")
        return fields

    def _initial_load(self):
        for attempt in range(3):
            try:
                r = self.session.get(self.SESSION_URL, timeout=30)
                r.raise_for_status()
                return BeautifulSoup(r.text, "lxml")
            except Exception as e:
                log.warning(f"Initial load attempt {attempt+1}: {e}")
                time.sleep(2)
        return None

    def _post_search(self, soup, doc_code: str):
        try:
            vs   = self._get_viewstate(soup)
            form = soup.find("form")
            if not form:
                return None
            payload = {}
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
            payload.update(vs)
            payload["__EVENTTARGET"] = ""
            payload["__EVENTARGUMENT"] = ""
            for fn in ["ctl00$ContentPlaceHolder1$ddlInstrType",
                       "ddlInstrType", "InstrType", "DocType"]:
                if fn in payload:
                    payload[fn] = doc_code
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
            submit = form.find("input", {"type": "submit"})
            if submit and submit.get("name"):
                payload[submit["name"]] = submit.get("value", "Search")
            r = self.session.post(self.SESSION_URL, data=payload, timeout=60)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:
            log.warning(f"POST failed for {doc_code}: {e}")
            return None

    def scrape_all(self) -> list:
        log.info("Requests fallback scrape")
        soup = self._initial_load()
        if not soup:
            return []
        pw = HarrisClerkScraper()
        for doc_code, (cat, cat_label) in DOC_TYPES.items():
            try:
                result_soup = self._post_search(soup, doc_code)
                if result_soup:
                    rows = pw._parse_results_table(
                        str(result_soup), doc_code, cat, cat_label)
                    log.info(f"  {doc_code}: {len(rows)} rows")
                    self.records.extend(rows)
                    soup = self._initial_load() or soup
            except Exception as e:
                log.error(f"Error on {doc_code}: {e}")
        return self.records


def enrich_records(raw_records: list, parcel: HCADParcelLookup) -> list:
    today    = datetime.now()
    seen     = set()
    enriched = []
    for rec in raw_records:
        try:
            key = (rec.get("doc_num", ""), rec.get("doc_type", ""))
            if key in seen:
                continue
            seen.add(key)
            owner = rec.get("owner", "")
            p     = parcel.find(owner) if owner else None
            rec["prop_address"] = p["site_addr"]  if p else ""
            rec["prop_city"]    = p["site_city"]  if p else "Houston"
            rec["prop_state"]   = p["site_state"] if p else "TX"
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
            score        = compute_score(rec, flags, today)
            rec["flags"] = flags
            rec["score"] = score
            enriched.append(rec)
        except Exception as e:
            log.warning(f"Enrich error: {e}")
            continue
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)
    log.info(f"Enriched {len(enriched)} unique records")
    return enriched


def save_records(records: list, today: datetime, days_back: int):
    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at":   today.isoformat(),
        "source":       "Harris County Clerk / HCAD",
        "date_range": {
            "from": (today - timedelta(days=days_back)).strftime("%Y-%m-%d"),
            "to":   today.strftime("%Y-%m-%d"),
        },
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }
    for d in OUTPUT_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        out = d / "records.json"
        out.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Saved → {out}")


def export_ghl_csv(records: list, today: datetime):
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
        try:
            parts  = (r.get("owner") or "").strip().split()
            first  = parts[0] if parts else ""
            last   = " ".join(parts[1:]) if len(parts) > 1 else ""
            amount = r.get("amount")
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
        csv_path = d / f"ghl_export_{today.strftime('%Y%m%d')}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"GHL CSV → {csv_path}")


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
        log.warning(f"HCAD load failed: {e} — continuing without parcel data")

    log.info("\n[2/3] Scraping Harris County Clerk portal...")
    raw_records = []
    try:
        raw_records = await HarrisClerkScraper(days_back=days_back).scrape_all()
    except Exception as e:
        log.warning(f"Playwright failed: {e} — trying requests fallback")
    if not raw_records:
        try:
            raw_records = HarrisClerkRequestsScraper(days_back=days_back).scrape_all()
        except Exception as e:
            log.error(f"Requests scraper failed: {e}")

    log.info("\n[3/3] Enriching and scoring records...")
    records = enrich_records(raw_records or [], parcel)

    log.info("\nSaving outputs...")
    save_records(records, today, days_back)
    export_ghl_csv(records, today)

    log.info("\n✓ Done!")
    log.info(f"  Total:           {len(records)}")
    log.info(f"  With address:    {sum(1 for r in records if r.get('prop_address'))}")
    log.info(f"  Hot (≥70):       {sum(1 for r in records if r.get('score', 0) >= 70)}")
    log.info(f"  Warm (50-69):    {sum(1 for r in records if 50 <= r.get('score', 0) < 70)}")


if __name__ == "__main__":
    asyncio.run(main())
