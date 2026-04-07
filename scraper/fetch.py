"""
Harris County Motivated Seller Lead Scraper
fetch.py - Production ready, all issues resolved
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

CLERK_URL    = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"
GDRIVE_ID    = "1wV4EW-uxasZUjkc_wxOr-UjVKUTK5-qh"
FILE_NUMBER_RE = re.compile(r'RP-\d{4}-\d+')

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
    def __init__(self):
        self.lookup: dict = {}

    def _normalize(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.strip().upper())

    def _variants(self, name: str) -> list:
        n = self._normalize(name)
        parts = n.split()
        v = [n]
        if len(parts) >= 2:
            v.append(f"{parts[-1]} {' '.join(parts[:-1])}")
            v.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return v

    def _download_gdrive(self) -> Optional[bytes]:
        """Download HCAD file from Google Drive handling large file warning."""
        try:
            log.info(f"Downloading HCAD from Google Drive...")
            session = requests.Session()
            session.headers.update({"User-Agent": "Mozilla/5.0"})

            # Try direct usercontent URL first (works for large files)
            urls = [
                f"https://drive.usercontent.google.com/download?id={GDRIVE_ID}&export=download&confirm=t",
                f"https://drive.google.com/uc?export=download&id={GDRIVE_ID}&confirm=t",
            ]
            for url in urls:
                try:
                    r = session.get(url, timeout=300)
                    if r.status_code == 200 and r.content[:2] == b'PK':
                        log.info(f"  ✓ Downloaded {len(r.content)/1e6:.1f} MB")
                        return r.content
                except Exception as e:
                    log.warning(f"  URL failed: {e}")

            # Fallback: get warning page then confirm
            r = session.get(
                f"https://drive.google.com/uc?export=download&id={GDRIVE_ID}",
                timeout=60)
            # Find uuid token
            m = re.search(r'name="uuid" value="([^"]+)"', r.text)
            if m:
                uuid = m.group(1)
                r2 = session.get(
                    f"https://drive.usercontent.google.com/download"
                    f"?id={GDRIVE_ID}&export=download&authuser=0&confirm=t&uuid={uuid}",
                    timeout=300)
                if r2.status_code == 200 and r2.content[:2] == b'PK':
                    log.info(f"  ✓ Downloaded {len(r2.content)/1e6:.1f} MB")
                    return r2.content
        except Exception as e:
            log.warning(f"  Google Drive download failed: {e}")
        return None

    def _parse_zip(self, data: bytes) -> list:
        records = []
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                names = z.namelist()
                # Try DBF first
                dbf_files = [n for n in names if n.lower().endswith(".dbf")]
                if dbf_files and DBF:
                    with z.open(dbf_files[0]) as f:
                        raw = f.read()
                    tmp = Path("/tmp/_hcad.dbf")
                    tmp.write_bytes(raw)
                    for rec in DBF(str(tmp), encoding="latin-1",
                                   ignore_missing_memofile=True):
                        records.append(dict(rec))
                    return records
                # Try CSV/TXT
                csv_files = [n for n in names
                             if n.lower().endswith((".csv", ".txt"))]
                if csv_files:
                    with z.open(csv_files[0]) as f:
                        text = f.read().decode("latin-1", errors="replace")
                    for row in csv.DictReader(io.StringIO(text)):
                        records.append(dict(row))
        except Exception as e:
            log.warning(f"Zip parse error: {e}")
        return records

    def _ingest(self, raw: list):
        count = 0
        for rec in raw:
            try:
                r = {k.upper().strip(): str(v).strip() if v else ""
                     for k, v in rec.items()}
                owner = (r.get("OWNER") or r.get("OWN1") or "").upper().strip()
                if not owner:
                    continue
                parcel = {
                    "site_addr":  r.get("SITE_ADDR") or r.get("SITEADDR") or "",
                    "site_city":  r.get("SITE_CITY") or "HOUSTON",
                    "site_state": "TX",
                    "site_zip":   r.get("SITE_ZIP")  or r.get("SITEZIP") or "",
                    "mail_addr":  r.get("ADDR_1")    or r.get("MAILADR1") or "",
                    "mail_city":  r.get("CITY")      or r.get("MAILCITY") or "",
                    "mail_state": r.get("STATE")     or "TX",
                    "mail_zip":   r.get("ZIP")       or r.get("MAILZIP") or "",
                }
                for v in self._variants(owner):
                    self.lookup[v] = parcel
                count += 1
            except Exception:
                continue
        log.info(f"  Ingested {count} parcels → {len(self.lookup)} name keys")

    def load(self):
        data = self._download_gdrive()
        if data:
            records = self._parse_zip(data)
            if records:
                self._ingest(records)
                return
        log.warning("HCAD unavailable — running without address enrichment")

    def find(self, name: str) -> Optional[dict]:
        if not name:
            return None
        for v in self._variants(name):
            if v in self.lookup:
                return self.lookup[v]
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
    BASE_URL = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back  = days_back
        self.today      = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.records: list = []

    def _parse_results_page(self, html: str, doc_code: str,
                            cat: str, cat_label: str) -> list:
        """
        Harris County renders results as 137+ separate mini-tables per page.
        The page text contains all data linearly — we parse by finding
        RP-YYYY-NNNNN file number patterns and extracting surrounding context.
        
        Structure per record (from debug log):
        Table with header: File Number | File Date | Type Vol Page | Names | Legal | Pgs | Film Code
        Then nested tables for Names and Legal per row.
        
        Strategy: find the main results table by header, parse each TR.
        Each data TR has cells: [file_num, file_date, type, names_block, legal_block, pgs, film_link]
        But names and legal are in nested sub-tables within those cells.
        """
        soup = BeautifulSoup(html, "lxml")
        rows_out = []

        # Find ALL text nodes matching RP-YYYY-NNNNN pattern
        # Then work backwards to find the enclosing table row
        
        # Approach: find the header table first, then get all subsequent rows
        header_table = None
        all_tables = soup.find_all("table")
        
        for tbl in all_tables:
            txt = tbl.get_text(" ")
            if "File Number" in txt and "File Date" in txt and "Film Code" in txt:
                header_table = tbl
                break

        if not header_table:
            return []

        # Get all rows in this table
        rows = header_table.find_all("tr")
        
        for tr in rows:
            try:
                cells = tr.find_all("td")
                if len(cells) < 5:
                    continue

                # Cell 0: File Number
                file_num = cells[0].get_text(strip=True)
                if not FILE_NUMBER_RE.match(file_num):
                    continue

                # Cell 1: File Date
                raw_date = cells[1].get_text(strip=True)
                filed_iso = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                    try:
                        filed_iso = datetime.strptime(
                            raw_date[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass

                # Cell 2: Type (instrument link)
                type_link = cells[2].find("a")
                rec_type  = type_link.get_text(strip=True) if type_link else doc_code

                # Cell 3: Names — contains nested tables with Grantor/Grantee rows
                grantor  = ""
                grantees = []
                names_cell = cells[3]
                cell_text  = names_cell.get_text("\n")
                for line in cell_text.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    low = line.lower()
                    if low.startswith("grantor"):
                        # Strip "Grantor :" or "Grantor:"
                        grantor = re.sub(r'^grantor\s*:\s*', '', line,
                                         flags=re.IGNORECASE).strip()
                    elif low.startswith("grantee"):
                        g = re.sub(r'^grantee\s*:\s*', '', line,
                                   flags=re.IGNORECASE).strip()
                        if g:
                            grantees.append(g)

                # Cell 4: Legal Description
                legal = ""
                if len(cells) > 4:
                    legal = cells[4].get_text(" ", strip=True)

                # Last cell: Film Code link → direct URL
                clerk_url = self.BASE_URL
                last_cell = cells[-1]
                a = last_cell.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    if href.startswith("http"):
                        clerk_url = href
                    elif href:
                        clerk_url = "https://www.cclerk.hctx.net" + href

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

    async def _search_one(self, page, doc_code: str, cat: str, cat_label: str):
        log.info(f"  → {doc_code} ({cat_label})")

        for attempt in range(3):
            try:
                await page.goto(self.BASE_URL,
                                wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)
                break
            except PWTimeout:
                if attempt == 2:
                    return

        try:
            date_from = self.start_date.strftime("%m/%d/%Y")
            date_to   = self.today.strftime("%m/%d/%Y")

            # Exact field IDs confirmed from debug log
            await page.fill('#ctl00_ContentPlaceHolder1_txtInstrument', doc_code)
            await page.fill('#ctl00_ContentPlaceHolder1_txtFrom', date_from)
            await page.fill('#ctl00_ContentPlaceHolder1_txtTo', date_to)

            log.info(f"    Filled: {doc_code} | {date_from} → {date_to}")

            # Click search
            await page.click('input[value="Search"]')
            await page.wait_for_load_state("domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)

            # Paginate
            page_num = 0
            while True:
                page_num += 1
                html = await page.content()
                rows = self._parse_results_page(html, doc_code, cat, cat_label)
                log.info(f"    Page {page_num}: {len(rows)} rows")
                self.records.extend(rows)

                next_btn = await page.query_selector(
                    'input[value="NEXT"], input[value="Next"],'
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
                    "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
                ),
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

        log.info(f"Total records: {len(self.records)}")
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
            rec["prop_state"]   = "TX"
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
    log.info(f"Enriched {len(enriched)} unique records")
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
            parts = (r.get("owner") or "").strip().split()
            amount = r.get("amount")
            rows.append({
                "First Name":             parts[0] if parts else "",
                "Last Name":              " ".join(parts[1:]) if len(parts)>1 else "",
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
        log.info(f"GHL CSV → {path}")


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
        log.warning(f"HCAD failed: {e}")

    log.info("\n[2/3] Scraping Harris County Clerk...")
    raw = []
    try:
        raw = await HarrisClerkScraper(days_back=days_back).scrape_all()
    except Exception as e:
        log.error(f"Scraper failed: {e}")

    log.info("\n[3/3] Enriching and scoring...")
    records = enrich_records(raw or [], parcel)

    log.info("\nSaving...")
    save_records(records, today, days_back)
    export_ghl_csv(records, today)

    log.info(f"\n✓ Done! Total={len(records)} | "
             f"Hot={sum(1 for r in records if r.get('score',0)>=70)} | "
             f"Warm={sum(1 for r in records if 50<=r.get('score',0)<70)} | "
             f"WithAddr={sum(1 for r in records if r.get('prop_address'))}")


if __name__ == "__main__":
    asyncio.run(main())
