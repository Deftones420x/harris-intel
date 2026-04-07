"""
Harris County Motivated Seller Lead Scraper
Scrapes RP.aspx using Playwright (real browser session required)
"""

import asyncio
import json
import csv
import os
import re
import sys
import time
import io
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

CLERK_URL = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"

# Real Harris County instrument codes from Codes.aspx
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
    KNOWN_URLS = [
        # Google Drive direct download (Real_Account_Owner.zip)
        "https://drive.google.com/uc?export=download&id=1wV4EW-uxasZUjkc_wxOr-UjVKUTK5-qh",
        # HCAD fallbacks
        "https://pdata.hcad.org/download/2024/real_acct.zip",
        "https://pdata.hcad.org/Desc/2024/real_acct.zip",
    ]

    def __init__(self):
        self.lookup: dict = {}

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
            log.info(f"Trying URL: {url[:60]}...")
            session = requests.Session()
            r = session.get(url, timeout=300, stream=True)
            # Handle Google Drive large file warning page
            if b"virus scan warning" in r.content[:500].lower() or                b"download_warning" in r.url.lower() or                (r.status_code == 200 and r.content[:2] != b'PK' and b'google' in url.encode()):
                # Extract confirmation token and retry
                import re as _re
                token = None
                for k, v in r.cookies.items():
                    if k.startswith("download_warning"):
                        token = v
                        break
                if not token:
                    m = _re.search(r'confirm=([0-9A-Za-z_]+)', r.text)
                    if m:
                        token = m.group(1)
                if token:
                    params = {"confirm": token}
                    if "id=" in url:
                        fid = url.split("id=")[-1]
                        params["id"] = fid
                    r = session.get(
                        "https://drive.google.com/uc?export=download",
                        params=params, timeout=300, stream=True)
            content = r.content
            if r.status_code == 200 and content[:2] == b'PK':
                log.info(f"  ✓ {len(content)/1e6:.1f} MB")
                return content
            log.warning(f"  ✗ Not a zip (got {len(content)} bytes, starts: {content[:4]})")
        except Exception as e:
            log.warning(f"  ✗ {e}")
        return None

    def _parse_zip(self, zip_bytes: bytes) -> list:
        records = []
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                # Try DBF
                dbf_files = [n for n in z.namelist() if n.lower().endswith(".dbf")]
                if dbf_files and DBF:
                    with z.open(dbf_files[0]) as f:
                        raw = f.read()
                    tmp = Path("/tmp/_hcad.dbf")
                    tmp.write_bytes(raw)
                    for rec in DBF(str(tmp), encoding="latin-1",
                                   ignore_missing_memofile=True):
                        records.append(dict(rec))
                    return records
                # Try CSV
                csv_files = [n for n in z.namelist()
                             if n.lower().endswith((".csv", ".txt"))]
                if csv_files:
                    with z.open(csv_files[0]) as f:
                        text = f.read().decode("latin-1", errors="replace")
                    reader = csv.DictReader(io.StringIO(text))
                    for row in reader:
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
                owner = (r.get("OWNER") or r.get("OWN1") or "").strip().upper()
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
                for v in self._name_variants(owner):
                    self.lookup[v] = parcel
                count += 1
            except Exception:
                continue
        log.info(f"  Ingested {count} parcels → {len(self.lookup)} name keys")

    def load(self):
        for url in self.KNOWN_URLS:
            try:
                raw = self._try_download_zip(url)
                if not raw:
                    continue
                records = self._parse_zip(raw)
                if records:
                    self._ingest(records)
                    return
            except Exception as e:
                log.warning(f"  Failed {url}: {e}")
        log.warning("HCAD unavailable — running without address enrichment")

    def find(self, name: str) -> Optional[dict]:
        if not name:
            return None
        for v in self._name_variants(name):
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
    """
    Uses Playwright to interact with RP.aspx.
    The site uses encrypted session tokens in GET params — must use real browser.
    Results table columns: File Number | File Date | Type/Vol/Page | Names | Legal Desc | Pgs | Film Code
    """

    BASE_URL = "https://www.cclerk.hctx.net/applications/websearch/RP.aspx"

    def __init__(self, days_back: int = 7):
        self.days_back  = days_back
        self.today      = datetime.now()
        self.start_date = self.today - timedelta(days=days_back)
        self.records: list = []

    def _parse_results_page(self, html: str, doc_code: str,
                            cat: str, cat_label: str) -> list:
        """
        Parse the RP_R.aspx results table.
        Columns: File Number | File Date | Type Vol Page | Names | Legal Description | Pgs | Film Code
        Names cell contains lines like:
          Grantor:FOXWOOD HOMEOWNERS ASSOCIATION
          Grantee:TREVINO CLAUDIA
        Legal cell contains:
          Desc: FOXWOOD  Sec: 4  Lot: 15  Block:4
        """
        soup = BeautifulSoup(html, "lxml")
        rows_out = []

        # Find the results table — look for one with "File Number" header
        result_table = None
        for tbl in soup.find_all("table"):
            text = tbl.get_text(" ", strip=True).lower()
            if "file number" in text and "file date" in text:
                result_table = tbl
                break

        if not result_table:
            return []

        data_rows = result_table.find_all("tr")
        for tr in data_rows:
            try:
                cells = tr.find_all("td")
                if len(cells) < 4:
                    continue

                # Col 0: File Number
                file_num = cells[0].get_text(strip=True)
                if not file_num or "file" in file_num.lower():
                    continue

                # Col 1: File Date
                file_date_raw = cells[1].get_text(strip=True)
                filed_iso = ""
                for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                    try:
                        filed_iso = datetime.strptime(
                            file_date_raw[:10], fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        pass

                # Col 2: Type / Vol / Page
                type_text = cells[2].get_text(" ", strip=True)
                # The instrument type link is in this cell
                type_link = cells[2].find("a")
                rec_type  = type_link.get_text(strip=True) if type_link else doc_code

                # Col 3: Names (Grantor / Grantee)
                names_text = cells[3].get_text(" ", strip=True)
                grantor = ""
                grantees = []
                for line in cells[3].get_text("\n").split("\n"):
                    line = line.strip()
                    if line.lower().startswith("grantor:"):
                        grantor = line[8:].strip()
                    elif line.lower().startswith("grantee:"):
                        grantees.append(line[8:].strip())

                # Col 4: Legal Description
                legal = ""
                if len(cells) > 4:
                    legal = cells[4].get_text(" ", strip=True)

                # Col 6 or last: Film Code (direct link)
                clerk_url = self.BASE_URL
                film_cell = cells[-1] if len(cells) >= 6 else None
                if film_cell:
                    a = film_cell.find("a", href=True)
                    if a:
                        href = a.get("href", "")
                        if href.startswith("http"):
                            clerk_url = href
                        elif href:
                            clerk_url = "https://www.cclerk.hctx.net" + href

                if not file_num:
                    continue

                rows_out.append({
                    "doc_num":   file_num,
                    "doc_type":  rec_type or doc_code,
                    "filed":     filed_iso or file_date_raw,
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
                log.warning(f"    Timeout loading page (attempt {attempt+1})")
                if attempt == 2:
                    return

        try:
            # The Instrument Type field is a plain text input
            # From the screenshot: label "Instrument Type" next to a text box
            instr_selectors = [
                'input[name*="InstrumentType"]',
                'input[id*="InstrumentType"]',
                'input[name*="txtInstrType"]',
                'input[id*="txtInstrType"]',
                'input[name*="InstrType"]',
                'input[id*="InstrType"]',
                'input[placeholder*="nstrument"]',
            ]

            filled = False
            for sel in instr_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.triple_click()
                        await el.fill(doc_code)
                        log.info(f"    Filled instrument via: {sel}")
                        filled = True
                        break
                except Exception:
                    pass

            # Fallback: find input near "Instrument Type" label
            if not filled:
                try:
                    # Get all visible inputs and find the one near instrument label
                    inputs = await page.query_selector_all('input[type="text"]')
                    for inp in inputs:
                        # Check if nearby text contains "instrument"
                        parent = await inp.evaluate_handle(
                            'el => el.closest("div")')
                        parent_text = await page.evaluate(
                            'el => el ? el.innerText : ""', parent)
                        if "instrument" in parent_text.lower():
                            await inp.triple_click()
                            await inp.fill(doc_code)
                            log.info("    Filled instrument via label proximity")
                            filled = True
                            break
                except Exception:
                    pass

            if not filled:
                log.warning(f"    Could not find instrument type field for {doc_code}")

            # Date fields
            date_from = self.start_date.strftime("%m/%d/%Y")
            date_to   = self.today.strftime("%m/%d/%Y")

            date_from_selectors = [
                'input[name*="DateFrom"]', 'input[id*="DateFrom"]',
                'input[name*="BeginDate"]', 'input[name*="StartDate"]',
                'input[name*="FileDateFrom"]', 'input[id*="FileDateFrom"]',
            ]
            date_to_selectors = [
                'input[name*="DateTo"]', 'input[id*="DateTo"]',
                'input[name*="EndDate"]', 'input[name*="StopDate"]',
                'input[name*="FileDateTo"]', 'input[id*="FileDateTo"]',
            ]

            for sel in date_from_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.triple_click()
                        await el.fill(date_from)
                        break
                except Exception:
                    pass

            for sel in date_to_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.triple_click()
                        await el.fill(date_to)
                        break
                except Exception:
                    pass

            # Click Search button
            for sel in [
                'input[value="SEARCH"]',
                'input[value="Search"]',
                'button:has-text("SEARCH")',
                'button:has-text("Search")',
                'input[type="submit"]',
                'input[id*="btnSearch"]',
                'input[name*="btnSearch"]',
            ]:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        await page.wait_for_load_state(
                            "domcontentloaded", timeout=20000)
                        await page.wait_for_timeout(2000)
                        log.info(f"    Clicked search via: {sel}")
                        break
                except Exception:
                    pass

            # Collect pages
            page_num = 0
            while True:
                page_num += 1
                html = await page.content()
                rows = self._parse_results_page(html, doc_code, cat, cat_label)
                log.info(f"    Page {page_num}: {len(rows)} rows")
                self.records.extend(rows)

                # Check for NEXT button
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
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()
            page.set_default_timeout(30000)

            # Debug: dump page HTML on first load to check field names
            try:
                await page.goto(self.BASE_URL,
                                wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)
                html = await page.content()
                # Log all input fields found
                inputs = await page.query_selector_all("input")
                log.info(f"  Found {len(inputs)} inputs on page")
                for inp in inputs[:20]:
                    try:
                        name = await inp.get_attribute("name") or ""
                        iid  = await inp.get_attribute("id") or ""
                        typ  = await inp.get_attribute("type") or ""
                        val  = await inp.get_attribute("value") or ""
                        if name or iid:
                            log.info(f"    INPUT name={name!r} id={iid!r} "
                                     f"type={typ!r} value={val!r}")
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"Debug load failed: {e}")

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


# ─── Save Outputs ─────────────────────────────────────────────────────────────
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
        out = d / "records.json"
        out.write_text(json.dumps(payload, indent=2, default=str))
        log.info(f"Saved → {out}")


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

    log.info(f"\n✓ Done! Total: {len(records)} | "
             f"Hot: {sum(1 for r in records if r.get('score',0)>=70)} | "
             f"Warm: {sum(1 for r in records if 50<=r.get('score',0)<70)}")


if __name__ == "__main__":
    asyncio.run(main())
