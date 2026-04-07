# Harris County Motivated Seller Lead Scraper

Automated daily scraper for Harris County, Houston TX public records.
Targets motivated seller lead types, enriches with HCAD parcel data, and scores each record.

## Live Dashboard

Hosted on GitHub Pages → `https://<your-username>.github.io/<repo-name>/`

## Lead Types Collected

| Code | Type |
|------|------|
| LP | Lis Pendens |
| NOFC / NOF | Notice of Foreclosure |
| TAXDEED | Tax Deed |
| JUD / CCJ / DRJUD | Judgment / Certified / Domestic |
| LNCORPTX / LNIRS / LNFED | Corp Tax / IRS / Federal Lien |
| LN / LNMECH / LNHOA | Lien / Mechanic / HOA |
| MEDLN | Medicaid Lien |
| PRO | Probate |
| NOC | Notice of Commencement |
| RELLP | Release Lis Pendens |

## Seller Score (0–100)

| Rule | Points |
|------|--------|
| Base | 30 |
| Per flag | +10 |
| LP + Foreclosure combo | +20 |
| Amount > $100k | +15 |
| Amount > $50k | +10 |
| Filed this week | +5 |
| Has property address | +5 |

## Setup

### Local Run

```bash
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
python scraper/fetch.py
```

### GitHub Actions

The workflow runs daily at 7:00 AM UTC (2:00 AM Central).

**Required repo settings:**
1. Go to Settings → Pages → Source: `GitHub Actions`
2. No secrets needed — uses built-in `GITHUB_TOKEN`

**Manual trigger:**
- Actions → "Scrape Harris County Motivated Seller Leads" → Run workflow

## File Structure

```
├── scraper/
│   ├── fetch.py           # Main scraper
│   └── requirements.txt   # Python deps
├── dashboard/
│   ├── index.html         # Lead intelligence dashboard
│   └── records.json       # Latest scraped records
├── data/
│   └── records.json       # Mirror of records for API use
└── .github/
    └── workflows/
        └── scrape.yml     # Daily automation
```

## Output JSON Schema

```json
{
  "fetched_at": "ISO datetime",
  "source": "Harris County Clerk / HCAD",
  "date_range": { "from": "YYYY-MM-DD", "to": "YYYY-MM-DD" },
  "total": 0,
  "with_address": 0,
  "records": [{
    "doc_num": "",
    "doc_type": "LP",
    "filed": "YYYY-MM-DD",
    "cat": "lp",
    "cat_label": "Lis Pendens",
    "owner": "SMITH JOHN",
    "grantee": "",
    "amount": 125000.00,
    "legal": "LT 5 BLK 12 RIVERSIDE TERRACE",
    "prop_address": "1234 MAIN ST",
    "prop_city": "HOUSTON",
    "prop_state": "TX",
    "prop_zip": "77001",
    "mail_address": "5678 OAK DR",
    "mail_city": "HOUSTON",
    "mail_state": "TX",
    "mail_zip": "77002",
    "clerk_url": "https://...",
    "flags": ["Lis pendens", "New this week"],
    "score": 75
  }]
}
```

## GHL CSV Export

Exported daily to `dashboard/ghl_export_YYYYMMDD.csv` with columns ready to import into GoHighLevel.

## Notes

- The Harris County Clerk portal uses ASP.NET WebForms with `__doPostBack` — the scraper handles viewstate and event targets automatically.
- HCAD bulk parcel data is attempted from multiple known endpoints; if unavailable, records still export without address enrichment.
- All errors are caught per-record — the scraper never crashes on bad data.
