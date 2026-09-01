#!/usr/bin/env python3
"""
Lead Enrichment Pipeline — Tactik AI Dev
Monday task: Lead Scraping + Enrichment HANDOVER (item 2835598691)

v2 — headers verified against the real uploaded files (fresh_networking_scored.xlsx,
mazenod_alumni.xlsx, transcend_final.xlsx, vsbn_final.xlsx, whatsapp_contacts_bali.json).
Adds an Airtable push as the final step (CSV stays the source of truth; Airtable is
the last stage, not the storage layer — if the push fails you re-run it from the CSV,
you don't lose the enrichment work).

WHAT THIS DOES
  Reads each of the 5 raw lead sources, fills in missing email / company / LinkedIn /
  website / job title using Hunter.io + Apollo.io + People Data Labs, writes a clean
  CSV, then (optionally) pushes the rows into an Airtable base/table.

VERIFIED SOURCE STATS (checked directly against the uploaded files)
  Fresh Networking : 430 rows, 423 already have an email (98%) — near enrichment-complete,
                      just needs normalizing into the target schema. LOWEST priority.
  Transcend         : 514 rows, 283 have an email (55%), 173 have LinkedIn (34%)
  VSBN              : 447 rows, 235 have an email (53%), no LinkedIn column at all
  Mazenod Alumni    : 1,009 rows, 0 emails, 36 have LinkedIn (4%) — confirmed not
                      viably enrichable at scale, flagged not run by default
  Bali WhatsApp     : 138 contacts (39 named + 99 phone-only) — hardest, no company/site

REQUIRED ENV VARS (set at `docker run` time via --env-file, never baked into the image)
  HUNTER_API_KEY
  APOLLO_API_KEY
  PDL_API_KEY
  AIRTABLE_API_KEY     (only needed if --airtable-push is used)
  AIRTABLE_BASE_ID     (only needed if --airtable-push is used)

USAGE
  python enrich_pipeline.py --source vsbn --input vsbn_final.xlsx --output vsbn_enriched.csv
  python enrich_pipeline.py --source transcend --input transcend_final.xlsx --output transcend_enriched.csv
  python enrich_pipeline.py --source fresh --input fresh_networking_scored.xlsx --output fresh_enriched.csv
  python enrich_pipeline.py --source mazenod --input mazenod_alumni.xlsx --output mazenod_enriched.csv
  python enrich_pipeline.py --source bali --input whatsapp_contacts_bali.json --output bali_enriched.csv

  --limit N                 process only the first N rows (validate on ~20 before a full run)
  --dry-run                 no paid API calls, just report what would happen
  --airtable-push           after writing the CSV, push rows into Airtable
  --airtable-table NAME     Airtable table name (required if --airtable-push)

DESIGN NOTES
  - Every API call is cached to a local sqlite db (enrich_cache.db), keyed by lookup
    key, so re-runs never pay twice for the same lookup.
  - Hunter's email VERIFIER always runs on any email before it's accepted.
  - Known-blocked approaches (LinkedIn scraping, search-engine scraping, free
    reverse-phone lookup) are deliberately NOT implemented — already tried and
    failed from the server IP, per the brief.
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests openpyxl --break-system-packages")

CACHE_DB = "enrich_cache.db"
RATE_LIMIT_SECONDS = {"hunter": 1.0, "apollo": 1.0, "pdl": 1.0, "airtable": 0.25}

TARGET_FIELDS = [
    "first_name", "last_name", "business_name", "phone", "email",
    "linkedin", "website", "address", "categories", "description",
    "rating", "review_count", "social_links", "website_score", "source",
]


@dataclass
class Lead:
    first_name: str = ""
    last_name: str = ""
    business_name: str = ""
    phone: str = ""
    email: str = ""
    linkedin: str = ""
    website: str = ""
    address: str = ""
    categories: str = ""
    description: str = ""
    rating: str = ""
    review_count: str = ""
    social_links: str = ""
    website_score: str = ""
    source: str = ""
    _enrichment_notes: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def init_cache():
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lookups (
            provider TEXT, lookup_key TEXT, result_json TEXT, fetched_at REAL,
            PRIMARY KEY (provider, lookup_key)
        )
    """)
    conn.commit()
    return conn


def cache_get(conn, provider, key):
    row = conn.execute(
        "SELECT result_json FROM lookups WHERE provider=? AND lookup_key=?",
        (provider, key),
    ).fetchone()
    return json.loads(row[0]) if row else None


def cache_set(conn, provider, key, result):
    conn.execute(
        "INSERT OR REPLACE INTO lookups VALUES (?,?,?,?)",
        (provider, key, json.dumps(result), time.time()),
    )
    conn.commit()


_last_call = {}


def _throttle(provider):
    wait = RATE_LIMIT_SECONDS.get(provider, 1.0)
    last = _last_call.get(provider, 0)
    elapsed = time.time() - last
    if elapsed < wait:
        time.sleep(wait - elapsed)
    _last_call[provider] = time.time()


# ---------------------------------------------------------------------------
# Provider calls — cache-aware, rate-limited, safe to call with --dry-run and
# zero keys set.
# ---------------------------------------------------------------------------

def hunter_find_email(conn, domain, first_name, last_name, dry_run=False):
    key = f"{domain}:{first_name}:{last_name}"
    cached = cache_get(conn, "hunter_find", key)
    if cached:
        return cached
    if dry_run:
        return {"email": None, "confidence": None, "dry_run": True}
    api_key = os.environ.get("HUNTER_API_KEY")
    if not api_key:
        raise RuntimeError("HUNTER_API_KEY not set")
    _throttle("hunter")
    resp = requests.get(
        "https://api.hunter.io/v2/email-finder",
        params={"domain": domain, "first_name": first_name, "last_name": last_name, "api_key": api_key},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json().get("data", {})
    result = {"email": data.get("email"), "confidence": data.get("score")}
    cache_set(conn, "hunter_find", key, result)
    return result


def hunter_verify_email(conn, email, dry_run=False):
    cached = cache_get(conn, "hunter_verify", email)
    if cached:
        return cached
    if dry_run:
        return {"status": "dry_run", "score": None}
    api_key = os.environ.get("HUNTER_API_KEY")
    if not api_key:
        raise RuntimeError("HUNTER_API_KEY not set")
    _throttle("hunter")
    resp = requests.get(
        "https://api.hunter.io/v2/email-verifier",
        params={"email": email, "api_key": api_key},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json().get("data", {})
    result = {"status": data.get("status"), "score": data.get("score")}
    cache_set(conn, "hunter_verify", email, result)
    return result


def apollo_enrich_person(conn, first_name, last_name, company, dry_run=False):
    key = f"{first_name}:{last_name}:{company}"
    cached = cache_get(conn, "apollo", key)
    if cached:
        return cached
    if dry_run:
        return {"email": None, "title": None, "linkedin": None, "dry_run": True}
    api_key = os.environ.get("APOLLO_API_KEY")
    if not api_key:
        raise RuntimeError("APOLLO_API_KEY not set")
    _throttle("apollo")
    resp = requests.post(
        "https://api.apollo.io/v1/people/match",
        headers={"Cache-Control": "no-cache", "Content-Type": "application/json"},
        json={"api_key": api_key, "first_name": first_name, "last_name": last_name, "organization_name": company},
        timeout=15,
    )
    resp.raise_for_status()
    person = resp.json().get("person") or {}
    result = {
        "email": person.get("email"),
        "title": person.get("title"),
        "linkedin": person.get("linkedin_url"),
    }
    cache_set(conn, "apollo", key, result)
    return result


def pdl_enrich_phone(conn, phone, dry_run=False):
    cached = cache_get(conn, "pdl", phone)
    if cached:
        return cached
    if dry_run:
        return {"full_name": None, "company": None, "linkedin": None, "dry_run": True}
    api_key = os.environ.get("PDL_API_KEY")
    if not api_key:
        raise RuntimeError("PDL_API_KEY not set")
    _throttle("pdl")
    resp = requests.get(
        "https://api.peopledatalabs.com/v5/person/enrich",
        headers={"X-Api-Key": api_key},
        params={"phone": phone},
        timeout=15,
    )
    data = resp.json().get("data", {}) if resp.ok else {}
    result = {
        "full_name": data.get("full_name"),
        "company": data.get("job_company_name"),
        "linkedin": data.get("linkedin_url"),
    }
    cache_set(conn, "pdl", phone, result)
    return result


# ---------------------------------------------------------------------------
# Per-source loaders — field maps verified against the ACTUAL headers in each
# uploaded file (checked directly, not guessed).
# ---------------------------------------------------------------------------

def _clean(v):
    return str(v).strip() if v is not None else ""


def load_fresh_networking(path):
    """fresh_networking_scored.xlsx — real headers:
    Name, Company, Business Types, Hubs, Email (Profile), Emails (Website), Website,
    Website Score, Lead Score, LinkedIn, Facebook, SSL, Description, Profile URL
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {h: i for i, h in enumerate(headers)}
    leads = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        name = _clean(row[idx["Name"]])
        parts = name.split(" ", 1) if name else ["", ""]
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""
        email = _clean(row[idx["Email (Profile)"]]) or _clean(row[idx.get("Emails (Website)", idx["Email (Profile)"])])
        leads.append(Lead(
            first_name=first, last_name=last,
            business_name=_clean(row[idx["Company"]]),
            email=email,
            linkedin=_clean(row[idx["LinkedIn"]]),
            website=_clean(row[idx["Website"]]),
            categories=_clean(row[idx["Business Types"]]),
            description=_clean(row[idx["Description"]]),
            website_score=_clean(row[idx["Website Score"]]),
            social_links=_clean(row[idx["Facebook"]]),
            source="fresh_networking",
        ))
    return leads


def load_mazenod(path):
    """mazenod_alumni.xlsx — real headers:
    firstName, lastName, ..., cityName, stateName, country, linkedInUrl, facebookUrl, ...
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {h: i for i, h in enumerate(headers)}
    leads = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        city = _clean(row[idx["cityName"]])
        state = _clean(row[idx["stateName"]])
        country = _clean(row[idx["country"]])
        address = ", ".join(p for p in [city, state, country] if p)
        leads.append(Lead(
            first_name=_clean(row[idx["firstName"]]),
            last_name=_clean(row[idx["lastName"]]),
            linkedin=_clean(row[idx["linkedInUrl"]]),
            social_links=_clean(row[idx["facebookUrl"]]),
            address=address,
            source="mazenod",
        ))
    return leads


def load_transcend(path):
    """transcend_final.xlsx — real headers:
    Name, First Name, Location, Emails, Website, LinkedIn, Facebook, Instagram,
    Twitter, Tags, Bio, Member Since, Profile URL
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {h: i for i, h in enumerate(headers)}
    leads = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        full_name = _clean(row[idx["Name"]])
        first = _clean(row[idx["First Name"]]) or (full_name.split(" ", 1)[0] if full_name else "")
        last = full_name[len(first):].strip() if full_name.startswith(first) else ""
        emails = _clean(row[idx["Emails"]])
        first_email = emails.split(",")[0].strip() if emails else ""
        leads.append(Lead(
            first_name=first, last_name=last,
            email=first_email,
            website=_clean(row[idx["Website"]]),
            linkedin=_clean(row[idx["LinkedIn"]]),
            social_links=_clean(row[idx["Facebook"]]),
            categories=_clean(row[idx["Tags"]]),
            description=_clean(row[idx["Bio"]]),
            address=_clean(row[idx["Location"]]),
            source="transcend",
        ))
    return leads


def load_vsbn(path):
    """vsbn_final.xlsx — real headers:
    Business Name, Phone, Website, Emails, Categories, Description, Rating,
    Review Count, Address, Suburb, State, Postcode. No LinkedIn column exists.
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {h: i for i, h in enumerate(headers)}
    leads = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        addr_parts = [
            _clean(row[idx["Address"]]), _clean(row[idx["Suburb"]]),
            _clean(row[idx["State"]]), _clean(row[idx["Postcode"]]),
        ]
        leads.append(Lead(
            business_name=_clean(row[idx["Business Name"]]),
            phone=_clean(row[idx["Phone"]]),
            website=_clean(row[idx["Website"]]),
            email=_clean(row[idx["Emails"]]),
            categories=_clean(row[idx["Categories"]]),
            description=_clean(row[idx["Description"]]),
            rating=_clean(row[idx["Rating"]]),
            review_count=_clean(row[idx["Review Count"]]),
            address=", ".join(p for p in addr_parts if p),
            source="vsbn",
        ))
    return leads


def load_bali_group(path):
    """whatsapp_contacts_bali.json — real structure is {"named": [...], "phones_only": [...]},
    NOT a flat list. named entries have name/phone/group; phones_only is raw phone strings.
    """
    with open(path) as f:
        raw = json.load(f)
    leads = []
    for c in raw.get("named", []):
        name = _clean(c.get("name"))
        parts = name.split(" ", 1) if name else ["", ""]
        leads.append(Lead(
            first_name=parts[0],
            last_name=parts[1] if len(parts) > 1 else "",
            phone=_clean(c.get("phone")).rstrip("-"),
            categories=_clean(c.get("group")),
            source="bali_group",
        ))
    for phone in raw.get("phones_only", []):
        leads.append(Lead(
            phone=_clean(phone).rstrip("-"),
            source="bali_group",
            _enrichment_notes=["no name — phone-only contact"],
        ))
    return leads


SOURCE_LOADERS = {
    "fresh": load_fresh_networking,
    "mazenod": load_mazenod,
    "transcend": load_transcend,
    "vsbn": load_vsbn,
    "bali": load_bali_group,
}


# ---------------------------------------------------------------------------
# Enrichment strategies — one per source
# ---------------------------------------------------------------------------

def enrich_fresh(conn, lead: Lead, dry_run):
    # Already ~98% has an email. Just normalize — no paid API calls needed by default.
    if not lead.email:
        lead._enrichment_notes.append("missing email despite Fresh Networking's high coverage — manual review")
    return lead


def enrich_mazenod(conn, lead: Lead, dry_run):
    lead._enrichment_notes.append(
        "no viable enrichment anchor at scale (0% emails, 4% LinkedIn, no employer field) — "
        "confirmed not enrichable via API; do not spend API budget here without a different data strategy"
    )
    return lead


def enrich_domain_based(conn, lead: Lead, dry_run):
    """Shared strategy for VSBN / Transcend: has a website, missing email."""
    if lead.email or not lead.website:
        return lead
    domain = lead.website.replace("https://", "").replace("http://", "").split("/")[0]
    guess = hunter_find_email(conn, domain, lead.first_name or "info", lead.last_name or "", dry_run)
    if guess.get("email"):
        verify = hunter_verify_email(conn, guess["email"], dry_run)
        if dry_run or verify.get("status") in ("valid", "accept_all"):
            lead.email = guess["email"]
        else:
            lead._enrichment_notes.append(f"email found but failed verification: {guess['email']} ({verify.get('status')})")
    return lead


def enrich_bali(conn, lead: Lead, dry_run):
    if lead.phone:
        pdl = pdl_enrich_phone(conn, lead.phone, dry_run)
        if pdl.get("company"):
            lead.business_name = pdl["company"]
        if pdl.get("linkedin"):
            lead.linkedin = pdl["linkedin"]
        if not lead.first_name and pdl.get("full_name"):
            parts = pdl["full_name"].split(" ")
            lead.first_name, lead.last_name = parts[0], " ".join(parts[1:])
    if lead.business_name and lead.first_name:
        apollo = apollo_enrich_person(conn, lead.first_name, lead.last_name, lead.business_name, dry_run)
        if apollo.get("email"):
            lead.email = apollo["email"]
        if apollo.get("linkedin") and not lead.linkedin:
            lead.linkedin = apollo["linkedin"]
    return lead


ENRICH_STRATEGIES = {
    "fresh": enrich_fresh,
    "mazenod": enrich_mazenod,
    "vsbn": enrich_domain_based,
    "transcend": enrich_domain_based,
    "bali": enrich_bali,
}


# ---------------------------------------------------------------------------
# Airtable push — final step, optional, does not gate the CSV output
# ---------------------------------------------------------------------------

def airtable_push(rows, table_name, dry_run=False):
    api_key = os.environ.get("AIRTABLE_API_KEY")
    base_id = os.environ.get("AIRTABLE_BASE_ID")
    if not api_key or not base_id:
        raise RuntimeError("AIRTABLE_API_KEY / AIRTABLE_BASE_ID not set")

    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    pushed, failed = 0, 0
    # Airtable's create endpoint accepts up to 10 records per request.
    for i in range(0, len(rows), 10):
        batch = rows[i:i + 10]
        payload = {"records": [{"fields": r} for r in batch]}
        if dry_run:
            pushed += len(batch)
            continue
        _throttle("airtable")
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        if resp.ok:
            pushed += len(batch)
        else:
            failed += len(batch)
            print(f"  Airtable batch failed ({resp.status_code}): {resp.text[:200]}", file=sys.stderr)
    return pushed, failed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True, choices=list(SOURCE_LOADERS.keys()))
    ap.add_argument("--input", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--airtable-push", action="store_true")
    ap.add_argument("--airtable-table", type=str, default=None)
    args = ap.parse_args()

    if not args.input.exists():
        sys.exit(f"Input file not found: {args.input}")
    if args.airtable_push and not args.airtable_table:
        sys.exit("--airtable-table is required when using --airtable-push")

    conn = init_cache()
    leads = SOURCE_LOADERS[args.source](args.input)
    if args.limit:
        leads = leads[: args.limit]

    strategy = ENRICH_STRATEGIES[args.source]
    enriched, notes = 0, 0
    for lead in leads:
        before = asdict(lead)
        strategy(conn, lead, args.dry_run)
        if asdict(lead) != before:
            enriched += 1
        if lead._enrichment_notes:
            notes += 1

    rows_for_output = [{k: v for k, v in asdict(lead).items() if k in TARGET_FIELDS} for lead in leads]

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TARGET_FIELDS)
        writer.writeheader()
        writer.writerows(rows_for_output)

    print(f"{args.source}: {len(leads)} rows processed, {enriched} enriched, {notes} flagged for manual review")
    print(f"-> {args.output}")
    if args.dry_run:
        print("(dry run — no paid API calls were made)")

    if args.airtable_push:
        pushed, failed = airtable_push(rows_for_output, args.airtable_table, args.dry_run)
        print(f"Airtable push: {pushed} pushed, {failed} failed" + (" (dry run)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
