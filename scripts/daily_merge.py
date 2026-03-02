#!/usr/bin/env python3
"""
Daily Merge — Merge duplicate Airtable records from today's activity.

Finds all riders active today, groups duplicates by IG URL / name overlap,
merges fields into the "best" record (most data), and deletes the rest.

Also handles:
  - Multiple FB URLs (personal + racing page → FB URL 2)
  - Junk records (search results, photo pages)
  - Cross-contaminated URLs (name ≠ IG username)

Usage:
  python3 scripts/daily_merge.py              # Dry run (show what would happen)
  python3 scripts/daily_merge.py --execute    # Actually merge & delete
  python3 scripts/daily_merge.py --all        # Check ALL records, not just today
"""

import sys, os, re, json, argparse
from datetime import datetime, date
from collections import defaultdict

# Load config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    # Try loading from airtable_config.js
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                               'chrome-extension', 'airtable_config.js')
    if os.path.exists(config_path):
        with open(config_path) as f:
            content = f.read()
        API_KEY = re.search(r"AIRTABLE_API_KEY\s*=\s*['\"](.+?)['\"]", content).group(1)
        BASE_ID = re.search(r"AIRTABLE_BASE_ID\s*=\s*['\"](.+?)['\"]", content).group(1)
        TABLE = re.search(r"AIRTABLE_TABLE\s*=\s*['\"](.+?)['\"]", content).group(1)
    else:
        raise FileNotFoundError
except:
    API_KEY = os.environ.get('AIRTABLE_API_KEY', '')
    BASE_ID = os.environ.get('AIRTABLE_BASE_ID', 'appOK1dNqufKg0bEd')
    TABLE = os.environ.get('AIRTABLE_TABLE', 'Riders')

import requests

BASE_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}"
HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

# Stage priority (higher = more advanced)
STAGE_ORDER = [
    'Contact', 'Messaged', 'Outreach', 'Replied', 'Link Sent',
    'Blueprint Link Sent', 'Race Weekend', 'Race Review Complete',
    'Blueprint Started', 'Registered', 'Day 1 Complete',
    'Day 2 Complete', 'Day 3 Complete', 'Strategy Call Booked',
    'Strategy Call Complete', 'Client', 'Not a good fit'
]

# Junk patterns in Full Name — these records should be deleted
JUNK_PATTERNS = [
    r'search results?$',
    r'^https?://',
    r'^\d+$',
    r'\bmessaged you\b',
    r'\bsent you\b',
    r'\breplied to\b',
    r'\bliked your\b',
    r'\bmentioned you\b',
    r'\btagged you\b',
    r'\bis typing\b',
]

# Junk FB URLs that should be cleared
JUNK_FB_URLS = [
    'facebook.com/photo/', 'facebook.com/search/',
    'facebook.com/profile.php', 'facebook.com/hashtag/',
    'facebook.com/watch/', 'facebook.com/gaming/',
]


def fetch_all_records(date_filter=None):
    """Fetch all records from Airtable, handling pagination."""
    from urllib.parse import urlencode
    records = []
    field_names = [
        "Full Name", "First Name", "Last Name", "Stage",
        "IG URL", "FB URL", "Championship",
        "Last Activity", "Date Messaged", "Notes",
        "Also Known As", "Tags", "Race Results",
        "Season Summary", "R1 Circuit", "R1 Qual",
        "R1 Race 1", "R1 Race 2",
    ]

    offset = None
    while True:
        params = [("pageSize", "100")]
        for fn in field_names:
            params.append(("fields[]", fn))
        if date_filter:
            params.append(("filterByFormula", f"DATESTR({{Last Activity}})='{date_filter}'"))
        if offset:
            params.append(("offset", offset))

        url = BASE_URL + "?" + urlencode(params)
        resp = requests.get(url, headers=HEADERS)
        data = resp.json()
        if 'error' in data:
            print(f"⚠️ Airtable error: {data['error']}")
            break
        records.extend(data.get('records', []))
        offset = data.get('offset')
        if not offset:
            break

    return records


def is_junk_record(name):
    """Check if a record name matches junk patterns."""
    if not name:
        return True
    for pat in JUNK_PATTERNS:
        if re.search(pat, name, re.IGNORECASE):
            return True
    return False


def is_junk_fb_url(url):
    """Check if a FB URL is a junk/non-profile URL."""
    if not url:
        return False
    return any(j in url for j in JUNK_FB_URLS)


def is_handle_name(name):
    """Check if a name looks like a social media handle rather than a real name."""
    if not name:
        return True
    # No spaces + has underscores/dots = probably a handle
    if ' ' not in name and ('_' in name or '.' in name):
        return True
    # No spaces + numbers at end (e.g. "gricey4", "anthchiodo_")
    if ' ' not in name and re.search(r'\d', name):
        return True
    return False


def name_tokens(name):
    """Extract significant tokens from a name."""
    if not name:
        return set()
    # Remove emojis / unicode flags
    cleaned = re.sub(r'[\U0001F000-\U0001FFFF]', '', name)
    cleaned = re.sub(r'[#\d]+', '', cleaned)  # Remove numbers
    parts = re.split(r'[\s_.\-]+', cleaned.lower().strip())
    return {p for p in parts if len(p) > 1}


def names_match(name1, name2):
    """Check if two names refer to the same person."""
    t1 = name_tokens(name1)
    t2 = name_tokens(name2)
    if not t1 or not t2:
        return False
    overlap = t1 & t2
    # Need at least 2 token overlap for multi-word names
    if len(t1) >= 2 and len(t2) >= 2:
        return len(overlap) >= 2
    # For single-token names, need exact match
    return len(overlap) >= 1 and (t1 <= t2 or t2 <= t1)


def ig_username(url):
    """Extract username from IG URL."""
    if not url:
        return None
    m = re.search(r'instagram\.com/([A-Za-z0-9_.]+)', url)
    return m.group(1).lower() if m else None


def record_score(rec):
    """Score a record by data completeness. Higher = better to keep."""
    f = rec['fields']
    score = 0

    # Strongly prefer real names over handles
    name = f.get('Full Name', '').strip()
    if ' ' in name and not is_handle_name(name):
        score += 100  # Real name like "Lachlan BRINKMAN"
    elif ' ' in name:
        score += 40   # Multi-word but might be partial
    elif name and ' ' not in name:
        score -= 50   # Single word like "lachibrinkman", "gricey4" — almost certainly a handle

    # Stage progress
    stage = f.get('Stage', '')
    if stage in STAGE_ORDER:
        score += STAGE_ORDER.index(stage) * 3

    # Data richness
    if f.get('IG URL'):   score += 10
    if f.get('FB URL'):   score += 10
    if f.get('Championship'): score += 5
    if f.get('Race Results'):  score += 20
    if f.get('Season Summary'): score += 10
    if f.get('Notes'):    score += 5
    if f.get('Date Messaged'): score += 5
    if f.get('R1 Circuit'): score += 15
    if f.get('Also Known As'): score += 5
    if f.get('Tags'): score += 3
    if f.get('Date Blueprint Started'): score += 8
    if f.get('Date Day 1 Assessment'): score += 8
    if f.get('Country'): score += 3

    # Count non-empty fields
    score += sum(1 for v in f.values() if v)

    return score


def merge_fields(keep_rec, merge_recs):
    """Merge fields from duplicate records into the keep record.
    Returns the merged fields dict (only changed/new fields)."""
    keep_f = keep_rec['fields']
    merged = {}

    # Collect all FB URLs across records
    all_fb_urls = set()
    if keep_f.get('FB URL') and not is_junk_fb_url(keep_f['FB URL']):
        all_fb_urls.add(keep_f['FB URL'])
    if keep_f.get('FB URL 2') and not is_junk_fb_url(keep_f['FB URL 2']):
        all_fb_urls.add(keep_f['FB URL 2'])

    # Collect AKA names
    aka_parts = set()
    existing_aka = keep_f.get('Also Known As', '')
    if existing_aka:
        aka_parts.update(a.strip() for a in existing_aka.split(',') if a.strip())

    for mr in merge_recs:
        mf = mr['fields']

        # Merge simple text fields (keep first non-empty)
        for field in ['IG URL', 'Championship', 'Notes', 'Race Results',
                      'Season Summary', 'Date Messaged', 'Tags',
                      'Date Blueprint Started', 'Date Day 1 Assessment',
                      'Date Day 2 Assessment', 'Date Strategy Call',
                      'R1 Circuit', 'R1 Qual', 'R1 Race 1', 'R1 Race 2', 'R1 Race 3',
                      'Website URL', 'LinkedIn URL', 'Country']:
            if not keep_f.get(field) and mf.get(field):
                merged[field] = mf[field]

        # Collect FB URLs
        if mf.get('FB URL') and not is_junk_fb_url(mf['FB URL']):
            all_fb_urls.add(mf['FB URL'])
        if mf.get('FB URL 2') and not is_junk_fb_url(mf['FB URL 2']):
            all_fb_urls.add(mf['FB URL 2'])

        # Stage: keep highest
        keep_stage = keep_f.get('Stage', '')
        merge_stage = mf.get('Stage', '')
        if merge_stage and merge_stage in STAGE_ORDER:
            keep_idx = STAGE_ORDER.index(keep_stage) if keep_stage in STAGE_ORDER else -1
            merge_idx = STAGE_ORDER.index(merge_stage)
            if merge_idx > keep_idx:
                merged['Stage'] = merge_stage

        # Last Activity: keep most recent
        keep_la = keep_f.get('Last Activity', '')
        merge_la = mf.get('Last Activity', '')
        if merge_la and (not keep_la or merge_la > keep_la):
            merged['Last Activity'] = merge_la

        # Date Messaged: keep most recent
        keep_dm = keep_f.get('Date Messaged', '')
        merge_dm = mf.get('Date Messaged', '')
        if merge_dm and (not keep_dm or merge_dm > keep_dm):
            merged['Date Messaged'] = merge_dm

        # Race Results: merge JSON arrays
        if mf.get('Race Results') and keep_f.get('Race Results'):
            try:
                keep_rr = json.loads(keep_f['Race Results'])
                merge_rr = json.loads(mf['Race Results'])
                # Combine, dedup by session+date
                existing_keys = {(r.get('session',''), r.get('date','')) for r in keep_rr}
                for r in merge_rr:
                    key = (r.get('session',''), r.get('date',''))
                    if key not in existing_keys:
                        keep_rr.append(r)
                merged['Race Results'] = json.dumps(keep_rr)
            except (json.JSONDecodeError, TypeError):
                pass

        # Championship: merge comma-separated
        if mf.get('Championship') and keep_f.get('Championship'):
            keep_c = set(c.strip() for c in keep_f['Championship'].split(','))
            merge_c = set(c.strip() for c in mf['Championship'].split(','))
            combined = keep_c | merge_c
            if combined != keep_c:
                merged['Championship'] = ', '.join(sorted(combined))

        # Notes: concatenate
        if mf.get('Notes') and keep_f.get('Notes'):
            if mf['Notes'] not in keep_f['Notes']:
                merged['Notes'] = keep_f['Notes'] + '\n' + mf['Notes']

        # AKA: add the merged record's name
        merge_name = mf.get('Full Name', '').strip()
        if merge_name and merge_name != keep_f.get('Full Name', '').strip():
            aka_parts.add(merge_name)

    # Assign FB URLs (primary + secondary)
    fb_list = sorted(all_fb_urls)
    if fb_list:
        # Primary FB URL: prefer personal profile over Page
        personal = [u for u in fb_list if '/p/' not in u and 'Racing' not in u and 'racing' not in u]

        primary = personal[0] if personal else fb_list[0]
        if primary != keep_f.get('FB URL', ''):
            merged['FB URL'] = primary

        # Secondary FB URL: save to Notes (Airtable can't auto-create URL fields)
        remaining = [u for u in fb_list if u != primary]
        if remaining:
            fb2_note = f"FB Racing Page: {remaining[0]}"
            existing_notes = merged.get('Notes') or keep_f.get('Notes', '')
            if fb2_note not in existing_notes:
                merged['Notes'] = (existing_notes + '\n' + fb2_note).strip() if existing_notes else fb2_note

    # AKA
    if aka_parts:
        new_aka = ', '.join(sorted(aka_parts))
        if new_aka != existing_aka:
            merged['Also Known As'] = new_aka

    return merged


def find_duplicate_groups(records):
    """Group records that are duplicates of each other."""
    groups = []
    used = set()

    # Build IG URL index
    ig_index = defaultdict(list)
    for r in records:
        ig = ig_username(r['fields'].get('IG URL', ''))
        if ig:
            ig_index[ig].append(r)

    # First pass: group by IG URL
    for ig, recs in ig_index.items():
        if len(recs) > 1:
            ids = tuple(sorted(r['id'] for r in recs))
            if ids not in used:
                groups.append(recs)
                used.add(ids)
                for r in recs:
                    used.add(r['id'])

    # Second pass: group by name overlap (for records not already grouped)
    ungrouped = [r for r in records if r['id'] not in used and not is_junk_record(r['fields'].get('Full Name', ''))]

    for i, r1 in enumerate(ungrouped):
        if r1['id'] in used:
            continue
        group = [r1]
        name1 = r1['fields'].get('Full Name', '')
        ig1 = ig_username(r1['fields'].get('IG URL', ''))

        for j, r2 in enumerate(ungrouped):
            if i >= j or r2['id'] in used:
                continue
            name2 = r2['fields'].get('Full Name', '')

            # Check name overlap
            if names_match(name1, name2):
                group.append(r2)
                used.add(r2['id'])

            # Check if handle name matches a real name
            elif is_handle_name(name2) and not is_handle_name(name1):
                handle_tokens = name_tokens(name2)
                real_tokens = name_tokens(name1)
                if handle_tokens and real_tokens and handle_tokens & real_tokens:
                    group.append(r2)
                    used.add(r2['id'])

        if len(group) > 1:
            used.add(r1['id'])
            groups.append(group)

    return groups


def main():
    parser = argparse.ArgumentParser(description='Merge duplicate Airtable records')
    parser.add_argument('--execute', action='store_true', help='Actually perform merges and deletes')
    parser.add_argument('--all', action='store_true', help='Check ALL records, not just today')
    parser.add_argument('--date', type=str, default=None, help='Specific date to check (YYYY-MM-DD)')
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()
    mode = "🔴 EXECUTE" if args.execute else "🟡 DRY RUN"

    print(f"{'='*60}")
    print(f"  Daily Merge — {mode}")
    print(f"  Date: {target_date if not args.all else 'ALL RECORDS'}")
    print(f"{'='*60}\n")

    # Fetch records
    date_filter = None if args.all else target_date
    records = fetch_all_records(date_filter)
    print(f"📊 Loaded {len(records)} records\n")

    # Step 1: Identify & clean junk records
    junk_records = [r for r in records if is_junk_record(r['fields'].get('Full Name', ''))]
    if junk_records:
        print(f"🗑️  JUNK RECORDS ({len(junk_records)}):")
        for r in junk_records:
            name = r['fields'].get('Full Name', '?')
            print(f"    ❌ {name} ({r['id']})")
        print()

    # Step 2: Clean junk FB URLs
    junk_fb = [(r, r['fields'].get('FB URL', '')) for r in records
               if is_junk_fb_url(r['fields'].get('FB URL', ''))]
    if junk_fb:
        print(f"🧹 JUNK FB URLs ({len(junk_fb)}):")
        for r, url in junk_fb:
            name = r['fields'].get('Full Name', '?')
            print(f"    🧹 {name:40s} → {url}")
            if args.execute:
                requests.patch(f"{BASE_URL}/{r['id']}", headers=HEADERS,
                             json={"fields": {"FB URL": ""}})
        print()

    # Step 3: Find duplicate groups
    valid_records = [r for r in records if not is_junk_record(r['fields'].get('Full Name', ''))]
    groups = find_duplicate_groups(valid_records)

    if not groups and not junk_records:
        print("✅ No duplicates found — database is clean!")
        return

    # Step 4: Process each group
    total_merged = 0
    total_deleted = 0

    for i, group in enumerate(groups):
        # Score each record — keep the one with most data
        scored = sorted(group, key=lambda r: record_score(r), reverse=True)
        keep = scored[0]
        discard = scored[1:]

        keep_name = keep['fields'].get('Full Name', '?')
        keep_score = record_score(keep)

        print(f"{'─'*50}")
        print(f"  Group {i+1}: {keep_name}")
        print(f"  KEEP:   {keep_name:40s} (score={keep_score}, stage={keep['fields'].get('Stage','')}, id={keep['id']})")
        for d in discard:
            d_name = d['fields'].get('Full Name', '?')
            d_score = record_score(d)
            print(f"  DELETE: {d_name:40s} (score={d_score}, stage={d['fields'].get('Stage','')}, id={d['id']})")

        # Calculate merged fields
        merged = merge_fields(keep, discard)
        if merged:
            print(f"  MERGE:  {list(merged.keys())}")

        if args.execute:
            # Patch the keep record with merged data
            if merged:
                resp = requests.patch(f"{BASE_URL}/{keep['id']}", headers=HEADERS,
                                    json={"fields": merged, "typecast": True})
                if resp.ok:
                    print(f"  ✅ Merged into {keep['id']}")
                    total_merged += 1
                else:
                    print(f"  ❌ Merge FAILED: {resp.status_code} {resp.text[:200]}")
                    continue  # Don't delete if merge failed

            # Delete discarded records
            for d in discard:
                resp = requests.delete(f"{BASE_URL}/{d['id']}", headers=HEADERS)
                if resp.ok:
                    print(f"  🗑️  Deleted {d['id']} ({d['fields'].get('Full Name', '?')})")
                    total_deleted += 1
                else:
                    print(f"  ❌ Delete FAILED: {resp.status_code}")
        print()

    # Delete junk records
    if junk_records and args.execute:
        print(f"{'─'*50}")
        print(f"  Deleting {len(junk_records)} junk records...")
        for r in junk_records:
            resp = requests.delete(f"{BASE_URL}/{r['id']}", headers=HEADERS)
            if resp.ok:
                print(f"  🗑️  Deleted junk: {r['fields'].get('Full Name', '?')}")
                total_deleted += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"    Duplicate groups:  {len(groups)}")
    print(f"    Junk records:      {len(junk_records)}")
    if args.execute:
        print(f"    Records merged:    {total_merged}")
        print(f"    Records deleted:   {total_deleted}")
    else:
        total_to_delete = sum(len(g) - 1 for g in groups) + len(junk_records)
        print(f"    Would delete:      {total_to_delete}")
        print(f"\n  Run with --execute to apply changes")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
