#!/usr/bin/env python3
"""
Merge duplicate Airtable contacts created by the IG username bug.
Focuses on records created today that look like username-based duplicates.
"""

import requests
import sys
import re
from collections import defaultdict

import os

API_KEY = os.environ.get("AIRTABLE_API_KEY", "")
if not API_KEY:
    print("ERROR: Set AIRTABLE_API_KEY environment variable")
    sys.exit(1)
BASE_ID = "appOK1dNqufKg0bEd"
TABLE = "Riders"
URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE}"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}


def fetch_all_records():
    """Fetch all records from Airtable (handles pagination)."""
    records = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        resp = requests.get(URL, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def is_username(name):
    """Check if a name looks like a social media username (no spaces)."""
    if not name:
        return False
    name = name.strip()
    # Must have no spaces AND (contain digits or be all lowercase or contain dots/underscores)
    if ' ' in name:
        return False
    return bool(re.search(r'\d', name)) or name == name.lower() or '.' in name or '_' in name


def normalise_for_match(name):
    """Normalise a name for fuzzy matching: lowercase, strip digits, separators, spaces."""
    if not name:
        return ""
    return re.sub(r'[_.\-\d\s]', '', name.lower().strip())


def find_username_duplicates(records):
    """Find records where one is a username and another is the real name for the same person."""
    # Build a lookup of normalised names -> records
    by_norm = defaultdict(list)
    for rec in records:
        name = (rec.get("fields", {}).get("Full Name") or "").strip()
        if not name:
            continue
        norm = normalise_for_match(name)
        if len(norm) >= 4:
            by_norm[norm].append(rec)

    duplicates = []
    for norm, recs in by_norm.items():
        if len(recs) < 2:
            continue

        # Separate into usernames (no spaces) vs real names (have spaces)
        usernames = [r for r in recs if is_username(r["fields"].get("Full Name", ""))]
        real_names = [r for r in recs if not is_username(r["fields"].get("Full Name", ""))]

        # Only merge if we have BOTH a username record AND a real name record
        if usernames and real_names:
            # Keep the real-name record with the most data
            best_real = max(real_names, key=lambda r: sum(1 for v in r["fields"].values() if v))
            for u in usernames:
                duplicates.append((u, best_real))

    return duplicates


def merge_and_delete(username_rec, real_rec, dry_run=True):
    """Merge data from username_rec into real_rec, then delete username_rec."""
    u_fields = username_rec.get("fields", {})
    r_fields = real_rec.get("fields", {})
    u_name = u_fields.get("Full Name", "?")
    r_name = r_fields.get("Full Name", "?")
    u_id = username_rec["id"]
    r_id = real_rec["id"]

    # Fields to merge (copy from username record if real record is empty)
    merge_fields = ["IG URL", "FB URL", "Championship", "Tags", "Stage", "Notes", "Last Activity"]
    updates = {}

    for field in merge_fields:
        u_val = u_fields.get(field, "")
        r_val = r_fields.get(field, "")
        if u_val and not r_val:
            updates[field] = u_val

    print(f"\n{'[DRY RUN] ' if dry_run else ''}MERGE: '{u_name}' → '{r_name}'")
    print(f"  Dup record:  {u_id}")
    print(f"  Keep record: {r_id}")
    if updates:
        print(f"  Fields to copy: {updates}")
    else:
        print(f"  No new fields to copy")

    if dry_run:
        return True

    # Step 1: Update real record with merged data
    if updates:
        resp = requests.patch(
            f"{URL}/{r_id}",
            headers=HEADERS,
            json={"fields": updates, "typecast": True}
        )
        if not resp.ok:
            print(f"  ❌ ERROR updating {r_id}: {resp.status_code} {resp.text}")
            return False
        print(f"  ✅ Updated '{r_name}' with: {list(updates.keys())}")

    # Step 2: Delete the username record
    resp = requests.delete(f"{URL}/{u_id}", headers=HEADERS)
    if not resp.ok:
        print(f"  ❌ ERROR deleting {u_id}: {resp.status_code} {resp.text}")
        return False
    print(f"  🗑️  Deleted '{u_name}'")
    return True


def main():
    dry_run = "--execute" not in sys.argv

    print("=" * 60)
    print("AIRTABLE DUPLICATE MERGER (username → real name)")
    print("=" * 60)

    if dry_run:
        print("\n⚠️  DRY RUN — no changes will be made")
        print("   Run with --execute to apply\n")
    else:
        print("\n🔴 LIVE MODE — will merge and delete!\n")

    print("Fetching all records...")
    records = fetch_all_records()
    print(f"Found {len(records)} total records\n")

    print("Scanning for username duplicates...")
    duplicates = find_username_duplicates(records)

    if not duplicates:
        print("✅ No username duplicates found!")
        return

    print(f"Found {len(duplicates)} username duplicate(s):")

    success = 0
    for username_rec, real_rec in duplicates:
        if merge_and_delete(username_rec, real_rec, dry_run=dry_run):
            success += 1

    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"Would merge {success} duplicate(s)")
        print("Run with --execute to apply")
    else:
        print(f"✅ Merged {success}/{len(duplicates)} duplicate(s)")


if __name__ == "__main__":
    main()
