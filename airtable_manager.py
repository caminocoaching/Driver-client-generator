import os
import json
import streamlit as st
from pyairtable import Api
from typing import List, Dict, Optional, Any
from datetime import datetime


class AirtableSettingsStore:
    """
    Persistent key-value settings storage using an Airtable 'Settings' table.

    Requires a table called 'Settings' in your Airtable base with two fields:
      - Key   (Single line text, Primary field)
      - Value (Long text)

    Values are stored as JSON strings for flexibility.
    Includes an in-memory cache to avoid repeated API calls within a session.
    """

    def __init__(self, api_key: str, base_id: str, table_name: str = "Settings"):
        self.api = Api(api_key)
        self.table = self.api.table(base_id, table_name)
        self._cache: Dict[str, Any] = {}
        self._available = True  # Assume available until proven otherwise

    def get(self, key: str, default: Any = None) -> Any:
        """Get a setting value (JSON-decoded). Returns default if not found."""
        if not self._available:
            return default
        if key in self._cache:
            return self._cache[key]
        try:
            records = self.table.all(formula=f"{{Key}} = '{key}'", max_records=1)
            if records:
                raw = records[0]['fields'].get('Value', '')
                value = json.loads(raw) if raw else default
                self._cache[key] = value
                return value
        except Exception as e:
            print(f"Settings store read error ({key}): {e}")
            self._available = False
            return default
        return default

    def set(self, key: str, value: Any) -> bool:
        """Set a setting value (JSON-encoded). Returns True on success."""
        if not self._available:
            return False
        try:
            json_val = json.dumps(value)
            records = self.table.all(formula=f"{{Key}} = '{key}'", max_records=1)
            if records:
                self.table.update(records[0]['id'], {'Value': json_val})
            else:
                self.table.create({'Key': key, 'Value': json_val})
            self._cache[key] = value
            return True
        except Exception as e:
            print(f"Settings store write error ({key}): {e}")
            self._available = False
            return False

    @property
    def is_available(self) -> bool:
        return self._available


class AirtableManager:
    """
    Manages interactions with the Airtable API for the Driver Pipeline.
    Handles fetching, upserting, and identity resolution (linking Social -> Email).
    """
    def __init__(self, api_key: str, base_id: str, table_name: str = "Drivers"):
        self.api = Api(api_key)
        self.base_id = base_id
        self.table_name = table_name
        self.table = self.api.table(base_id, table_name)
        self.drivers_cache = []

    def fetch_all_drivers(self) -> List[Dict]:
        """Fetches all records from Airtable."""
        try:
            records = self.table.all()
            clean_records = []
            for r in records:
                fields = r['fields']
                fields['id'] = r['id']
                fields['createdTime'] = r['createdTime']
                clean_records.append(fields)
            self.drivers_cache = clean_records
            return clean_records
        except Exception as e:
            st.error(f"Error fetching drivers from Airtable: {e}")
            return []

    def upsert_driver(self, driver_data: Dict, record_id: str = None) -> bool:
        """
        Updates or Inserts a driver based on Identity Resolution logic.
        Primary ID: Full Name + Championship (social-first funnel).
        """
        full_name = driver_data.get('Full Name')
        if not full_name and driver_data.get('First Name') and driver_data.get('Last Name'):
            full_name = f"{driver_data['First Name']} {driver_data['Last Name']}".strip()

        championship = driver_data.get('Championship', '')

        max_retries = 5
        attempt = 0
        clean_data = {}
        for k, v in driver_data.items():
            if v is None:
                continue
            if k in ('Full Name', 'Notes'):
                continue
            if k == 'Email' and isinstance(v, str) and v.startswith("no_email_"):
                continue
            clean_data[k] = v

        while attempt < max_retries:
            try:
                if record_id:
                    self.table.update(record_id, clean_data, typecast=True)
                    return True

                if not full_name:
                    print("Skipping upsert: No Full Name provided.")
                    return False

                existing_record = self._find_match(full_name, championship)

                if existing_record:
                    found_id = existing_record['id']
                    self.table.update(found_id, clean_data, typecast=True)
                    return True
                else:
                    self.table.create(clean_data, typecast=True)
                    return True

            except Exception as e:
                error_str = str(e)
                if "Unknown field name" in error_str:
                    import re
                    match = re.search(r'Unknown field name: "(.+?)"', error_str)
                    if match:
                        bad_field = match.group(1)
                        print(f"Warning: Airtable rejected field '{bad_field}'. Removing and retrying.")
                        if bad_field in clean_data:
                            del clean_data[bad_field]
                            attempt += 1
                            continue

                st.error(f"Error upserting driver to Airtable: {e}")
                return False

        return False

    def delete_record(self, record_id: str) -> bool:
        """Delete a record from Airtable by its record ID."""
        try:
            self.table.delete(record_id)
            return True
        except Exception as e:
            print(f"Airtable delete error: {e}")
            return False

    def find_empty_records(self) -> List[Dict]:
        """Find Airtable records with no First Name AND no Email (ghost records)."""
        try:
            records = self.table.all(
                formula="AND({First Name} = '', {Last Name} = '', {Email} = '')"
            )
            return records
        except Exception as e:
            print(f"Error finding empty records: {e}")
            return []

    @staticmethod
    def _is_nickname(name1: str, name2: str) -> bool:
        """Check if two first names are nickname-compatible.
        'Chris' matches 'Christopher', 'Mike' matches 'Michael', etc."""
        a = name1.lower().strip()
        b = name2.lower().strip()
        if not a or not b:
            return False
        a_nicks = cls.NICKNAMES.get(a, [])
        b_nicks = cls.NICKNAMES.get(b, [])
        if b in a_nicks or a in b_nicks:
            return True
        # Check if both map to the same canonical name
        if any(n in b_nicks or b.startswith(n) or n.startswith(b) for n in a_nicks):
            return True
        if any(a.startswith(n) or n.startswith(a) for n in b_nicks):
            return True
        return a == b or a.startswith(b) or b.startswith(a)

    NICKNAMES = {
        'issy': ['isabelle', 'isabella', 'isabel'], 'izzy': ['isabelle', 'isabella', 'isabel'],
        'bill': ['william'], 'will': ['william'], 'bob': ['robert'], 'rob': ['robert'],
        'dick': ['richard'], 'rick': ['richard'], 'rich': ['richard'],
        'ted': ['edward', 'theodore'], 'ed': ['edward', 'eduardo'],
        'jim': ['james'], 'jimmy': ['james'], 'jack': ['john', 'jackson'],
        'chuck': ['charles'], 'charlie': ['charles'],
        'hank': ['henry'], 'harry': ['henry', 'harold', 'harrison'],
        'tony': ['anthony', 'antonio'], 'tom': ['thomas'], 'tommy': ['thomas'],
        'joe': ['joseph', 'josephine'], 'joey': ['joseph', 'josephine'],
        'ben': ['benjamin'], 'benny': ['benjamin'],
        'sam': ['samuel', 'samantha'], 'al': ['alexander', 'albert', 'alan'],
        'alex': ['alexander', 'alexandra', 'alejandro'],
        'andy': ['andrew', 'andreas'], 'drew': ['andrew'],
        'matt': ['matthew', 'matthias'], 'pat': ['patrick', 'patricia'],
        'kat': ['katherine', 'katrina'], 'kate': ['katherine', 'katrina'],
        'liz': ['elizabeth'], 'beth': ['elizabeth', 'bethany'],
        'jen': ['jennifer'], 'jenny': ['jennifer'], 'becky': ['rebecca'],
        'nick': ['nicholas', 'nicolas'], 'nicky': ['nicholas', 'nicolas'],
        'steve': ['steven', 'stephen'], 'steph': ['stephanie', 'stephen'],
        'dave': ['david'], 'dan': ['daniel', 'daniela'], 'danny': ['daniel'],
        'mike': ['michael'], 'mikey': ['michael'],
        'chris': ['christopher', 'christian', 'christina', 'christine'],
        'greg': ['gregory'], 'pete': ['peter'], 'fred': ['frederick', 'alfred'],
        'gus': ['august', 'augustus', 'angus'], 'seb': ['sebastian', 'sebastien'],
        'nico': ['nicolas', 'nicholas'], 'phil': ['philip', 'phillip'],
        'jess': ['jessica', 'jesse'], 'josh': ['joshua'],
        'zach': ['zachary'], 'zak': ['zachary'], 'nate': ['nathan', 'nathaniel'],
        'jake': ['jacob'], 'max': ['maxwell', 'maximilian'],
        'ray': ['raymond'], 'russ': ['russell'], 'ty': ['tyler', 'tyrel'],
    }

    def _find_match(self, full_name: str, championship: str = '') -> Optional[Dict]:
        """
        Identity Resolution — find existing Airtable record.
        Primary ID: Full Name + Championship (social-first funnel).

        Search order:
        1. Exact Full Name (prefer same championship if ambiguous)
        2. Also Known As (AKA) field
        3. First Name + Last Name + Championship (nickname-aware)
        4. First Name prefix + Last Name (single match only — skip if ambiguous)
        """
        # 1. Exact Full Name
        safe_name = full_name.replace("'", "\\'")
        matches = self.table.all(formula=f"{{Full Name}} = '{safe_name}'", max_records=3)
        if matches:
            if championship and len(matches) > 1:
                for m in matches:
                    if (m['fields'].get('Championship') or '').lower() == championship.lower():
                        return m
            return matches[0]

        # 2. Also Known As (AKA)
        safe_lower = full_name.lower().replace("'", "\\'")
        try:
            aka = self.table.all(
                formula=f"FIND('{safe_lower}', LOWER({{Also Known As}}))",
                max_records=5
            )
            if aka:
                if championship and len(aka) > 1:
                    for m in aka:
                        if (m['fields'].get('Championship') or '').lower() == championship.lower():
                            return m
                return aka[0]
        except Exception:
            pass

        parts = full_name.strip().split()
        if len(parts) >= 2:
            search_first = parts[0]
            search_last = ' '.join(parts[1:]).replace("'", "\\'")

            # 3. First Name + Last Name + Championship (nickname-aware)
            if championship:
                safe_champ = championship.replace("'", "\\'")
                try:
                    champ_matches = self.table.all(
                        formula=f"AND(LOWER({{Last Name}}) = '{search_last.lower()}', "
                                f"LOWER({{Championship}}) = '{safe_champ.lower()}')",
                        max_records=10
                    )
                    for m in champ_matches:
                        db_first = m['fields'].get('First Name') or ''
                        if self._is_nickname(search_first, db_first):
                            return m
                except Exception:
                    pass

            # 4. First Name prefix + Last Name (single match only)
            try:
                last_matches = self.table.all(
                    formula=f"LOWER({{Last Name}}) = '{search_last.lower()}'",
                    max_records=10
                )
                nick_matches = [m for m in last_matches
                                if self._is_nickname(search_first, m['fields'].get('First Name') or '')]
                if len(nick_matches) == 1:
                    return nick_matches[0]
            except Exception:
                pass

        return None
