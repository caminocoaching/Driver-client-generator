"""
MotoGP — Free Public API Client (PulseLive)
============================================
Fetches race results from the free, public MotoGP API at
api.pulselive.motogp.com.  No API key, no login, no paid tier.

The API hierarchy is:
    Season → Event → Category (MotoGP/Moto2/Moto3) → Session → Classification

Usage:
    from motogp_client import MotoGPClient
    client = MotoGPClient()
    event_info, unique_names, rider_map = client.extract_rider_results(
        "THA", year=2026, categories=["MotoGP™"]
    )

Output matches SpeedhiveClient.extract_rider_results() format for
seamless integration with the Race Outreach pipeline.
"""

import requests
from datetime import datetime
from typing import List, Dict, Tuple, Optional


# Session type mapping — MotoGP API uses short codes
SESSION_TYPE_MAP = {
    "RAC":  "race",
    "SPR":  "race",      # Sprint race
    "Q":    "qualify",
    "Q1":   "qualify",
    "Q2":   "qualify",
    "PR":   "practice",  # Practice (combined)
    "FP":   "practice",  # Free Practice
    "WUP":  "warmup",
}

# Human-readable session names
SESSION_NAME_MAP = {
    "RAC":  "Race",
    "SPR":  "Sprint Race",
    "Q":    "Qualifying",
    "Q1":   "Qualifying 1",
    "Q2":   "Qualifying 2",
    "PR":   "Practice",
    "FP":   "Free Practice",
    "WUP":  "Warm Up",
}


class MotoGPClient:
    """Client for fetching and parsing MotoGP results via the free PulseLive API."""

    BASE_URL = "https://api.pulselive.motogp.com/motogp"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "MotoGPClient/1.0",
        })
        # Cache for season/event/category lookups
        self._season_cache = {}
        self._events_cache = {}
        self._categories_cache = {}

    # ─── LOW-LEVEL API METHODS ──────────────────────────────────

    def fetch_seasons(self) -> List[Dict]:
        """Fetch all available seasons. Returns list of {id, year, current}."""
        url = f"{self.BASE_URL}/v1/results/seasons"
        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def get_season_uuid(self, year: int) -> Optional[str]:
        """Get the season UUID for a given year."""
        if year in self._season_cache:
            return self._season_cache[year]

        seasons = self.fetch_seasons()
        for s in seasons:
            self._season_cache[s['year']] = s['id']

        return self._season_cache.get(year)

    def fetch_events(self, season_uuid: str, finished_only: bool = False) -> List[Dict]:
        """Fetch all events for a season.

        Args:
            season_uuid: Season UUID from get_season_uuid()
            finished_only: If True, only return finished/current events.

        Returns list of event dicts with: id, name, short_name, circuit, country, etc.
        """
        cache_key = (season_uuid, finished_only)
        if cache_key in self._events_cache:
            return self._events_cache[cache_key]

        url = f"{self.BASE_URL}/v1/results/events"
        params = {"seasonUuid": season_uuid}
        if finished_only:
            params["isFinished"] = "true"
        resp = self.session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        events = resp.json()
        self._events_cache[cache_key] = events
        return events

    def fetch_categories(self, event_uuid: str) -> List[Dict]:
        """Fetch categories (MotoGP, Moto2, Moto3) for an event.

        Returns list of {id, name, legacy_id}.
        """
        if event_uuid in self._categories_cache:
            return self._categories_cache[event_uuid]

        url = f"{self.BASE_URL}/v1/results/categories"
        resp = self.session.get(url, params={"eventUuid": event_uuid}, timeout=15)
        resp.raise_for_status()
        cats = resp.json()
        self._categories_cache[event_uuid] = cats
        return cats

    def fetch_sessions(self, event_uuid: str, category_uuid: str) -> List[Dict]:
        """Fetch all sessions for an event + category.

        Returns list of session dicts with: id, type, number, date, status,
        session_files (with PDF URLs), condition, circuit.
        """
        url = f"{self.BASE_URL}/v1/results/sessions"
        params = {"eventUuid": event_uuid, "categoryUuid": category_uuid}
        resp = self.session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def fetch_classification(self, session_uuid: str) -> Optional[Dict]:
        """Fetch classification (results) for a session.

        Returns dict with:
            classification: list of rider result dicts
            session: {id, type, number, date, status}
            records: list of record dicts
            files: dict of PDF URLs
        """
        url = f"{self.BASE_URL}/v2/results/classifications"
        params = {"session": session_uuid, "test": "false"}
        resp = self.session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data or not data.get("classification"):
            return None
        return data

    # ─── HIGH-LEVEL METHODS ─────────────────────────────────────

    def find_event_by_short_name(self, short_name: str, year: int = 2026) -> Optional[Dict]:
        """Find an event by its short name (e.g. 'THA', 'BRA', 'USA').

        Args:
            short_name: 3-letter event code (from motogp.com URL)
            year: Season year

        Returns event dict or None.
        """
        season_uuid = self.get_season_uuid(year)
        if not season_uuid:
            return None

        events = self.fetch_events(season_uuid)
        short_name_upper = short_name.upper()
        for ev in events:
            if ev.get('short_name', '').upper() == short_name_upper:
                return ev
        return None

    def find_event_by_date(self, date_str: str, year: int = 2026) -> Optional[Dict]:
        """Find an event by start date (e.g. '2026-02-27').

        Matches within ±2 days. Excludes test events.

        Returns event dict or None.
        """
        season_uuid = self.get_season_uuid(year)
        if not season_uuid:
            return None

        events = self.fetch_events(season_uuid)
        target = datetime.strptime(date_str, "%Y-%m-%d")

        # First try exact match on non-test events
        for ev in events:
            if ev.get('test', False):
                continue
            ev_start = ev.get('date_start', '')[:10]
            if ev_start == date_str:
                return ev

        # Fuzzy match (±2 days) on non-test events
        for ev in events:
            if ev.get('test', False):
                continue
            ev_start = ev.get('date_start', '')[:10]
            try:
                ev_dt = datetime.strptime(ev_start, "%Y-%m-%d")
                if abs((ev_dt - target).days) <= 2:
                    return ev
            except ValueError:
                continue

        return None

    def get_sessions(self, event_uuid: str, categories: Optional[List[str]] = None) -> List[Dict]:
        """Get all sessions for an event, optionally filtered by category names.

        Args:
            event_uuid: Event UUID
            categories: Optional list of category names to include,
                        e.g. ["MotoGP™", "Moto2™"]. None = all.

        Returns list of session dicts enriched with:
            id, name, type, session_type (mapped), category_name,
            category_uuid, status, date
        """
        cats = self.fetch_categories(event_uuid)
        if categories:
            cats = [c for c in cats if c['name'] in categories]

        all_sessions = []
        for cat in cats:
            sessions = self.fetch_sessions(event_uuid, cat['id'])
            for s in sessions:
                s_type = s.get('type', '')
                s_number = s.get('number')

                # Build human-readable name
                base_name = SESSION_NAME_MAP.get(s_type, s_type)
                if s_number and s_number > 1 and s_type == 'FP':
                    display_name = f"Free Practice {s_number}"
                elif s_number and s_type == 'Q':
                    display_name = f"Qualifying {s_number}"
                else:
                    display_name = base_name

                all_sessions.append({
                    'id': s['id'],
                    'name': display_name,
                    'type': SESSION_TYPE_MAP.get(s_type, 'other'),
                    'api_type': s_type,
                    'category_name': cat['name'],
                    'category_uuid': cat['id'],
                    'status': s.get('status', ''),
                    'date': s.get('date', ''),
                    'session_files': s.get('session_files', {}),
                })

        return all_sessions

    def extract_rider_results(
        self,
        url_or_id: str,
        session_types: Optional[List[str]] = None,
        selected_sessions: Optional[List[str]] = None,
        year: int = 2026,
        categories: Optional[List[str]] = None,
    ) -> Tuple[Dict, List[str], Dict]:
        """High-level: extract rider names and per-rider results from an event.

        Matches SpeedhiveClient.extract_rider_results() output format.

        Args:
            url_or_id: MotoGP event short name (e.g. 'THA'), event UUID,
                       or date string (e.g. '2026-02-27')
            session_types: Filter by type: ["race", "qualify", "practice"]. None = all.
            selected_sessions: Specific session UUIDs to fetch. Overrides session_types.
            year: Season year (default 2026)
            categories: Category names to include, e.g. ["MotoGP™"]. None = all.

        Returns:
            (event_info, unique_names, rider_results_map)

            event_info: dict with name, location, date, circuit, country, etc.
            unique_names: sorted list of rider names
            rider_results_map: {name: [{session_name, session_type, session_group,
                                        position, best_lap, total_time, laps,
                                        best_speed, result_class, status, start_number, ...}]}
        """
        # Resolve the event
        event = self._resolve_event(url_or_id, year)
        if not event:
            return {}, [], {}

        event_uuid = event['id']

        # Build event_info
        circuit = event.get('circuit', {})
        country = event.get('country', {})
        event_info = {
            'name': event.get('sponsored_name') or event.get('name', ''),
            'location': circuit.get('place', ''),
            'date': event.get('date_start', ''),
            'circuit': circuit.get('name', ''),
            'country': country.get('name', ''),
            'country_iso': country.get('iso', ''),
            'short_name': event.get('short_name', ''),
            'event_uuid': event_uuid,
        }

        # Get sessions
        sessions = self.get_sessions(event_uuid, categories)

        # Filter sessions
        if selected_sessions:
            sessions = [s for s in sessions if s['id'] in selected_sessions]
        elif session_types:
            sessions = [s for s in sessions if s['type'] in session_types]

        # Only fetch FINISHED sessions
        sessions = [s for s in sessions if s['status'] == 'FINISHED']

        all_names = set()
        rider_map = {}

        for sess in sessions:
            classification = self.fetch_classification(sess['id'])
            if not classification:
                continue

            for entry in classification.get('classification', []):
                rider = entry.get('rider', {})
                name = rider.get('full_name', '').strip()
                if not name:
                    continue

                all_names.add(name)

                # Extract best lap time
                best_lap_data = entry.get('best_lap', {})
                best_lap_time = best_lap_data.get('time', '') if best_lap_data else ''

                # Extract gap info
                gap = entry.get('gap', {})
                gap_first = gap.get('first', '') if gap else ''

                result = {
                    'session_name': sess['name'],
                    'session_type': sess['type'],
                    'session_group': sess['category_name'],
                    'position': entry.get('position'),
                    'position_in_class': entry.get('position'),
                    'best_lap': best_lap_time,
                    'total_time': gap_first if entry.get('position', 0) > 1 else best_lap_time,
                    'laps': entry.get('total_laps', 0),
                    'best_speed': entry.get('top_speed', 0),
                    'result_class': sess['category_name'],
                    'status': entry.get('status', 'Normal'),
                    'start_number': str(rider.get('number', '')),
                    'difference': gap_first,
                    'team': entry.get('team_name', ''),
                    'constructor': entry.get('constructor', {}).get('name', ''),
                    'nationality': rider.get('country', {}).get('iso', ''),
                }

                if name not in rider_map:
                    rider_map[name] = []
                rider_map[name].append(result)

        return event_info, sorted(all_names), rider_map

    def _resolve_event(self, url_or_id: str, year: int) -> Optional[Dict]:
        """Resolve a user input to an event dict.

        Accepts:
            - Short name: 'THA', 'BRA', 'USA'
            - Date string: '2026-02-27'
            - UUID: 'f3fd8ba7-2966-46bd-8687-b92047f5e733'
            - URL: 'https://www.motogp.com/en/gp-results/2026/THA/...'
        """
        _input = url_or_id.strip()

        # Extract short name from URL
        # e.g. https://www.motogp.com/en/gp-results/2026/THA/MotoGP/RAC/Classification
        if 'motogp.com' in _input:
            parts = _input.rstrip('/').split('/')
            for i, p in enumerate(parts):
                if p == 'gp-results' and i + 2 < len(parts):
                    try:
                        year = int(parts[i + 1])
                    except ValueError:
                        pass
                    _input = parts[i + 2]
                    break

        # UUID (contains dashes, 36 chars)
        if len(_input) == 36 and _input.count('-') == 4:
            season_uuid = self.get_season_uuid(year)
            if season_uuid:
                events = self.fetch_events(season_uuid)
                for ev in events:
                    if ev['id'] == _input:
                        return ev
            return None

        # Date string (YYYY-MM-DD)
        if len(_input) == 10 and _input[4] == '-' and _input[7] == '-':
            return self.find_event_by_date(_input, year)

        # Short name (2-4 letter code)
        if 2 <= len(_input) <= 4 and _input.isalpha():
            return self.find_event_by_short_name(_input, year)

        return None

    # ─── CONVENIENCE METHODS ────────────────────────────────────

    def list_events(self, year: int = 2026, races_only: bool = True) -> List[Dict]:
        """List all events for a year. Useful for browsing.

        Args:
            year: Season year
            races_only: If True, exclude test events

        Returns list of simplified event dicts.
        """
        season_uuid = self.get_season_uuid(year)
        if not season_uuid:
            return []

        events = self.fetch_events(season_uuid)
        result = []
        for ev in events:
            if races_only and ev.get('test', False):
                continue
            result.append({
                'id': ev['id'],
                'short_name': ev.get('short_name', ''),
                'name': ev.get('sponsored_name') or ev.get('name', ''),
                'circuit': ev.get('circuit', {}).get('name', ''),
                'location': ev.get('circuit', {}).get('place', ''),
                'country': ev.get('country', {}).get('name', ''),
                'date_start': ev.get('date_start', ''),
                'date_end': ev.get('date_end', ''),
                'status': ev.get('status', ''),
            })
        return result

    def list_categories(self, event_short_name: str, year: int = 2026) -> List[str]:
        """List available categories for an event. Returns list of names like ['MotoGP™', 'Moto2™', 'Moto3™']."""
        event = self.find_event_by_short_name(event_short_name, year)
        if not event:
            return []
        cats = self.fetch_categories(event['id'])
        return [c['name'] for c in cats]
