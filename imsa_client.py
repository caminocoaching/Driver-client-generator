"""
IMSA (Al Kamel Cloud) Results Client — Free JSON Results
=========================================================
Fetches and parses race results from the IMSA results portal (Al Kamel cloud).
Source: https://imsa.results.alkamelcloud.com

Covers:
    - Porsche Carrera Cup North America
    - Porsche Sprint Challenge North America
    - IMSA VP Racing SportsCar Challenge
    - Whelen Mazda MX-5 Cup
    - Any other IMSA-sanctioned series

All results are published FREE as JSON, CSV, and PDF.

Usage:
    client = IMSAClient()
    event_info, names, driver_map = client.extract_driver_results(
        "https://imsa.results.alkamelcloud.com/Results/25_2025/06_Sebring%20International%20Raceway/"
    )

Output matches SpeedhiveClient.extract_driver_results() format for
seamless integration with the Race Outreach pipeline.
"""

import re
import json
import requests
from typing import List, Dict, Tuple, Optional
from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, quote


# Base URL for IMSA results on Al Kamel Cloud
BASE_URL = "https://imsa.results.alkamelcloud.com"
TIMEOUT = 20  # seconds

# Known IMSA series name fragments for filtering
PORSCHE_SERIES = [
    "Porsche Carrera Cup",
    "Porsche Sprint Challenge",
]


class _AlKamelLinkExtractor(HTMLParser):
    """Extract all <a href=...> links with their text from the IMSA results page."""

    def __init__(self):
        super().__init__()
        self.links = []  # list of {href, text}
        self._current_href = None
        self._current_text = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, val in attrs:
                if name == 'href' and val:
                    self._current_href = val
                    self._current_text = []

    def handle_data(self, data):
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag):
        if tag == 'a' and self._current_href is not None:
            self.links.append({
                'href': self._current_href,
                'text': ''.join(self._current_text).strip()
            })
            self._current_href = None
            self._current_text = []


class IMSAClient:
    """Client for fetching and parsing IMSA race results from Al Kamel Cloud.

    Results are published freely as JSON files at:
    https://imsa.results.alkamelcloud.com/Results/{YY}_{YYYY}/{NN}_{Event}/{NN}_{Series}/{Timestamp}_{Session}/
        03_Results_{Session}.JSON        — Classification with driver names
        03_Results_{Session}_Official.JSON  — Official (post-penalties) classification
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "text/html,application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) DriverPipeline/1.0",
        })

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def extract_event_path(url_or_path: str) -> Optional[str]:
        """Extract the event directory path from an IMSA URL or raw path.

        Accepts:
            https://imsa.results.alkamelcloud.com/Results/25_2025/06_Sebring%20International%20Raceway/
            Results/25_2025/06_Sebring International Raceway
            25_2025/06_Sebring International Raceway
        """
        url_or_path = url_or_path.strip()
        # Strip base URL if present
        if url_or_path.startswith("http"):
            url_or_path = url_or_path.split("alkamelcloud.com")[-1]

        # Decode URL encoding
        url_or_path = unquote(url_or_path)

        # Strip leading /Results/
        url_or_path = url_or_path.lstrip("/")
        if url_or_path.startswith("Results/"):
            url_or_path = url_or_path[len("Results/"):]

        # Should now be like: 25_2025/06_Sebring International Raceway/...
        parts = url_or_path.strip("/").split("/")
        if len(parts) >= 2:
            # Return year/event part
            return f"Results/{parts[0]}/{parts[1]}"
        elif len(parts) == 1 and re.match(r'\d{2}_\d{4}', parts[0]):
            # Just the year — return it for listing events
            return f"Results/{parts[0]}"
        return None

    @staticmethod
    def build_year_path(year: int = 2026) -> str:
        """Build the path for a specific year's results."""
        yy = str(year)[2:]  # e.g. "26"
        return f"Results/{yy}_{year}"

    # ------------------------------------------------------------------
    # HTML directory browsing
    # ------------------------------------------------------------------

    def _fetch_page(self, path: str) -> Optional[str]:
        """Fetch an HTML page from the IMSA results site."""
        url = f"{BASE_URL}/{path}" if not path.startswith("http") else path
        try:
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
        except Exception as e:
            print(f"[IMSA] Error fetching {url}: {e}")
        return None

    def _extract_links(self, html: str) -> List[Dict]:
        """Extract all links from an HTML page."""
        parser = _AlKamelLinkExtractor()
        parser.feed(html)
        return parser.links

    def _fetch_json(self, url: str) -> Optional[Dict]:
        """Fetch and parse a JSON results file."""
        try:
            if not url.startswith("http"):
                url = f"{BASE_URL}/{url}"
            r = self.session.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                # Handle BOM (byte order mark) that Al Kamel includes
                text = r.text.lstrip('\ufeff')
                return json.loads(text)
        except Exception as e:
            print(f"[IMSA] Error fetching JSON {url}: {e}")
        return None

    # ------------------------------------------------------------------
    # Discovery: list events and sessions
    # ------------------------------------------------------------------

    def list_events(self, year: int = 2026) -> List[Dict]:
        """List all events for a given year.

        Returns list of dicts with: name, path, index
        """
        year_path = self.build_year_path(year)
        html = self._fetch_page(year_path)
        if not html:
            return []

        links = self._extract_links(html)
        events = []
        for link in links:
            href = unquote(link['href'])
            # Links are relative like: 06_Sebring%20International%20Raceway/
            # Or absolute like: /Results/26_2026/06_Sebring.../
            match = re.match(r'(\d{2})_(.+?)/?$', href)
            if match:
                full_path = f"{year_path}/{match.group(0).rstrip('/')}"
                events.append({
                    'index': match.group(1),
                    'name': match.group(2).strip(),
                    'path': full_path,
                })
            else:
                # Try absolute path
                abs_match = re.search(r'Results/\d{2}_\d{4}/(\d{2})_(.+?)/?$', href)
                if abs_match:
                    events.append({
                        'index': abs_match.group(1),
                        'name': abs_match.group(2).strip(),
                        'path': href.lstrip('/').rstrip('/'),
                    })
        # Sort by index (chronological)
        events.sort(key=lambda e: e['index'])
        return events

    def list_series(self, event_path: str) -> List[Dict]:
        """List all series at an event.

        Args:
            event_path: e.g. "Results/25_2025/06_Sebring International Raceway"

        Returns list of dicts with: name, path, index
        """
        html = self._fetch_page(event_path)
        if not html:
            return []

        links = self._extract_links(html)
        series_list = []
        for link in links:
            href = unquote(link['href'])
            # Relative links like: 04_Porsche%20Carrera%20Cup%20North%20America/
            match = re.match(r'(\d{2})_(.+?)/?$', href)
            if match:
                name = match.group(2).strip()
                if name and len(name) > 3:
                    full_path = f"{event_path}/{match.group(0).rstrip('/')}"
                    series_list.append({
                        'index': match.group(1),
                        'name': name,
                        'path': full_path,
                    })
        series_list.sort(key=lambda s: s['index'])
        return series_list

    def list_sessions(self, series_path: str) -> List[Dict]:
        """List all sessions for a series at an event.

        Args:
            series_path: e.g. "Results/25_2025/06_.../04_Porsche Carrera Cup..."

        Returns list of dicts with: name, path, type, timestamp
        """
        html = self._fetch_page(series_path)
        if not html:
            return []

        links = self._extract_links(html)
        sessions = []
        for link in links:
            href = unquote(link['href'])
            # Relative links like: 202503131655_Race 1 /
            match = re.match(r'(\d{12})_(.+?)/?$', href)
            if match:
                name = match.group(2).strip()
                timestamp = match.group(1)
                full_path = f"{series_path}/{match.group(0).rstrip('/')}"
                # Classify session type
                name_lower = name.lower()
                if 'race' in name_lower:
                    stype = 'race'
                elif 'qualif' in name_lower or 'quali' in name_lower:
                    stype = 'qualify'
                elif 'practice' in name_lower or 'warmup' in name_lower or 'warm up' in name_lower:
                    stype = 'practice'
                else:
                    stype = 'other'

                sessions.append({
                    'name': name,
                    'path': full_path,
                    'type': stype,
                    'timestamp': timestamp,
                })
        sessions.sort(key=lambda s: s['timestamp'])
        return sessions

    def discover_json_files(self, session_path: str) -> List[Dict]:
        """List JSON result files available for a session.

        Returns list of dicts with: name, url, type
        """
        html = self._fetch_page(session_path)
        if not html:
            return []

        links = self._extract_links(html)
        json_files = []
        for link in links:
            href = link['href']
            text = link['text']
            if href.endswith('.JSON'):
                # Classify file type
                href_lower = href.lower()
                if '03_results' in href_lower:
                    ftype = 'results'
                elif '05_results' in href_lower:
                    ftype = 'results_by_class'
                elif '13_best' in href_lower:
                    ftype = 'best_sectors'
                elif '17_fastest' in href_lower:
                    ftype = 'fastest_laps'
                elif '23_time' in href_lower:
                    ftype = 'time_cards'
                else:
                    ftype = 'other'

                # Prefer Official results over provisional
                is_official = '_official' in href_lower or '_Official' in href

                # Resolve relative URL to full URL
                if href.startswith('http'):
                    full_url = href
                elif href.startswith('/'):
                    full_url = f"{BASE_URL}{href}"
                else:
                    # Relative — join with session_path
                    full_url = f"{BASE_URL}/{session_path}/{href}"

                json_files.append({
                    'name': text,
                    'url': full_url,
                    'type': ftype,
                    'is_official': is_official,
                })
        return json_files

    # ------------------------------------------------------------------
    # Result fetching
    # ------------------------------------------------------------------

    def fetch_session_results(self, session_path: str) -> Optional[Dict]:
        """Fetch and parse results JSON for a session.

        Prefers Official results over provisional.
        Returns the parsed JSON dict with: session, classification, fastest_lap
        """
        json_files = self.discover_json_files(session_path)

        # Find the best results file: official > provisional, results > results_by_class
        results_files = [f for f in json_files if f['type'] == 'results']
        if not results_files:
            return None

        # Prefer official
        official = [f for f in results_files if f['is_official']]
        target = official[0] if official else results_files[0]

        return self._fetch_json(target['url'])

    def get_sessions(self, event_url: str, series_filter: Optional[List[str]] = None) -> Tuple[Optional[Dict], List[Dict]]:
        """Get event info and list of available sessions.

        Args:
            event_url: URL or path to an IMSA event
            series_filter: Optional list of series name fragments to filter by
                          e.g. ["Porsche Carrera Cup", "Porsche Sprint Challenge"]

        Returns (event_info, sessions_list):
            event_info: dict with name, year, etc.
            sessions_list: list of session dicts with: name, path, type, series_name
        """
        event_path = self.extract_event_path(event_url)
        if not event_path:
            return None, []

        # Extract event name from path
        parts = event_path.split('/')
        event_name = parts[-1] if parts else ''
        # Strip index prefix (e.g. "06_Sebring International Raceway" → "Sebring International Raceway")
        event_name = re.sub(r'^\d{2}_', '', event_name)

        event_info = {
            'name': event_name,
            'path': event_path,
        }

        # List series at this event
        all_series = self.list_series(event_path)
        if series_filter:
            all_series = [
                s for s in all_series
                if any(frag.lower() in s['name'].lower() for frag in series_filter)
            ]

        # Collect sessions from each series
        all_sessions = []
        for series in all_series:
            sessions = self.list_sessions(series['path'])
            for s in sessions:
                s['series_name'] = series['name']
                s['series_path'] = series['path']
            all_sessions.extend(sessions)

        return event_info, all_sessions

    # ------------------------------------------------------------------
    # High-level: extract driver results (matches SpeedhiveClient interface)
    # ------------------------------------------------------------------

    def extract_driver_results(
        self,
        event_url: str,
        session_types: Optional[List[str]] = None,
        selected_sessions: Optional[List[str]] = None,
        series_filter: Optional[List[str]] = None,
    ) -> Tuple[Optional[Dict], List[str], Dict[str, List[Dict]]]:
        """High-level: extract driver names and per-driver results from an event.

        Matches SpeedhiveClient.extract_driver_results() output format.

        Args:
            event_url: IMSA results URL or path for an event
            session_types: Filter by type, e.g. ["race", "qualify"]. None = all.
            selected_sessions: Specific session paths to process (overrides type filter)
            series_filter: Filter by series name fragments (e.g. ["Porsche Carrera Cup"])

        Returns:
            (event_info, unique_names, driver_results_map)
        """
        event_info, sessions = self.get_sessions(event_url, series_filter)
        if not event_info:
            return None, [], {}

        # Apply session type filter
        if selected_sessions:
            sessions = [s for s in sessions if s['path'] in selected_sessions]
        elif session_types:
            sessions = [s for s in sessions if s['type'] in session_types]

        all_names = set()
        driver_map = {}  # name -> list of results

        for session in sessions:
            data = self.fetch_session_results(session['path'])
            if not data or 'classification' not in data:
                continue

            session_info = data.get('session', {})

            for entry in data['classification']:
                # Build driver name from drivers list
                drivers = entry.get('drivers', [])
                if not drivers:
                    continue

                # Combine all driver names (usually 1 for sprint races)
                driver_names = []
                for d in drivers:
                    first = d.get('firstname', '').strip()
                    last = d.get('surname', '').strip()
                    name = f"{first} {last}".strip()
                    if name:
                        driver_names.append(name)

                if not driver_names:
                    continue

                # Use primary driver name as key
                primary_name = driver_names[0]
                all_names.add(primary_name)

                result = {
                    'session_name': session.get('name', session_info.get('session_name', '')),
                    'session_type': session.get('type', ''),
                    'session_group': session.get('series_name', session_info.get('championship_name', '')),
                    'position': entry.get('position'),
                    'position_in_class': entry.get('position'),  # IMSA results are usually per-class
                    'best_lap': entry.get('fastest_lap_time', ''),
                    'total_time': entry.get('elapsed_time', ''),
                    'laps': int(entry.get('laps', 0)) if entry.get('laps') else 0,
                    'best_speed': float(entry.get('fastest_lap_kph', 0)) if entry.get('fastest_lap_kph') else 0,
                    'result_class': entry.get('class', ''),
                    'status': entry.get('status', 'Classified'),
                    'start_number': entry.get('number', ''),
                    'difference': entry.get('gap_first', ''),
                    # IMSA-specific extras
                    'team': entry.get('team', ''),
                    'vehicle': entry.get('vehicle', ''),
                    'co_drivers': driver_names[1:] if len(driver_names) > 1 else [],
                    'hometown': drivers[0].get('hometown', '') if drivers else '',
                    'country': drivers[0].get('country', '') if drivers else '',
                    'circuit': session_info.get('circuit', {}).get('name', ''),
                    'event_name': session_info.get('event_name', ''),
                }

                if primary_name not in driver_map:
                    driver_map[primary_name] = []
                driver_map[primary_name].append(result)

        return event_info, sorted(all_names), driver_map

    # ------------------------------------------------------------------
    # Convenience: list Porsche events for a year
    # ------------------------------------------------------------------

    def list_porsche_events(self, year: int = 2026) -> List[Dict]:
        """List all events that have Porsche series results for a given year.

        Returns list of dicts with: event_name, event_path, series (list of series names)
        """
        events = self.list_events(year)
        porsche_events = []

        for event in events:
            series_list = self.list_series(event['path'])
            porsche_series = [
                s for s in series_list
                if any(p.lower() in s['name'].lower() for p in PORSCHE_SERIES)
            ]
            if porsche_series:
                porsche_events.append({
                    'event_name': event['name'],
                    'event_path': event['path'],
                    'series': [s['name'] for s in porsche_series],
                })

        return porsche_events
