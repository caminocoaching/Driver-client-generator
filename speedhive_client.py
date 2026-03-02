"""
Speedhive (MYLAPS) API Client
Fetches race results, qualifying times, and lap data from Speedhive events.

API discovered from: https://github.com/ysmilda/speedhive-go
Base URL: https://eventresults-api.speedhive.com

Usage:
    client = SpeedhiveClient()
    event = client.fetch_event(3347675)
    sessions = client.fetch_sessions(3347675)
    results = client.fetch_session_results(session_id)

    # Browse events by organization
    events = client.fetch_organization_events(119509)  # CVMA org ID
"""

import requests
import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime


BASE_URL = "https://eventresults-api.speedhive.com"
TIMEOUT = 15  # seconds

# Known championship → Speedhive organization ID mapping
# These are discovered by looking up any event URL for the championship.
# Users can link new championships via a one-time URL paste.
KNOWN_ORGS = {
    # Add car racing Speedhive orgs here as they are discovered
    # e.g. "BTCC": {"org_id": 12345, "name": "British Touring Car Championship", "sport": "Car"},
}


class SpeedhiveClient:
    """Lightweight client for the Speedhive event results API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "DriverPipeline/1.0"
        })

    @staticmethod
    def extract_event_id(url_or_id: str) -> Optional[int]:
        """Extract event ID from a Speedhive URL or raw ID string.

        Accepts:
            https://speedhive.mylaps.com/events/3347675
            speedhive.mylaps.com/events/3347675
            3347675
        """
        url_or_id = url_or_id.strip()
        # Try direct integer
        if url_or_id.isdigit():
            return int(url_or_id)
        # Extract from URL
        match = re.search(r'events/(\d+)', url_or_id)
        if match:
            return int(match.group(1))
        return None

    def fetch_event(self, event_id: int) -> Optional[Dict]:
        """Fetch event metadata (name, location, dates, organization)."""
        try:
            r = self.session.get(f"{BASE_URL}/events/{event_id}", timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"[Speedhive] Error fetching event {event_id}: {e}")
        return None

    def fetch_sessions(self, event_id: int) -> List[Dict]:
        """Fetch all sessions for an event. Returns flat list of sessions with group info."""
        try:
            r = self.session.get(f"{BASE_URL}/events/{event_id}/sessions", timeout=TIMEOUT)
            if r.status_code != 200:
                return []
            data = r.json()
            sessions = []
            for group in data.get("groups", []):
                for s in group.get("sessions", []):
                    sessions.append({
                        "id": s["id"],
                        "name": s["name"],
                        "type": s.get("type", "unknown"),
                        "group": group.get("name", ""),
                        "start_time": s.get("startTime", ""),
                        "event_id": event_id,
                        "participated": s.get("participated", 0),
                    })
                for sub in group.get("subGroups", []):
                    for s in sub.get("sessions", []):
                        sessions.append({
                            "id": s["id"],
                            "name": s["name"],
                            "type": s.get("type", "unknown"),
                            "group": group.get("name", ""),
                            "start_time": s.get("startTime", ""),
                            "event_id": event_id,
                            "participated": s.get("participated", 0),
                        })
            return sessions
        except Exception as e:
            print(f"[Speedhive] Error fetching sessions for event {event_id}: {e}")
            return []

    def fetch_session_results(self, session_id: int) -> Optional[Dict]:
        """Fetch classification (results) for a session.

        Returns dict with:
            type: "Race" | "PracticeAndQualification" etc.
            classes: list of class names
            bestLap: {name, lapNumber, lapTime, speed}
            rows: list of result rows
        """
        try:
            r = self.session.get(f"{BASE_URL}/sessions/{session_id}/classification", timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"[Speedhive] Error fetching results for session {session_id}: {e}")
        return None

    def fetch_event_results(self, event_id: int, session_types: Optional[List[str]] = None) -> Tuple[Dict, List[Dict]]:
        """Fetch event info + results for selected session types.

        Args:
            event_id: Speedhive event ID
            session_types: Filter by type, e.g. ["race", "qualify"]. None = all.

        Returns:
            (event_info, list_of_session_results)
            Each session result has: session metadata + driver results
        """
        event = self.fetch_event(event_id)
        if not event:
            return None, []

        sessions = self.fetch_sessions(event_id)
        if session_types:
            sessions = [s for s in sessions if s["type"] in session_types]

        all_results = []
        for s in sessions:
            classification = self.fetch_session_results(s["id"])
            if classification and classification.get("rows"):
                all_results.append({
                    "session": s,
                    "classification": classification,
                })

        return event, all_results

    def extract_driver_results(self, event_id: int, session_types: Optional[List[str]] = None) -> Tuple[Optional[Dict], List[str], Dict[str, List[Dict]]]:
        """High-level: extract driver names and per-driver results from an event.

        Returns:
            (event_info, unique_names, driver_results_map)

            driver_results_map: {name: [{session, pos, best_lap, total_time, laps, class, speed, status}]}
        """
        event, session_results = self.fetch_event_results(event_id, session_types)
        if not event:
            return None, [], {}

        all_names = set()
        driver_map = {}  # name -> list of results

        for sr in session_results:
            session_info = sr["session"]
            rows = sr["classification"].get("rows", [])

            for row in rows:
                name = row.get("name", "").strip()
                if not name:
                    continue
                all_names.add(name)

                result = {
                    "session_name": session_info["name"],
                    "session_type": session_info["type"],
                    "session_group": session_info["group"],
                    "position": row.get("position"),
                    "position_in_class": row.get("positionInClass"),
                    "best_lap": row.get("bestTime", ""),
                    "total_time": row.get("totalTime", ""),
                    "laps": row.get("numberOfLaps", 0),
                    "best_speed": row.get("bestSpeed", 0),
                    "result_class": row.get("resultClass", ""),
                    "status": row.get("status", "Normal"),
                    "start_number": row.get("startNumber", ""),
                    "difference": row.get("difference", {}).get("timeDifference", ""),
                }

                if name not in driver_map:
                    driver_map[name] = []
                driver_map[name].append(result)

        return event, sorted(all_names), driver_map

    # ------------------------------------------------------------------
    # Organization / Event browsing
    # ------------------------------------------------------------------

    def fetch_organization(self, org_id: int) -> Optional[Dict]:
        """Fetch organization details by ID."""
        try:
            r = self.session.get(f"{BASE_URL}/organizations/{org_id}", timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"[Speedhive] Error fetching org {org_id}: {e}")
        return None

    def fetch_organization_events(self, org_id: int) -> List[Dict]:
        """Fetch all events for an organization (most recent first).

        Returns list of event dicts with: id, name, startDate, location, sport, etc.
        """
        try:
            r = self.session.get(f"{BASE_URL}/organizations/{org_id}/events", timeout=TIMEOUT)
            if r.status_code == 200:
                events = r.json()
                # Sort by start date descending (most recent first)
                events.sort(key=lambda e: e.get('startDate', ''), reverse=True)
                return events
        except Exception as e:
            print(f"[Speedhive] Error fetching events for org {org_id}: {e}")
        return []

    def discover_org_from_event(self, event_id: int) -> Optional[Dict]:
        """Given an event ID, discover the organization behind it.

        Returns dict with: org_id, name, sport, city, country
        Useful for linking a championship to its Speedhive org via a one-time URL paste.
        """
        event = self.fetch_event(event_id)
        if event and event.get('organization'):
            org = event['organization']
            return {
                'org_id': org['id'],
                'name': org.get('name', ''),
                'sport': org.get('sport', ''),
                'city': org.get('city', ''),
                'country': org.get('country', {}).get('name', ''),
            }
        return None
