"""
Computime Race Timing Systems — PDF Results Scraper
====================================================
Fetches and parses race results from Computime timing sheets (PDF format).
Source: https://www.computime.com.au

Usage:
    client = ComputimeClient()
    event_info, names, driver_map = client.extract_driver_results(
        "https://www.computime.com.au/Web%20Services/Computime%20-%20WebServer%20Meetings/Resultspage?MeetID=17437"
    )

Output matches SpeedhiveClient.extract_driver_results() format for
seamless integration with the Race Outreach pipeline.
"""

import re
import requests
from typing import List, Dict, Tuple, Optional
from html.parser import HTMLParser


class _LinkExtractor(HTMLParser):
    """Extract all <a href=...> links from an HTML page."""
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    self.links.append(value)


def _parse_classification_pdf(pdf_bytes: bytes) -> Optional[Dict]:
    """Parse a Computime classification PDF and extract structured results.
    
    Returns dict with:
        event_name: e.g. "ASBK Round 1"
        class_name: e.g. "SW-MOTECH SUPERBIKE"
        session_name: e.g. "Race 1"
        date: e.g. "20/02/26"
        laps: number of laps
        rows: list of result dicts
    """
    try:
        import pdfplumber
    except ImportError:
        print("[Computime] pdfplumber not installed — pip install pdfplumber")
        return None

    import io
    results = []
    event_name = ""
    class_name = ""
    session_name = ""
    date_str = ""
    total_laps = 0

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.split('\n')

                # Extract header info from first page
                if not event_name and len(lines) >= 3:
                    event_name = lines[0].strip()
                    class_name = lines[1].strip()
                    session_name = lines[2].strip()

                # Extract date
                for line in lines:
                    m = re.search(r'Date:\s*(\S+)', line)
                    if m and not date_str:
                        date_str = m.group(1)
                    m = re.search(r'Laps:\s*(\d+)', line)
                    if m:
                        total_laps = int(m.group(1))

                # Check if this is a classification page (not lap times)
                if 'CLASSIFICATION' not in text.upper() and 'Pos No Name' not in text:
                    # Skip lap time pages, fastest lap pages, etc.
                    if 'LAP TIMES' in text.upper() or 'FASTEST LAPS' in text.upper():
                        continue

                # Parse result rows
                # Format: Pos No Name (State) / Sponsors Machine Laps Time ...
                # OR: DNF No Name ...
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    # Match classified drivers: "1 42 Riley NAUTA (QLD) / ... Machine 8 14:44.881 ..."
                    # Position is a number, bike number, then name with STATE in parens
                    pos_match = re.match(
                        r'^(\d+)\s+(\d+)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+[A-Z\'-]+(?:\s+[A-Z\'-]+)*)\s+\((\w+)\)',
                        line
                    )
                    # Also match: "DNF 23 Matthew RITTER (VIC) ..."
                    dnf_match = re.match(
                        r'^(DNF|DNS|DQ|DSQ)\s+(\d+)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+[A-Z\'-]+(?:\s+[A-Z\'-]+)*)\s+\((\w+)\)',
                        line
                    )

                    if pos_match:
                        pos = int(pos_match.group(1))
                        number = pos_match.group(2)
                        name = pos_match.group(3).strip()
                        state = pos_match.group(4)

                        # Extract machine, laps, time, fastest lap from rest of line
                        rest = line[pos_match.end():]
                        
                        # Try to extract fastest lap time (format: M:SS.mmm)
                        fastest_lap = ""
                        lap_times = re.findall(r'\d+:\d{2}\.\d{3}', rest)
                        if lap_times:
                            fastest_lap = lap_times[-1]  # Last time is usually fastest lap

                        # Extract total time
                        total_time = ""
                        if lap_times and len(lap_times) >= 1:
                            total_time = lap_times[0]  # First significant time

                        # Extract machine name (between state closing paren and lap count)
                        machine = ""
                        machine_match = re.search(r'\)\s*/.*?/\s*.*?\s+([\w\s-]+(?:YZF|CBR|Ninja|V4R|GSX|ZX|RSV|Ducati|Honda|Yamaha|Kawasaki|Suzuki|Aprilia|BMW)[\w\s-]*)', line)
                        if not machine_match:
                            # Try simpler pattern
                            machine_match = re.search(r'((?:Yamaha|Honda|Kawasaki|Ducati|Suzuki|Aprilia|BMW|KTM)\s+[\w-]+)', line)
                        if machine_match:
                            machine = machine_match.group(1).strip()

                        results.append({
                            'position': pos,
                            'start_number': number,
                            'name': name,
                            'state': state,
                            'machine': machine,
                            'best_lap': fastest_lap,
                            'total_time': total_time,
                            'laps': total_laps,
                            'status': 'Normal',
                        })

                    elif dnf_match:
                        status = dnf_match.group(1)
                        number = dnf_match.group(2)
                        name = dnf_match.group(3).strip()
                        state = dnf_match.group(4)

                        fastest_lap = ""
                        rest = line[dnf_match.end():]
                        lap_times = re.findall(r'\d+:\d{2}\.\d{3}', rest)
                        if lap_times:
                            fastest_lap = lap_times[-1]

                        # Extract laps completed for DNF
                        laps_done = 0
                        laps_m = re.search(r'(\d+)\s+\d+:\d{2}\.\d{3}', rest)
                        if laps_m:
                            laps_done = int(laps_m.group(1))

                        results.append({
                            'position': None,
                            'start_number': number,
                            'name': name,
                            'state': state,
                            'machine': '',
                            'best_lap': fastest_lap,
                            'total_time': '',
                            'laps': laps_done,
                            'status': status,
                        })

    except Exception as e:
        print(f"[Computime] Error parsing PDF: {e}")
        return None

    if not results:
        return None

    return {
        'event_name': event_name,
        'class_name': class_name,
        'session_name': session_name,
        'date': date_str,
        'laps': total_laps,
        'rows': results,
    }


# Pre-discovered MeetIDs for major championships (avoids postback scraping).
# Users can link new meetings via a one-time URL paste.
KNOWN_MEETINGS = {
    "ASBK": {
        "name": "Australian Superbike Championship",
        "meetings": {
            "2026": [
                {"meet_id": "17437", "name": "ASBK Round 1 (with WorldSBK)", "date": "2026-02-20", "location": "Phillip Island (Vic)"},
            ],
        },
    },
}


class ComputimeClient:
    """Client for fetching and parsing Computime race timing results."""

    BASE_URL = "https://www.computime.com.au"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ComputimeClient/1.0"
        })

    def fetch_meetings(self, year: int = 2026) -> List[Dict]:
        """Fetch all meetings from Computime for a given year.

        Scrapes the ASP.NET results page. Returns list of meeting dicts with:
            name, date, location, row_index (for __doPostBack), meet_id (if known)
        """
        url = f"{self.BASE_URL}/Web%20Services/Computime%20-%20WebServer%20Meetings/ResultsNew?ResultDate={year}"
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"[Computime] Error fetching {year} meetings: {e}")
            return []

        html = resp.text
        meetings = []

        # Split into table rows — use finditer to handle ASP.NET whitespace
        for tr_match in re.finditer(r'<tr\s', html):
            start = tr_match.start()
            end_tr = html.find('</tr>', start)
            if end_tr < 0:
                continue
            row_html = html[start:end_tr + 5]
            # Find the row index from onclick="...Select$N..."
            idx_match = re.search(r'Select\$(\d+)', row_html)
            if not idx_match:
                continue

            row_idx = int(idx_match.group(1))

            # Extract cell content — handles both <font>wrapped</font> and plain <td> formats
            # (Computime renders differently depending on User-Agent)
            fonts = re.findall(r'<font[^>]*>(.*?)</font>', row_html, re.DOTALL)
            cells = [f.strip() for f in fonts if '<input' not in f and f.strip()]

            if len(cells) < 4:
                # Fallback: parse <td> content directly (no font wrapping)
                tds = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
                cells = []
                for td in tds:
                    # Strip any inner tags
                    clean = re.sub(r'<[^>]+>', '', td).strip()
                    if clean and 'View' not in clean and len(clean) > 0:
                        cells.append(clean)

            # Cells should be: date, meeting_name, round, location, location_short
            if len(cells) < 4:
                continue

            date_str = cells[0]
            name = cells[1]
            round_num = cells[2]
            location = cells[3]

            # Convert DD/MM/YYYY to ISO
            parts = date_str.split('/')
            if len(parts) == 3:
                iso_date = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            else:
                iso_date = date_str

            meetings.append({
                'name': name.strip(),
                'date': iso_date,
                'location': location.strip(),
                'round': round_num.strip(),
                'row_index': row_idx,
            })

        # Enrich with known MeetIDs
        for m in meetings:
            for champ_data in KNOWN_MEETINGS.values():
                year_meetings = champ_data.get('meetings', {}).get(str(year), [])
                for km in year_meetings:
                    if km.get('date') == m['date'] or \
                       km.get('name', '').lower() in m['name'].lower():
                        m['meet_id'] = km['meet_id']

        return meetings

    def discover_meet_id(self, year: int, row_index: int) -> Optional[str]:
        """Simulate ASP.NET postback to discover a meeting's MeetID.

        Uses the __doPostBack mechanism from the results listing page.
        This makes a POST request and follows the redirect to get the MeetID.

        Args:
            year: Results year page
            row_index: Row index from fetch_meetings()

        Returns:
            MeetID string or None
        """
        url = f"{self.BASE_URL}/Web%20Services/Computime%20-%20WebServer%20Meetings/ResultsNew?ResultDate={year}"
        try:
            # First GET to grab viewstate etc.
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            html = resp.text

            # Extract ASP.NET form fields
            viewstate = re.search(r'__VIEWSTATE[^>]*value="([^"]*)"', html)
            viewstate_gen = re.search(r'__VIEWSTATEGENERATOR[^>]*value="([^"]*)"', html)
            event_val = re.search(r'__EVENTVALIDATION[^>]*value="([^"]*)"', html)

            if not viewstate:
                return None

            data = {
                '__EVENTTARGET': 'ctl00$MainContent$ResultsGV',
                '__EVENTARGUMENT': f'Select${row_index}',
                '__VIEWSTATE': viewstate.group(1),
            }
            if viewstate_gen:
                data['__VIEWSTATEGENERATOR'] = viewstate_gen.group(1)
            if event_val:
                data['__EVENTVALIDATION'] = event_val.group(1)

            # POST to trigger the postback — follow redirect to results page
            resp2 = self.session.post(url, data=data, timeout=15, allow_redirects=True)
            # The redirect URL should contain MeetID
            final_url = resp2.url
            m = re.search(r'MeetID=(\d+)', final_url)
            if m:
                return m.group(1)

        except Exception as e:
            print(f"[Computime] Error discovering MeetID: {e}")

        return None

    @staticmethod
    def extract_meet_id(url_or_id: str) -> Optional[str]:
        """Extract MeetID from a Computime URL or raw ID.
        
        Accepts:
            https://www.computime.com.au/.../Resultspage?MeetID=17437
            17437
        """
        if not url_or_id:
            return None
        url_or_id = url_or_id.strip()

        # Try URL parameter
        m = re.search(r'MeetID=(\d+)', url_or_id)
        if m:
            return m.group(1)

        # Raw numeric ID
        if url_or_id.isdigit():
            return url_or_id

        return None

    def fetch_meeting_page(self, meet_id: str) -> str:
        """Fetch the HTML results listing page for a meeting."""
        url = f"{self.BASE_URL}/Web%20Services/Computime%20-%20WebServer%20Meetings/Resultspage?MeetID={meet_id}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def discover_pdf_links(self, html: str) -> Dict[str, List[Dict]]:
        """Parse the results page HTML and extract PDF links grouped by session.
        
        Returns dict of session_code -> list of {type, url} dicts.
        Session codes like: P01, Q01, R01, R02, etc.
        Types: ALL (classification), CL (class), LT (lap times), FL (fastest laps), etc.
        """
        parser = _LinkExtractor()
        parser.feed(html)

        sessions = {}
        for link in parser.links:
            if not link.endswith('.pdf'):
                continue

            # Extract session code and type from filename
            # e.g. ASBK26_1_R01.pdf -> session=R01, type=ALL
            # e.g. ASBK26_1_R01_RES.pdf -> session=R01, type=RES
            filename = link.rstrip('/').split('/')[-1].replace('.pdf', '')
            parts = filename.split('_')

            # Find session code (P01, Q01, R01, etc.)
            session_code = None
            pdf_type = "ALL"
            for i, part in enumerate(parts):
                if re.match(r'^[PQR]\d{2}$', part):
                    session_code = part
                    # Everything after session code is the type
                    remaining = parts[i+1:]
                    if remaining:
                        pdf_type = '_'.join(remaining)
                    break

            if not session_code:
                # Try championship points and other special PDFs
                continue

            if session_code not in sessions:
                sessions[session_code] = []

            # Build full URL
            full_url = link if link.startswith('http') else f"{self.BASE_URL}{link}"

            sessions[session_code].append({
                'type': pdf_type,
                'url': full_url,
                'filename': filename,
            })

        return sessions

    def fetch_pdf(self, url: str) -> bytes:
        """Download a PDF file."""
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.content

    def get_sessions(self, meet_id: str) -> List[Dict]:
        """Get list of available sessions for a meeting.
        
        Returns list of session dicts with:
            id: session code (R01, Q01, etc.)
            name: human readable name (e.g. "Race 1", "Qualifying 1")
            type: "race" | "qualify" | "practice" 
            pdf_url: URL to the classification PDF
            group: session group/class name
        """
        html = self.fetch_meeting_page(meet_id)
        pdf_sessions = self.discover_pdf_links(html)

        sessions = []
        for code, pdfs in sorted(pdf_sessions.items()):
            # Determine session type from code
            if code.startswith('R'):
                stype = "race"
                num = int(code[1:])
                name = f"Race {num}"
            elif code.startswith('Q'):
                stype = "qualify"
                num = int(code[1:])
                name = f"Qualifying {num}"
            elif code.startswith('P'):
                stype = "practice"
                num = int(code[1:])
                name = f"Practice {num}"
            else:
                continue

            # Find the classification PDF (ALL type, or first available)
            cls_pdf = next((p for p in pdfs if p['type'] == 'ALL'), None)
            if not cls_pdf and pdfs:
                cls_pdf = pdfs[0]

            if cls_pdf:
                sessions.append({
                    'id': code,
                    'name': name,
                    'type': stype,
                    'pdf_url': cls_pdf['url'],
                    'all_pdfs': pdfs,
                    'group': '',  # Will be populated after parsing
                })

        return sessions

    def fetch_session_results(self, session: Dict) -> Optional[Dict]:
        """Fetch and parse results for a single session.
        
        Args:
            session: dict from get_sessions() with pdf_url
            
        Returns parsed classification dict or None.
        """
        try:
            pdf_bytes = self.fetch_pdf(session['pdf_url'])
            parsed = _parse_classification_pdf(pdf_bytes)
            if parsed:
                # Update session group with class name from PDF
                session['group'] = f"{parsed['class_name']} — {parsed['session_name']}"
            return parsed
        except Exception as e:
            print(f"[Computime] Error fetching {session['pdf_url']}: {e}")
            return None

    def extract_driver_results(
        self,
        url_or_id: str,
        session_types: Optional[List[str]] = None,
        selected_sessions: Optional[List[str]] = None
    ) -> Tuple[Dict, List[str], Dict]:
        """High-level: extract driver names and per-driver results from a meeting.
        
        Matches SpeedhiveClient.extract_driver_results() output format.
        
        Args:
            url_or_id: Computime URL or MeetID
            session_types: Filter by type: ["race", "qualify", "practice"]. None = all.
            selected_sessions: Specific session codes to fetch (e.g. ["R01", "R02"])
            
        Returns:
            (event_info, unique_names, driver_results_map)
            
            event_info: dict with name, location, date
            unique_names: sorted list of driver names
            driver_results_map: {name: [{session, pos, best_lap, total_time, laps, class, status}]}
        """
        meet_id = self.extract_meet_id(url_or_id)
        if not meet_id:
            raise ValueError(f"Could not extract MeetID from: {url_or_id}")

        sessions = self.get_sessions(meet_id)

        # Filter sessions
        if selected_sessions:
            sessions = [s for s in sessions if s['id'] in selected_sessions]
        elif session_types:
            sessions = [s for s in sessions if s['type'] in session_types]

        all_names = set()
        driver_map = {}
        event_info = {'name': '', 'location': '', 'date': '', 'meet_id': meet_id}

        for session in sessions:
            parsed = self.fetch_session_results(session)
            if not parsed:
                continue

            # Capture event info from first parsed PDF
            if not event_info['name']:
                event_info['name'] = parsed['event_name']
                event_info['date'] = parsed['date']

            for row in parsed['rows']:
                name = row['name']
                if not name:
                    continue

                all_names.add(name)
                if name not in driver_map:
                    driver_map[name] = []

                driver_map[name].append({
                    'session_name': session['name'],
                    'session_type': session['type'],
                    'session_group': session.get('group', ''),
                    'position': row['position'],
                    'position_in_class': row['position'],
                    'best_lap': row.get('best_lap', ''),
                    'total_time': row.get('total_time', ''),
                    'laps': row.get('laps', 0),
                    'best_speed': 0,
                    'result_class': parsed['class_name'],
                    'status': row.get('status', 'Normal'),
                    'start_number': row.get('start_number', ''),
                })

        return event_info, sorted(all_names), driver_map
