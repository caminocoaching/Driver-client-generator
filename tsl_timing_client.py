"""
TSL Timing — PDF Results Scraper for BSB
=========================================
Fetches and parses race results from TSL Timing (tsl-timing.com).
Used by British Superbikes (BSB), British Touring Cars, British GT, etc.

Usage:
    client = TSLTimingClient()
    event_info, names, driver_map = client.extract_driver_results(
        "https://www.tsl-timing.com/event/251804"
    )

Output matches SpeedhiveClient.extract_driver_results() format for
seamless integration with the Race Outreach pipeline.
"""

import re
import requests
from typing import List, Dict, Tuple, Optional
from html.parser import HTMLParser


class _TSLLinkExtractor(HTMLParser):
    """Extract all <a href=...> links with their text from an HTML page."""
    def __init__(self):
        super().__init__()
        self.links = []
        self._current_href = None
        self._current_text = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    self._current_href = value
                    self._current_text = []

    def handle_data(self, data):
        if self._current_href is not None:
            self._current_text.append(data.strip())

    def handle_endtag(self, tag):
        if tag == 'a' and self._current_href is not None:
            text = ' '.join(t for t in self._current_text if t)
            self.links.append({'href': self._current_href, 'text': text})
            self._current_href = None
            self._current_text = []


class _TSLSectionParser(HTMLParser):
    """Extract class sections (h3 headings) and their PDF links."""
    def __init__(self):
        super().__init__()
        self.sections = []  # list of {class_name, links: [{text, href}]}
        self._in_h3 = False
        self._current_section = None
        self._current_href = None
        self._current_text = []

    def handle_starttag(self, tag, attrs):
        if tag == 'h3':
            self._in_h3 = True
            self._current_text = []
        elif tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    self._current_href = value
                    self._current_text = []

    def handle_data(self, data):
        d = data.strip()
        if not d:
            return
        if self._in_h3:
            self._current_text.append(d)
        elif self._current_href is not None:
            self._current_text.append(d)

    def handle_endtag(self, tag):
        if tag == 'h3' and self._in_h3:
            name = ' '.join(self._current_text).strip()
            if name:
                self._current_section = {'class_name': name, 'links': []}
                self.sections.append(self._current_section)
            self._in_h3 = False
            self._current_text = []
        elif tag == 'a' and self._current_href is not None:
            text = ' '.join(t for t in self._current_text if t)
            if self._current_section and self._current_href.endswith('.pdf'):
                self._current_section['links'].append({
                    'text': text,
                    'href': self._current_href,
                })
            self._current_href = None
            self._current_text = []


def _split_tsl_name(raw: str) -> str:
    """Split TSL concatenated names like 'BradleyRAY' into 'Bradley RAY'.
    
    TSL PDFs concatenate first and last names. The pattern is:
    - First name: starts with uppercase, then lowercase
    - Last name: ALL UPPERCASE (may include hyphens, apostrophes)
    
    Handles: Mc/Mac prefixes (JohnMcPHEE → John McPHEE),
    'van/de/von' particles (JaimievanSIKKELERUS → Jaimie van SIKKELERUS),
    initials (TJTOMS → TJ TOMS), hyphenated (JoeSHELDON-SHAW → Joe SHELDON-SHAW)
    """
    if not raw:
        return raw or ""
    raw = raw.strip()
    if not raw:
        return raw

    # Already has a space — return as-is (DNF entries have normal names)
    if ' ' in raw:
        return raw

    # Handle 'van/de/von' particles: find lowered prefix before ALL-CAPS surname
    van_match = re.match(r'^([A-Z][a-z]+)(van|de|von|mc|Mac)([A-Z][A-Z\'-]+)$', raw)
    if van_match:
        return f"{van_match.group(1)} {van_match.group(2)}{van_match.group(3)}"

    # Handle Mc prefix: JohnMcPHEE, BillyMcCONNELL, MorganMcLAREN-WOOD
    mc_match = re.match(r'^([A-Z][a-z]+)(Mc[A-Z][A-Z\'-]+(?:-[A-Z]+)?)$', raw)
    if mc_match:
        return f"{mc_match.group(1)} {mc_match.group(2)}"

    # Standard split: find transition from lowercase to ALL-CAPS block
    # Walk through the string to find where a block of 2+ uppercase letters
    # runs through to the end (allowing hyphens/apostrophes)
    for i in range(1, len(raw)):
        if raw[i].isupper():
            # Check if everything from here to end is uppercase/hyphen/apostrophe
            rest = raw[i:]
            # Count uppercase chars (must be 2+)
            upper_chars = sum(1 for c in rest if c.isupper())
            all_ok = all(c.isupper() or c in "-'" for c in rest)
            if all_ok and upper_chars >= 2:
                return f"{raw[:i]} {raw[i:]}"

    return raw


def _parse_tsl_pdf(pdf_bytes: bytes) -> Optional[Dict]:
    """Parse a TSL Timing classification PDF and extract structured results.
    
    Handles two BSB column formats:
    - Superbike: POS NO NAME NAT ENTRY ... BEST ...
    - Supersport: POS NO CL PIC NAME NAT ENTRY ... BEST ...
    - CUP entries: POS NO CUP PIC NAME NAT ...
    """
    try:
        import pdfplumber
    except ImportError:
        print("[TSL] pdfplumber not installed — pip install pdfplumber")
        return None

    import io
    results = []
    event_name = ""
    class_name = ""
    session_name = ""
    total_laps = 0
    has_cl_column = False
    in_not_classified = False

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.split('\n')

                # Extract header info
                if not event_name and len(lines) >= 3:
                    event_name = lines[0].strip()
                    class_name = lines[1].strip()
                    for line in lines[:5]:
                        if 'CLASSIFICATION' in line.upper():
                            session_name = line.replace(' - CLASSIFICATION', '').replace('CLASSIFICATION', '').strip()
                            # Clean up " - SPRINT" etc.
                            session_name = session_name.replace(' - SPRINT', ' SPRINT').strip(' -')
                            break

                # Detect column format from header
                for line in lines[:6]:
                    if line.strip().startswith('POS'):
                        if ' CL ' in line or ' PIC ' in line:
                            has_cl_column = True
                        break

                # Extract total laps
                for line in lines:
                    m = re.search(r'Race Distance:\s*(\d+)\s*Laps', line)
                    if m:
                        total_laps = int(m.group(1))
                        break

                # Skip non-classification pages
                if 'CLASSIFICATION' not in text.upper():
                    continue

                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    if 'NOT CLASSIFIED' in line:
                        in_not_classified = True
                        continue
                    if line.startswith('FASTEST LAP') or line.startswith('Weather') or line.startswith('These results'):
                        break

                    # Skip headers / non-data lines
                    if line.startswith('POS') or line.startswith('Race Distance') or '↑↓' in line:
                        continue
                    if 'Championship' in line and not line[0].isdigit():
                        continue

                    # ── DNF / DNS rows (names are space-separated normally) ──
                    # Format: DNF 21 Christian IDDON GBR Kawasaki - ...
                    # Names in DNF rows are: First LAST (normal spacing, LAST is all caps)
                    dnf_match = re.match(
                        r'^(DNF|DNS|DQ|DSQ|EXC|RET)\s+(\d+)\s+([A-Z][a-z]+(?:\s+[a-z]+)?)\s+([A-Z][A-Z\'-]+(?:-[A-Z]+)?)\s+([A-Z]{2,3})\s+',
                        line
                    )
                    if dnf_match:
                        status = dnf_match.group(1)
                        number = dnf_match.group(2)
                        name = f"{dnf_match.group(3).strip()} {dnf_match.group(4).strip()}"
                        # Extract best lap (last M:SS.mmm pattern)
                        best_lap = ""
                        lap_times = re.findall(r'\d:\d{2}\.\d{3}', line)
                        if lap_times:
                            best_lap = lap_times[-1]

                        results.append({
                            'position': None, 'start_number': number,
                            'name': name, 'best_lap': best_lap,
                            'total_time': '', 'laps': 0, 'status': status,
                        })
                        continue

                    # ── Classified drivers ──
                    # Both formats start with: POS NO ...
                    if not line[0].isdigit():
                        continue

                    # Tokenize the line
                    tokens = line.split()
                    if len(tokens) < 4:
                        continue

                    try:
                        pos = int(tokens[0])
                    except ValueError:
                        continue
                    number = tokens[1]

                    # Detect CL PIC or CUP columns after the bike number
                    # Supersport: "1 8 1 LukeSTAPLEFORD GBR ..."  (CL=missing in header, PIC=1)
                    # CUP:        "22 25 CUP 1 LewisJONES GBR ..."
                    name_token_idx = 2  # default: token[2] is the name

                    if has_cl_column:
                        # Supersport format: POS NO CL_PIC NAME NAT ...
                        # token[2] is often a small number (class position), token[3] is name
                        # But sometimes CUP appears: "22 25 CUP 1 LewisJONES GBR ..."
                        if tokens[2] == 'CUP':
                            name_token_idx = 4  # skip CUP and PIC number
                        elif tokens[2].isdigit() and len(tokens) > 3:
                            # CL position number — name is next token
                            name_token_idx = 3
                    else:
                        # Superbike format: POS NO NAME NAT ...
                        name_token_idx = 2

                    if name_token_idx >= len(tokens):
                        continue

                    raw_name = tokens[name_token_idx]

                    # Validate: next token should be nationality (2-3 letter code)
                    nat_idx = name_token_idx + 1
                    if nat_idx >= len(tokens):
                        continue
                    nat = tokens[nat_idx]
                    if not re.match(r'^[A-Z]{2,3}$', nat):
                        continue

                    name = _split_tsl_name(raw_name)

                    # Extract best lap: in BSB format, the BEST column shows M:SS.mmm
                    # The line has multiple time values. Best lap is typically the
                    # second-to-last M:SS.mmm (last one could be M:SS.mmm from ON/GRD cols)
                    best_lap = ""
                    total_time = ""
                    all_times = re.findall(r'\d+:\d{2}\.\d{3}', line)
                    if all_times:
                        total_time = all_times[0]  # First is total race time
                        # BEST lap is after MPH column — usually 2nd time value
                        if len(all_times) >= 2:
                            best_lap = all_times[1]

                    results.append({
                        'position': pos,
                        'start_number': number,
                        'name': name,
                        'best_lap': best_lap,
                        'total_time': total_time,
                        'laps': total_laps,
                        'status': 'Normal',
                    })

    except Exception as e:
        print(f"[TSL] Error parsing PDF: {e}")
        import traceback
        traceback.print_exc()
        return None

    if not results:
        return None

    return {
        'event_name': event_name,
        'class_name': class_name,
        'session_name': session_name,
        'laps': total_laps,
        'rows': results,
    }


class TSLTimingClient:
    """Client for fetching and parsing TSL Timing results (BSB, BTCC, BGT etc.)."""

    BASE_URL = "https://www.tsl-timing.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) TSLClient/1.0"
        })

    @staticmethod
    def extract_event_id(url_or_id: str) -> Optional[str]:
        """Extract event ID from a TSL Timing URL or raw ID.
        
        Accepts:
            https://www.tsl-timing.com/event/251804
            251804
        """
        if not url_or_id:
            return None
        url_or_id = url_or_id.strip()

        m = re.search(r'/event/(\d+)', url_or_id)
        if m:
            return m.group(1)

        if url_or_id.isdigit():
            return url_or_id

        return None

    def fetch_event_page(self, event_id: str) -> str:
        """Fetch the HTML event page."""
        url = f"{self.BASE_URL}/event/{event_id}"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def discover_sessions(self, html: str) -> List[Dict]:
        """Parse event page HTML and extract session PDFs grouped by class.
        
        Returns list of session dicts with:
            id: unique key (e.g. "rc1sbk")
            name: human readable name (e.g. "Race 1 Result")
            class_name: e.g. "Bennetts British Superbike Championship with Pirelli"
            type: "race" | "qualify" | "practice" | "warmup" | "grid" | "other"
            pdf_url: full URL to the PDF
        """
        parser = _TSLSectionParser()
        parser.feed(html)

        sessions = []
        for section in parser.sections:
            for link in section['links']:
                href = link['href']
                text = link['text']

                if not href.endswith('.pdf'):
                    continue

                # Build full URL
                full_url = href if href.startswith('http') else f"{self.BASE_URL}{href}"

                # Determine type from link text
                text_lower = text.lower()
                if 'race' in text_lower and 'result' in text_lower:
                    stype = 'race'
                elif 'heat' in text_lower and 'result' in text_lower:
                    stype = 'race'
                elif 'sprint' in text_lower and 'result' not in text_lower and 'class' not in text_lower:
                    # Sprint race classification
                    stype = 'race'
                elif 'qualifying' in text_lower or text_lower.startswith('q'):
                    stype = 'qualify'
                elif 'practice' in text_lower or text_lower.startswith('fp'):
                    stype = 'practice'
                elif 'warm' in text_lower:
                    stype = 'warmup'
                elif 'grid' in text_lower:
                    stype = 'grid'
                elif 'combined' in text_lower:
                    stype = 'qualify'
                elif 'point' in text_lower or 'book' in text_lower:
                    continue  # Skip points tables and PDF books
                else:
                    # Check the filename for result type
                    fname = href.split('/')[-1].replace('.pdf', '')
                    if 'rc' in fname:
                        stype = 'race'
                    elif 'qu' in fname:
                        stype = 'qualify'
                    elif 'fp' in fname:
                        stype = 'practice'
                    else:
                        continue

                # Extract ID from filename
                fname = href.split('/')[-1].replace('.pdf', '')
                # Remove the event ID prefix to get session code
                session_id = re.sub(r'^\d+', '', fname)
                if not session_id:
                    session_id = fname

                sessions.append({
                    'id': session_id,
                    'name': text,
                    'class_name': section['class_name'],
                    'type': stype,
                    'pdf_url': full_url,
                })

        return sessions

    def get_sessions(self, event_id: str) -> Tuple[str, List[Dict]]:
        """Get event title and list of available sessions for an event.
        
        Returns (event_title, sessions_list)
        """
        html = self.fetch_event_page(event_id)

        # Extract event title from <title> tag
        title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
        event_title = ""
        if title_match:
            raw_title = title_match.group(1)
            # Clean up: "Event Details - BSB Round 1 - ... :: Timing Solutions Ltd."
            event_title = raw_title.split('::')[0].replace('Event Details -', '').strip().rstrip(' -')

        sessions = self.discover_sessions(html)
        return event_title, sessions

    def fetch_pdf(self, url: str) -> bytes:
        """Download a PDF file."""
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.content

    def fetch_session_results(self, session: Dict) -> Optional[Dict]:
        """Fetch and parse results for a single session."""
        try:
            pdf_bytes = self.fetch_pdf(session['pdf_url'])
            parsed = _parse_tsl_pdf(pdf_bytes)
            return parsed
        except Exception as e:
            print(f"[TSL] Error fetching {session['pdf_url']}: {e}")
            return None

    def extract_driver_results(
        self,
        url_or_id: str,
        session_types: Optional[List[str]] = None,
        selected_sessions: Optional[List[str]] = None
    ) -> Tuple[Dict, List[str], Dict]:
        """High-level: extract driver names and per-driver results from an event.
        
        Matches SpeedhiveClient.extract_driver_results() output format.
        
        Returns:
            (event_info, unique_names, driver_results_map)
        """
        event_id = self.extract_event_id(url_or_id)
        if not event_id:
            raise ValueError(f"Could not extract event ID from: {url_or_id}")

        event_title, sessions = self.get_sessions(event_id)

        # Filter sessions
        if selected_sessions:
            sessions = [s for s in sessions if s['id'] in selected_sessions]
        elif session_types:
            sessions = [s for s in sessions if s['type'] in session_types]

        all_names = set()
        driver_map = {}
        event_info = {'name': event_title, 'location': '', 'date': '', 'event_id': event_id}

        for session in sessions:
            parsed = self.fetch_session_results(session)
            if not parsed:
                continue

            # Capture event info from first parsed PDF
            if not event_info.get('date'):
                # Try to extract date from PDF footer
                event_info['name'] = event_info['name'] or parsed['event_name']

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
                    'session_group': f"{session['class_name']} — {parsed.get('session_name', session['name'])}",
                    'position': row['position'],
                    'position_in_class': row['position'],
                    'best_lap': row.get('best_lap', ''),
                    'total_time': row.get('total_time', ''),
                    'laps': row.get('laps', 0),
                    'best_speed': 0,
                    'result_class': session['class_name'],
                    'status': row.get('status', 'Normal'),
                    'start_number': row.get('start_number', ''),
                })

        return event_info, sorted(all_names), driver_map
