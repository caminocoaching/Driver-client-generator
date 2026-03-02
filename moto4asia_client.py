"""
Moto4 Asia Cup (Asia Talent Cup) — Free PDF Results Client
==========================================================
Fetches and parses race results from the free PDF timing sheets
published on moto4asiacup.com.

The site structure:
    - Main results: https://www.moto4asiacup.com/results
    - Per-event pages: https://www.moto4asiacup.com/event/2026-2/{circuit-slug}/
    - PDFs: https://www.moto4asiacup.com/wp-content/uploads/...

PDF naming pattern:
    M4A_2026_THA-MotoGP_FRI_FP1_Full-Results.pdf
    M4A_2026_THA-MotoGP_SAT_QP_Classification.pdf
    M4A_2026_THA-MotoGP_SAT_RAC1_Classification.pdf
    M4A_2026_THA-MotoGP_SUN_RAC2_Classification.pdf

Usage:
    from moto4asia_client import Moto4AsiaClient
    client = Moto4AsiaClient()
    event_info, unique_names, rider_map = client.extract_rider_results()

Output matches SpeedhiveClient.extract_rider_results() format.
"""

import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Tuple, Optional

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False


# Event slug mapping for the 2026 calendar
EVENT_SLUGS_2026 = {
    "R1": "chang-international-circuit",
    "R2": "lusail-international-circuit",
    "R3": "petronas-sepang-international-circuit",
    "R4": "mobility-resort-motegi",
    "R5": "pertamina-mandalika-circuit",
    "R6": "petronas-sepang-international-circuit-2",
}

# Session type detection from PDF filename or title
SESSION_PATTERNS = {
    r'RAC\s*1|RACE\s*1': ('Race 1', 'race'),
    r'RAC\s*2|RACE\s*2': ('Race 2', 'race'),
    r'RAC|RACE': ('Race', 'race'),
    r'QP|QUAL': ('Qualifying', 'qualify'),
    r'FP\s*1|FREE PRACTICE NR\.\s*1|PRACTICE NR\.\s*1': ('Free Practice 1', 'practice'),
    r'FP\s*2|FREE PRACTICE NR\.\s*2|PRACTICE NR\.\s*2': ('Free Practice 2', 'practice'),
    r'FP|PRACTICE': ('Practice', 'practice'),
    r'WUP|WARM': ('Warm Up', 'warmup'),
}


class Moto4AsiaClient:
    """Client for the Idemitsu Moto4 Asia Cup (formerly Asia Talent Cup)."""

    BASE_URL = "https://www.moto4asiacup.com"
    RESULTS_URL = f"{BASE_URL}/results"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Moto4AsiaClient/1.0",
        })

    # ─── PDF DISCOVERY ──────────────────────────────────────────

    def discover_pdfs(self, year: int = 2026, round_key: str = None) -> List[Dict]:
        """Discover available PDF result files.

        Args:
            year: Season year
            round_key: Optional round key (e.g. 'R1') to filter by event.
                       If None, fetches all from the main results page.

        Returns list of dicts: {url, date, filename, session_name, session_type}
        """
        if round_key and round_key in EVENT_SLUGS_2026:
            slug = EVENT_SLUGS_2026[round_key]
            url = f"{self.BASE_URL}/event/{year}-2/{slug}/"
        else:
            url = self.RESULTS_URL

        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        pdfs = []
        for link in soup.find_all('a', href=lambda h: h and '.pdf' in h.lower()):
            href = link.get('href', '')
            if not href:
                continue

            filename = href.split('/')[-1]
            date_text = link.get_text(strip=True).replace('VIEW ONLINE', '').strip()

            # Detect session type from filename
            session_name, session_type = self._classify_session(filename)

            pdfs.append({
                'url': href,
                'filename': filename,
                'date': date_text,
                'session_name': session_name,
                'session_type': session_type,
            })

        return pdfs

    def _classify_session(self, filename: str) -> Tuple[str, str]:
        """Determine session name and type from PDF filename."""
        fn_upper = filename.upper()
        for pattern, (name, stype) in SESSION_PATTERNS.items():
            if re.search(pattern, fn_upper):
                return name, stype
        return filename, 'other'

    # ─── PDF PARSING ────────────────────────────────────────────

    def parse_classification_pdf(self, pdf_url: str) -> Optional[Dict]:
        """Download and parse a classification PDF.

        Returns dict with:
            event_name: e.g. "PT GRAND PRIX OF THAILAND"
            session_name: e.g. "Free Practice 1"
            session_type: e.g. "practice"
            circuit: e.g. "Chang International Circuit"
            date: e.g. "February 27, 2026"
            rows: list of rider result dicts
        """
        # Download PDF
        resp = self.session.get(pdf_url, timeout=30)
        resp.raise_for_status()

        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        try:
            return self._parse_pdf_file(tmp_path, pdf_url)
        finally:
            import os
            os.unlink(tmp_path)

    def _parse_pdf_file(self, pdf_path: str, source_url: str = '') -> Optional[Dict]:
        """Parse the classification page (page 1 only) of a Moto4 Asia Cup PDF."""
        # Only extract first page — it has the classification table.
        # Subsequent pages have detailed per-rider lap data with abbreviated names.
        text = self._extract_first_page(pdf_path)
        if not text:
            return None

        # Detect session from filename
        session_name, session_type = self._classify_session(source_url)

        # Extract event info from header
        event_name = 'Moto4 Asia Cup'
        circuit = ''
        date_str = ''

        # Look for event name (e.g. "GRAND PRIX OF THAILAND")
        gp_match = re.search(r'(?:GP|GRAND PRIX)\s+OF\s+([A-Z]+(?:\s+[A-Z]+)*)', text, re.IGNORECASE)
        if gp_match:
            event_name = f"Grand Prix of {gp_match.group(1).title()}"

        # Look for circuit name (e.g. "Chang International Circuit 4554 m.")
        circuit_match = re.search(r'([A-Z][\w\s-]+Circuit(?:\s+[\w]+)*?)\s+\d+\s*m', text)
        if circuit_match:
            circuit = circuit_match.group(1).strip()

        # Look for date at bottom
        date_match = re.search(r'(\w+),\s*(\w+,\s+\w+\s+\d+,\s+\d+)', text)
        if date_match:
            date_str = date_match.group(2).strip()

        # Detect session from text header too
        for pattern, (name, stype) in SESSION_PATTERNS.items():
            if re.search(pattern, text, re.IGNORECASE):
                session_name, session_type = name, stype
                break

        # Parse rider rows
        rows = self._parse_classification_rows(text)

        return {
            'event_name': event_name or 'Moto4 Asia Cup',
            'session_name': session_name,
            'session_type': session_type,
            'circuit': circuit,
            'date': date_str,
            'rows': rows,
        }

    def _parse_classification_rows(self, text: str) -> List[Dict]:
        """Parse rider classification rows from PDF text.

        Expected format on classification page:
        POS  #  RIDER_NAME  NATION  TEAM  MOTORCYCLE  TIME  LAP  TOTAL  GAP1  GAP2  SPEED
        """
        rows = []
        lines = text.split('\n')

        # Pattern for classification row:
        # 1 20 Noprutpong BUNPRAWES THA Idemitsu M4A - Thailand HONDA 1'48.755 4 7 210.5
        row_pattern = re.compile(
            r'^\s*(\d{1,2})\s+'           # Position
            r'(\d{1,3})\s+'               # Rider number
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+'  # First name(s)
            r'[A-Z]{2,}(?:\s+[A-Z]{2,})*)\s+'      # SURNAME(S)
            r'([A-Z]{2,3})\s+'            # Nation code
            r'(.*?)\s+'                   # Team
            r'(HONDA|YAMAHA|DUCATI|KTM|APRILIA|SUZUKI|BMW|MV AGUSTA)\s+'  # Motorcycle
            r"(\d+[':]\d+\.\d+)\s+"       # Time
            r'(\d+)\s+'                   # Best lap number
            r'(\d+)'                      # Total laps
        )

        for line in lines:
            match = row_pattern.match(line)
            if match:
                position = int(match.group(1))
                number = match.group(2)
                name_raw = match.group(3).strip()
                nation = match.group(4)
                team = match.group(5).strip()
                motorcycle = match.group(6)
                time = match.group(7)
                best_lap = int(match.group(8))
                total_laps = int(match.group(9))

                # Clean up name: "Noprutpong BUNPRAWES" -> "Noprutpong Bunprawes"
                name = self._normalize_name(name_raw)

                # Try to extract gap from remaining text
                remaining = line[match.end():].strip()
                gap = ''
                gap_match = re.match(r'([\d.]+)\s+([\d.]+)', remaining)
                if gap_match:
                    gap = gap_match.group(1)

                rows.append({
                    'position': position,
                    'start_number': number,
                    'name': name,
                    'nationality': nation,
                    'team': team,
                    'constructor': motorcycle,
                    'best_lap': time,
                    'total_laps': total_laps,
                    'gap': gap,
                    'status': 'Normal',
                })
            else:
                # Try a simpler pattern for lines that don't match the full pattern
                simple_match = re.match(
                    r'^\s*(\d{1,2})\s+(\d{1,3})\s+(.+?)\s+([A-Z]{2,3})\s+.*?(HONDA)\s+'
                    r"(\d+[':]\d+\.\d+)",
                    line
                )
                if simple_match:
                    position = int(simple_match.group(1))
                    number = simple_match.group(2)
                    name = self._normalize_name(simple_match.group(3).strip())
                    nation = simple_match.group(4)
                    time = simple_match.group(6)

                    rows.append({
                        'position': position,
                        'start_number': number,
                        'name': name,
                        'nationality': nation,
                        'team': '',
                        'constructor': 'HONDA',
                        'best_lap': time,
                        'total_laps': 0,
                        'gap': '',
                        'status': 'Normal',
                    })

        return rows

    def _normalize_name(self, raw: str) -> str:
        """Normalize rider name: 'Noprutpong BUNPRAWES' -> 'Noprutpong Bunprawes'."""
        parts = raw.split()
        normalized = []
        for p in parts:
            if p.isupper() and len(p) > 1:
                normalized.append(p.title())
            else:
                normalized.append(p)
        return ' '.join(normalized)

    def _extract_first_page(self, pdf_path: str) -> Optional[str]:
        """Extract text from the FIRST page only of a PDF file."""
        if HAS_PDFPLUMBER:
            with pdfplumber.open(pdf_path) as pdf:
                if pdf.pages:
                    return pdf.pages[0].extract_text() or ''
        elif HAS_PYPDF2:
            import PyPDF2
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                if reader.pages:
                    return reader.pages[0].extract_text() or ''
        return None

    def _extract_text(self, pdf_path: str) -> Optional[str]:
        """Extract text from a PDF file."""
        if HAS_PDFPLUMBER:
            with pdfplumber.open(pdf_path) as pdf:
                texts = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        texts.append(t)
                return '\n'.join(texts)
        elif HAS_PYPDF2:
            import PyPDF2
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                texts = []
                for page in reader.pages:
                    t = page.extract_text()
                    if t:
                        texts.append(t)
                return '\n'.join(texts)
        return None

    # ─── HIGH-LEVEL METHODS ─────────────────────────────────────

    def extract_rider_results(
        self,
        round_key: str = None,
        year: int = 2026,
        session_types: Optional[List[str]] = None,
    ) -> Tuple[Dict, List[str], Dict]:
        """High-level: extract rider names and per-rider results.

        Matches SpeedhiveClient.extract_rider_results() output format.

        Args:
            round_key: Round identifier (e.g. 'R1'). If None, fetches all available.
            year: Season year
            session_types: Filter by type: ["race", "qualify", "practice"]. None = all.

        Returns:
            (event_info, unique_names, rider_results_map)
        """
        pdfs = self.discover_pdfs(year, round_key)
        if not pdfs:
            return {}, [], {}

        # Filter by session type
        if session_types:
            pdfs = [p for p in pdfs if p['session_type'] in session_types]

        all_names = set()
        rider_map = {}
        event_info = {
            'name': 'Moto4 Asia Cup',
            'location': '',
            'date': '',
            'circuit': '',
            'country': '',
        }

        for pdf_info in pdfs:
            parsed = self.parse_classification_pdf(pdf_info['url'])
            if not parsed or not parsed.get('rows'):
                continue

            # Update event info from first parsed PDF
            if not event_info['circuit']:
                event_info.update({
                    'name': parsed.get('event_name', event_info['name']),
                    'circuit': parsed.get('circuit', ''),
                    'date': parsed.get('date', ''),
                })

            for row in parsed['rows']:
                name = row['name']
                if not name:
                    continue

                all_names.add(name)
                if name not in rider_map:
                    rider_map[name] = []

                rider_map[name].append({
                    'session_name': pdf_info.get('session_name') or parsed.get('session_name', ''),
                    'session_type': pdf_info.get('session_type') or parsed.get('session_type', ''),
                    'session_group': 'Moto4 Asia Cup',
                    'position': row['position'],
                    'position_in_class': row['position'],
                    'best_lap': row.get('best_lap', ''),
                    'total_time': row.get('gap', '') or row.get('best_lap', ''),
                    'laps': row.get('total_laps', 0),
                    'best_speed': 0,
                    'result_class': 'Moto4 Asia Cup',
                    'status': row.get('status', 'Normal'),
                    'start_number': row.get('start_number', ''),
                    'difference': row.get('gap', ''),
                    'team': row.get('team', ''),
                    'constructor': row.get('constructor', ''),
                    'nationality': row.get('nationality', ''),
                })

        return event_info, sorted(all_names), rider_map

    def list_events(self, year: int = 2026) -> List[Dict]:
        """List available events for a year.

        Returns list of dicts with round info.
        """
        events = []
        for round_key, slug in EVENT_SLUGS_2026.items():
            events.append({
                'round': round_key,
                'slug': slug,
                'url': f"{self.BASE_URL}/event/{year}-2/{slug}/",
            })
        return events
