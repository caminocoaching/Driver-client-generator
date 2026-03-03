"""
Championship Researcher — AI-powered web research for motorsport championships.

Uses:
  - Tavily (free search API) to find championship websites
  - Gemini Flash (free LLM) to extract structured driver/calendar/results data

Flow:
  1. User enters a championship name (e.g. "GR86 Championship NZ 2025-2026")
  2. Tavily searches for official sites, driver pages, results
  3. Top pages are fetched and their text extracted
  4. Gemini Flash reads the content and returns structured JSON
  5. App presents the data for review and import
"""

import json
import re
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime

# ---------------------------------------------------------------------------
# HTML → plain-text helper (no extra dependency)
# ---------------------------------------------------------------------------
def _html_to_text(html: str, max_chars: int = 30_000) -> str:
    """Crude but effective HTML → readable text converter."""
    if not html:
        return ""
    # Remove script/style blocks
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
    # Replace <br>, <p>, <div>, <li>, <tr>, <h*> with newlines
    text = re.sub(r'<(br|/p|/div|/li|/tr|/h\d)[^>]*>', '\n', text, flags=re.IGNORECASE)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode common entities
    for entity, char in [('&amp;', '&'), ('&lt;', '<'), ('&gt;', '>'),
                          ('&quot;', '"'), ('&#39;', "'"), ('&nbsp;', ' ')]:
        text = text.replace(entity, char)
    # Collapse whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()[:max_chars]


# ---------------------------------------------------------------------------
# Tavily Search
# ---------------------------------------------------------------------------
def _tavily_search(api_key: str, query: str, max_results: int = 5) -> List[Dict]:
    """
    Search using Tavily API. Returns list of {title, url, content, raw_content}.
    Falls back gracefully if tavily-python is not installed.
    """
    try:
        from tavily import TavilyClient
        client = TavilyClient(api_key=api_key)
        response = client.search(
            query=query,
            max_results=max_results,
            search_depth="advanced",       # deeper scraping
            include_raw_content=True,       # get full page text
        )
        return response.get("results", [])
    except ImportError:
        # Fallback: use Tavily REST API directly
        resp = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key": api_key,
                "query": query,
                "max_results": max_results,
                "search_depth": "advanced",
                "include_raw_content": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])


# ---------------------------------------------------------------------------
# Page Fetcher (for URLs Tavily didn't fully scrape)
# ---------------------------------------------------------------------------
def _fetch_page_text(url: str, max_chars: int = 30_000) -> str:
    """Fetch a URL and return its text content."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return _html_to_text(resp.text, max_chars)
    except Exception as e:
        return f"[Failed to fetch {url}: {e}]"


# ---------------------------------------------------------------------------
# Gemini Flash — Structured Extraction
# ---------------------------------------------------------------------------

_EXTRACTION_PROMPT = """You are a motorsport data extraction expert. I will give you text content scraped from web pages about a racing championship.

Extract the following structured data and return it as a single JSON object. If a field is not found, use an empty list or empty string.

{{
  "championship_name": "Full official name of the championship",
  "season": "e.g. 2025-2026",
  "country": "Primary country (e.g. NZ, UK, USA)",
  "website": "Official website URL if found",
  "facebook": "Facebook page URL if found",
  "instagram": "Instagram handle/URL if found",
  
  "calendar": [
    {{
      "round": "R1",
      "name": "Event/venue name",
      "start_date": "YYYY-MM-DD",
      "end_date": "YYYY-MM-DD",
      "venue": "Circuit/track name"
    }}
  ],
  
  "drivers": [
    {{
      "number": "Car/race number (string)",
      "first_name": "First name",
      "last_name": "Last name",
      "nationality": "Country code or name",
      "team": "Team name if available"
    }}
  ],
  
  "timing_source": {{
    "type": "speedhive|tsl|computime|imsa|natsoft|none",
    "url": "URL to timing results page if found (e.g. speedhive.mylaps.com/events/..., tsl-timing.com/event/..., computime.racetecresults.com/...)",
    "event_id": "Event or meeting ID if extractable from the URL"
  }},
  
  "results_summary": "Brief text summary of any results found (e.g. championship standings, recent race winners)"
}}

IMPORTANT RULES:
- Dates MUST be in YYYY-MM-DD format. If only month/day is given, assume the most recent season year.
- For driver numbers, extract from text like "#1", "No. 1", "Car 1", etc.
- Nationality codes: use standard 3-letter codes (NZL, USA, GBR, AUS, etc.) or country names as found.
- If you see race results (positions, lap times), include them in results_summary.
- For timing_source, look for links or references to these timing providers:
  - Speedhive/MYLAPS: speedhive.mylaps.com URLs
  - TSL Timing: tsl-timing.com URLs (used by BSB, BTCC, British GT, British F4, GB3)
  - Computime: computime.racetecresults.com URLs (used by Australian/Porsche Cup AU)
  - IMSA / Al Kamel: results.imsa.com URLs (used by Porsche Cup NA, IMSA)
  - Natsoft: natsoft.com.au URLs (used by some Australian championships)
  - If none found, set type to "none"
- Return ONLY valid JSON, no markdown code fences, no commentary.

Here is the championship name the user is researching:
"{championship_query}"

Here is the scraped web content:

{content}
"""


def _gemini_extract(api_key: str, championship_query: str, content: str) -> Dict:
    """
    Use Gemini Flash to extract structured championship data from text.
    Returns parsed JSON dict.
    """
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash")
    except ImportError:
        # Fallback: use REST API directly
        return _gemini_extract_rest(api_key, championship_query, content)

    # Escape curly braces in content — scraped HTML/CSS/JS contains {} which
    # Python's .format() would try to interpret as placeholders
    _safe_content = content[:80_000].replace('{', '{{').replace('}', '}}')
    prompt = _EXTRACTION_PROMPT.format(
        championship_query=championship_query,
        content=_safe_content,
    )

    try:
        response = model.generate_content(
            prompt,
            generation_config={
                "temperature": 0.1,
                "max_output_tokens": 8192,
            }
        )
        raw_text = response.text.strip()
        # Strip markdown code fences if present
        if raw_text.startswith("```"):
            raw_text = re.sub(r'^```(?:json)?\s*', '', raw_text)
            raw_text = re.sub(r'\s*```$', '', raw_text)
        return json.loads(raw_text)
    except json.JSONDecodeError as e:
        return {"error": f"Failed to parse Gemini response as JSON: {e}", "raw": raw_text[:2000]}
    except Exception as e:
        return {"error": f"Gemini API error: {e}"}


def _gemini_extract_rest(api_key: str, championship_query: str, content: str) -> Dict:
    """Fallback: call Gemini via REST API without the SDK."""
    _safe_content = content[:80_000].replace('{', '{{').replace('}', '}}')
    prompt = _EXTRACTION_PROMPT.format(
        championship_query=championship_query,
        content=_safe_content,
    )
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 8192},
    }
    try:
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        raw_text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        if raw_text.startswith("```"):
            raw_text = re.sub(r'^```(?:json)?\s*', '', raw_text)
            raw_text = re.sub(r'\s*```$', '', raw_text)
        return json.loads(raw_text)
    except Exception as e:
        return {"error": f"Gemini REST error: {e}"}


# ---------------------------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------------------------

class ChampionshipResearcher:
    """
    AI-powered championship research.
    
    Usage:
        researcher = ChampionshipResearcher(tavily_key, gemini_key)
        result = researcher.research("GR86 Championship NZ 2025-2026")
        # result = {championship_name, calendar, drivers, results_summary, ...}
    """

    def __init__(self, tavily_api_key: str, gemini_api_key: str):
        self.tavily_key = tavily_api_key
        self.gemini_key = gemini_api_key

    def research(self, championship_query: str, progress_callback=None) -> Dict[str, Any]:
        """
        Full research pipeline:
        1. Search for the championship
        2. Fetch relevant pages
        3. Extract structured data with AI
        
        progress_callback(step, message) is called with updates if provided.
        """
        def _progress(step, msg):
            if progress_callback:
                progress_callback(step, msg)
            print(f"[Research {step}] {msg}")

        # ── Step 1: Search ──
        _progress(1, f"Searching for '{championship_query}'...")
        
        search_queries = [
            f"{championship_query} drivers entry list",
            f"{championship_query} calendar schedule rounds results",
        ]
        
        all_results = []
        seen_urls = set()
        
        for query in search_queries:
            try:
                results = _tavily_search(self.tavily_key, query, max_results=5)
                for r in results:
                    url = r.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_results.append(r)
            except Exception as e:
                _progress(1, f"Search error for '{query}': {e}")

        if not all_results:
            return {"error": "No search results found. Try a more specific championship name."}

        _progress(2, f"Found {len(all_results)} relevant pages")

        # ── Step 2: Gather content ──
        _progress(3, "Reading page content...")
        
        content_parts = []
        sources = []
        
        for r in all_results[:8]:  # Limit to top 8 pages
            url = r.get("url", "")
            title = r.get("title", "")
            
            # Prefer Tavily's raw_content (already scraped), fall back to our fetch
            page_text = r.get("raw_content") or r.get("content", "")
            
            if not page_text or len(page_text) < 100:
                # Tavily didn't get the full content — fetch it ourselves
                page_text = _fetch_page_text(url, max_chars=15_000)
            
            if page_text and len(page_text) > 50:
                content_parts.append(
                    f"\n{'='*60}\n"
                    f"SOURCE: {title}\n"
                    f"URL: {url}\n"
                    f"{'='*60}\n"
                    f"{page_text[:15_000]}"
                )
                sources.append({"title": title, "url": url})

        combined_content = "\n".join(content_parts)
        
        if len(combined_content) < 200:
            return {"error": "Could not fetch enough content from search results."}

        _progress(4, f"Collected {len(combined_content):,} chars from {len(sources)} pages")

        # ── Step 3: AI Extraction ──
        _progress(5, "AI analyzing content...")
        
        extracted = _gemini_extract(self.gemini_key, championship_query, combined_content)
        
        if "error" in extracted:
            return extracted

        # Enrich with metadata
        extracted["sources"] = sources
        extracted["searched_at"] = datetime.now().isoformat()
        extracted["query"] = championship_query

        # ── Post-processing ──
        self._post_process(extracted)

        _progress(6, f"Done! Found {len(extracted.get('drivers', []))} drivers, "
                     f"{len(extracted.get('calendar', []))} rounds")

        return extracted

    def research_from_url(self, url: str, progress_callback=None) -> Dict[str, Any]:
        """
        Research a championship from a direct URL.
        
        1. Fetches the main page
        2. Discovers related pages (calendar, drivers, results, entry-list)
        3. Fetches those pages too
        4. Feeds everything to Gemini for structured extraction
        
        This is faster and more accurate than Tavily search for known URLs.
        """
        def _progress(step, msg):
            if progress_callback:
                progress_callback(step, msg)
            print(f"[Research URL {step}] {msg}")

        from urllib.parse import urljoin, urlparse

        # ── Step 1: Fetch main page ──
        _progress(1, f"Fetching {url}...")
        main_text = _fetch_page_text(url, max_chars=30_000)
        
        if not main_text or len(main_text) < 100:
            return {"error": f"Could not fetch content from {url}"}

        content_parts = [
            f"\n{'='*60}\n"
            f"SOURCE: Main Championship Page\n"
            f"URL: {url}\n"
            f"{'='*60}\n"
            f"{main_text}"
        ]
        sources = [{"title": "Main Championship Page", "url": url}]

        # ── Step 2: Discover related pages ──
        _progress(2, "Finding related pages (calendar, drivers, results)...")
        
        # Also fetch the raw HTML to extract links
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36"
            }
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            raw_html = resp.text
        except Exception:
            raw_html = ""

        # Find related page URLs from the HTML
        related_keywords = [
            'calendar', 'results', 'drivers', 'entry-list', 'entry_list',
            'standings', 'schedule', 'rounds', 'teams', 'competitors',
            'participants', 'grid', 'lineup'
        ]
        
        parsed_base = urlparse(url)
        base_domain = parsed_base.netloc
        related_urls = set()
        
        if raw_html:
            # Find all href links
            import re as _re
            links = _re.findall(r'href=["\']([^"\']+)["\']', raw_html)
            for link in links:
                full_url = urljoin(url, link)
                parsed = urlparse(full_url)
                # Only follow links on the same domain
                if parsed.netloc != base_domain:
                    continue
                # Check if the URL path contains any related keywords
                path_lower = parsed.path.lower()
                if any(kw in path_lower for kw in related_keywords):
                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    if clean_url.rstrip('/') != url.rstrip('/'):
                        related_urls.add(clean_url)

        _progress(3, f"Found {len(related_urls)} related pages to fetch...")
        
        # ── Step 3: Fetch related pages ──
        for i, rurl in enumerate(list(related_urls)[:6]):  # Limit to 6 related pages
            _progress(3, f"Fetching page {i+1}/{min(len(related_urls), 6)}: {rurl}")
            page_text = _fetch_page_text(rurl, max_chars=20_000)
            if page_text and len(page_text) > 50:
                # Infer title from URL path
                path_parts = urlparse(rurl).path.rstrip('/').split('/')
                title = ' '.join(p.replace('-', ' ').replace('_', ' ').title() 
                                for p in path_parts[-2:] if p)
                content_parts.append(
                    f"\n{'='*60}\n"
                    f"SOURCE: {title}\n"
                    f"URL: {rurl}\n"
                    f"{'='*60}\n"
                    f"{page_text}"
                )
                sources.append({"title": title, "url": rurl})

        combined_content = "\n".join(content_parts)
        _progress(4, f"Collected {len(combined_content):,} chars from {len(sources)} pages")

        # ── Step 4: AI Extraction ──
        _progress(5, "AI analyzing content...")
        
        # Use a more descriptive query derived from the URL
        query_hint = urlparse(url).path.strip('/').replace('-', ' ').replace('/', ' ')
        extracted = _gemini_extract(self.gemini_key, query_hint, combined_content)
        
        if "error" in extracted:
            return extracted

        # Enrich with metadata
        extracted["sources"] = sources
        extracted["searched_at"] = datetime.now().isoformat()
        extracted["query"] = query_hint
        extracted["source_url"] = url

        # ── Post-processing ──
        self._post_process(extracted)

        _progress(6, f"Done! Found {len(extracted.get('drivers', []))} drivers, "
                     f"{len(extracted.get('calendar', []))} rounds")

        return extracted

    def _post_process(self, extracted: Dict):
        """Clean up extracted data — validate dates, deduplicate drivers."""
        # Ensure calendar dates are valid
        for event in extracted.get("calendar", []):
            for key in ["start_date", "end_date"]:
                val = event.get(key, "")
                if val:
                    try:
                        datetime.strptime(val, "%Y-%m-%d")
                    except ValueError:
                        event[key] = ""  # Clear invalid dates

        # Deduplicate drivers
        seen_drivers = set()
        unique_drivers = []
        for d in extracted.get("drivers", []):
            key = f"{d.get('first_name', '').lower()} {d.get('last_name', '').lower()}".strip()
            if key and key not in seen_drivers:
                seen_drivers.add(key)
                unique_drivers.append(d)
        extracted["drivers"] = unique_drivers


# ---------------------------------------------------------------------------
# Helper: Convert research results to RACE_CALENDARS format
# ---------------------------------------------------------------------------
def research_to_calendar_dict(data: Dict, color: str = "#607D8B") -> Dict:
    """Convert research results to the RACE_CALENDARS entry format."""
    rounds = []
    for event in data.get("calendar", []):
        if event.get("start_date") and event.get("end_date"):
            country = data.get("country", "")
            flag = {
                "NZ": "🇳🇿", "UK": "🇬🇧", "GB": "🇬🇧", "USA": "🇺🇸", "US": "🇺🇸",
                "AU": "🇦🇺", "DE": "🇩🇪", "FR": "🇫🇷", "IT": "🇮🇹", "ES": "🇪🇸",
                "JP": "🇯🇵", "AE": "🇦🇪", "NL": "🇳🇱", "AT": "🇦🇹", "BE": "🇧🇪",
            }.get(country.upper(), "")
            
            venue = event.get("venue") or event.get("name", "TBC")
            name = f"{venue} {flag}".strip()
            
            rounds.append({
                "round": event.get("round", f"R{len(rounds)+1}"),
                "name": name,
                "start": event["start_date"],
                "end": event["end_date"],
            })
    
    return {
        "color": color,
        "rounds": rounds,
    }


def research_to_driver_csv(data: Dict, championship_name: str = "") -> str:
    """Convert research results to CSV format for import."""
    lines = ["First Name,Last Name,Championship,Notes"]
    for d in data.get("drivers", []):
        first = d.get("first_name", "").replace(",", "")
        last = d.get("last_name", "").replace(",", "")
        champ = championship_name or data.get("championship_name", "")
        
        notes_parts = []
        if d.get("number"):
            notes_parts.append(f"#{d['number']}")
        if d.get("nationality"):
            notes_parts.append(d["nationality"])
        if d.get("team"):
            notes_parts.append(d["team"])
        notes = " · ".join(notes_parts)
        
        if first and last:
            lines.append(f"{first},{last},{champ},{notes}")
    
    return "\n".join(lines)
