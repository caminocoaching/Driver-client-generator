"""
Unified Event Selector — Championship-first flow.
================================================================
1. Pick a championship from the calendar
2. Pick the round (shows dates/circuits)
3. App auto-connects to the timing source, finds the event, loads riders

Also supports saved events and manual paste as fallback.

Usage:
    from event_selector import render_event_selector
    raw_results_list, rider_results, event_info = render_event_selector(settings)

Returns:
    raw_results_list: list of rider name strings
    rider_results: dict of {name: [session_result_dicts]} (Speedhive format)
    event_info: dict with event metadata or None
"""

import streamlit as st
from datetime import datetime

# ─── CHAMPIONSHIP → TIMING SOURCE MAPPING ────────────────────
# Maps each championship name to its timing provider and lookup config.
#
# source values:
#   "motogp"    — Free PulseLive API (auto-discover by date)
#   "speedhive" — Free Speedhive API (needs org_id for auto-discover)
#   "computime" — Computime scraper (auto-discover by date)
#   "tsl"       — TSL Timing (manual event link, then auto-load)
#   "moto4asia" — PDF timing sheet parser
#   "paste"     — Manual paste with championship-specific download link
#
# Championships without a free public API use "paste" + a results_url
# so the user can quickly grab names from the official results page.
CHAMPIONSHIP_TIMING_MAP = {
    # ── Car Racing Championships ──
    # Auto-discover (TSL covers most UK circuits)
    "BTCC":           {"source": "tsl", "event_id_prefix": "26"},
    "British F4":     {"source": "tsl", "event_id_prefix": "26"},
    "GB3":            {"source": "tsl", "event_id_prefix": "26"},
    "British GT":     {"source": "tsl", "event_id_prefix": "26"},
    "Porsche Cup GB": {"source": "tsl", "event_id_prefix": "26"},
    "F4 CEZ":         {"source": "tsl", "event_id_prefix": "26"},
    # ── Manual paste (no free public API) ──
    "UAE F4":         {"source": "paste", "results_url": "https://www.uaef4.com/results", "results_label": "uaef4.com — Results"},
    "IndyNXT":        {"source": "paste", "results_url": "https://www.indycar.com/results", "results_label": "indycar.com — Results"},
    "Porsche Sprint NA": {"source": "paste", "results_url": "https://www.porschesprint.com/results", "results_label": "porschesprint.com — Results"},
    "Euro F4":        {"source": "paste", "results_url": "https://www.euroformularacing.com/results/", "results_label": "euroformularacing.com — Results"},
    "F1 Academy":     {"source": "paste", "results_url": "https://www.fiaformula1academy.com/Results", "results_label": "F1 Academy — Results"},
}


def render_event_selector(settings):
    """Render the unified event selector and return results.

    Args:
        settings: AirtableSettingsStore instance (for persistence)

    Returns:
        (raw_results_list, rider_results_map, event_info)
    """
    raw_results_list = []
    rider_results = {}
    event_info = None

    # ── Load saved events from Airtable settings ──
    if '_saved_events' not in st.session_state:
        _stored = []
        if settings and settings.is_available:
            _stored = settings.get('saved_events', []) or []
        st.session_state['_saved_events'] = _stored
    _saved_events = st.session_state['_saved_events']

    def _persist_events():
        if settings and settings.is_available:
            settings.set('saved_events', st.session_state['_saved_events'])

    # ── Load Speedhive linked orgs ──
    if 'sh_linked_orgs' not in st.session_state:
        _stored_orgs = {}
        if settings and settings.is_available:
            _stored_orgs = settings.get('speedhive_orgs', {}) or {}
        try:
            from speedhive_client import KNOWN_ORGS
            _merged = dict(KNOWN_ORGS)
        except ImportError:
            _merged = {}
        _merged.update(_stored_orgs)
        st.session_state.sh_linked_orgs = _merged

    # ═══════════════════════════════════════════════════
    # MAIN SELECTOR: Championship-first flow
    # ═══════════════════════════════════════════════════
    _MODE_CHAMP = "🏆 Championship Calendar"
    _MODE_SAVED = "📋 Saved Events"
    _MODE_PASTE = "📝 Paste Names"
    _MODE_ADD   = "➕ Add Event Manually"

    _mode = st.radio(
        "Event Source",
        [_MODE_CHAMP, _MODE_SAVED, _MODE_PASTE, _MODE_ADD],
        key="event_source_mode", horizontal=True
    )

    # ═══════════════════════════════════════════════════
    # 🏆 CHAMPIONSHIP CALENDAR FLOW
    # ═══════════════════════════════════════════════════
    if _mode == _MODE_CHAMP:
        raw_results_list, rider_results, event_info = _championship_calendar_flow(
            settings, _saved_events, _persist_events
        )

    # ═══════════════════════════════════════════════════
    # 📋 SAVED EVENTS
    # ═══════════════════════════════════════════════════
    elif _mode == _MODE_SAVED:
        raw_results_list, rider_results, event_info = _saved_events_flow(
            settings, _saved_events, _persist_events
        )

    # ═══════════════════════════════════════════════════
    # 📝 PASTE TEXT
    # ═══════════════════════════════════════════════════
    elif _mode == _MODE_PASTE:
        text_input = st.text_area("Rider List (Name per line)", height=150)
        if text_input:
            raw_results_list = text_input.split('\n')

    # ═══════════════════════════════════════════════════
    # ➕ ADD EVENT MANUALLY
    # ═══════════════════════════════════════════════════
    elif _mode == _MODE_ADD:
        st.markdown("#### ➕ Add New Event")
        _add_source = st.radio(
            "Timing Source",
            ["🌐 Speedhive", "🕐 Computime", "🇬🇧 TSL (BSB/BTCC)", "🏍️ MotoGP", "🏆 Moto4 Asia"],
            key="add_event_source", horizontal=True
        )

        if _add_source == "🌐 Speedhive":
            _add_speedhive_event(settings, _saved_events, _persist_events)
        elif _add_source == "🕐 Computime":
            _add_computime_event(_saved_events, _persist_events)
        elif _add_source == "🇬🇧 TSL (BSB/BTCC)":
            _add_tsl_event(_saved_events, _persist_events)
        elif _add_source == "🏍️ MotoGP":
            _add_motogp_event(_saved_events, _persist_events)
        elif _add_source == "🏆 Moto4 Asia":
            _add_moto4asia_event(_saved_events, _persist_events)

    return raw_results_list, rider_results, event_info


# ─── CHAMPIONSHIP CALENDAR FLOW ──────────────────────────────

def _championship_calendar_flow(settings, saved_events, persist_fn):
    """Championship-first flow: pick championship → pick round → auto-load riders."""
    from championship_calendars import CHAMPIONSHIP_CALENDARS

    raw_results_list = []
    rider_results = {}
    event_info = None

    # 1. Championship dropdown
    champ_names = sorted([c['name'] for c in CHAMPIONSHIP_CALENDARS])
    _champ_col, _round_col = st.columns([1, 2])

    with _champ_col:
        selected_champ_name = st.selectbox(
            "🏆 Championship",
            options=champ_names,
            key="cal_champ_select",
            help="Select the championship from your calendar"
        )

    # Find the championship data
    champ_data = next((c for c in CHAMPIONSHIP_CALENDARS if c['name'] == selected_champ_name), None)
    if not champ_data:
        return [], {}, None

    # 2. Round dropdown — only show race rounds (not TEST)
    race_events = [e for e in champ_data['events'] if e['round'] != 'TEST']
    all_events = champ_data['events']

    with _round_col:
        # Build display labels
        event_opts = {i: f"{e['round']}  —  {e['name']}  ({e['start']})" for i, e in enumerate(all_events)}
        selected_event_idx = st.selectbox(
            "📅 Round / Event",
            options=list(event_opts.keys()),
            format_func=lambda x: event_opts[x],
            key="cal_round_select",
            help="Select the race round to load riders from"
        )

    if selected_event_idx is None:
        return [], {}, None

    selected_event = all_events[selected_event_idx]
    _event_date = selected_event['start']
    _event_name = selected_event['name']

    # Auto-fill circuit and championship
    # Extract circuit name from event name (remove country prefix if present)
    _circuit = _event_name.split('—')[-1].strip() if '—' in _event_name else _event_name
    st.session_state['_prefill_circuit'] = _circuit
    st.session_state['global_championship'] = selected_champ_name

    # 3. Look up timing source for this championship
    timing_config = CHAMPIONSHIP_TIMING_MAP.get(selected_champ_name)
    if not timing_config:
        st.warning(f"⚠️ No timing source configured for **{selected_champ_name}** yet. Use 'Add Event Manually' to link one.")
        st.caption("You can still use 'Paste Names' to manually enter rider names.")
        return [], {}, None

    _source = timing_config['source']

    # ── PASTE MODE: Show inline paste area with championship-specific link ──
    if _source == 'paste':
        _results_url = timing_config.get('results_url', '')
        _results_label = timing_config.get('results_label', 'Official Results')
        st.caption(f"Data source: 📋 Manual Paste")

        # Styled championship summary bar
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);border-left:4px solid #e94560;'
            f'border-radius:8px;padding:12px 18px;margin:8px 0;font-size:16px;color:#eee;">'
            f'🏁 <strong>{selected_champ_name}</strong> &nbsp;·&nbsp; {_circuit}'
            f'</div>', unsafe_allow_html=True
        )

        _paste_chk = st.checkbox("📋 Paste driver names manually", value=True, key=f"paste_chk_{selected_champ_name}")
        if _results_url:
            st.markdown(f"📥 Download results from [{_results_label}]({_results_url}) → copy driver names → paste below")

        if _paste_chk:
            text_input = st.text_area("Driver List (Name per line)", height=150, key=f"paste_area_{selected_champ_name}")
            if text_input:
                raw_results_list = [n.strip() for n in text_input.split('\n') if n.strip()]

        return raw_results_list, rider_results, event_info

    # ── API MODE: Auto-discover from timing source ──
    st.caption(f"🔗 Timing source: **{_source.title()}** | 📅 {_event_date} | 📍 {_circuit}")

    # 4. Auto-discover and load event from timing source
    _cache_key = f"cal_auto_{selected_champ_name}_{_event_date}"
    if _cache_key not in st.session_state:
        with st.spinner(f"🔍 Searching {_source.title()} for {selected_champ_name} {_event_name}..."):
            result = _auto_discover_event(_source, timing_config, selected_champ_name, _event_date, _event_name, settings)
            st.session_state[_cache_key] = result

    discovery = st.session_state[_cache_key]

    if discovery and discovery.get('found'):
        _src_id = discovery['source_id']
        _src = discovery['source']
        st.success(f"✅ Found: **{discovery.get('event_title', _event_name)}**")

        # Auto-save to saved events if not already there
        _already_saved = any(
            e.get('source_id') == str(_src_id) and e.get('source') == _src
            for e in saved_events
        )
        if not _already_saved:
            saved_events.insert(0, {
                "source": _src,
                "source_id": str(_src_id),
                "event_name": discovery.get('event_title', _event_name),
                "circuit": _circuit,
                "championship": selected_champ_name,
                "date": _event_date,
            })
            st.session_state['_saved_events'] = saved_events
            persist_fn()

        # Load results
        if _src == 'speedhive':
            raw_results_list, rider_results, event_info = _load_speedhive_event(str(_src_id))
        elif _src == 'computime':
            raw_results_list, rider_results, event_info = _load_computime_event(str(_src_id))
        elif _src == 'tsl':
            raw_results_list, rider_results, event_info = _load_tsl_event(str(_src_id))
        elif _src == 'motogp':
            raw_results_list, rider_results, event_info = _load_motogp_event(str(_src_id))
        elif _src == 'moto4asia':
            raw_results_list, rider_results, event_info = _load_moto4asia_event(str(_src_id))
    else:
        _reason = discovery.get('reason', 'No matching event found') if discovery else 'Search failed'
        st.warning(f"⚠️ Could not auto-discover event: {_reason}")
        st.caption("Try 'Add Event Manually' or 'Paste Names' instead.")

    return raw_results_list, rider_results, event_info


def _auto_discover_event(source, config, champ_name, event_date, event_name, settings):
    """Try to auto-discover a timing event by championship + date.

    Returns dict with: found, source, source_id, event_title
    Or: found=False, reason=...
    """
    try:
        if source == 'speedhive':
            return _discover_speedhive_event(config, champ_name, event_date, event_name, settings)
        elif source == 'computime':
            return _discover_computime_event(config, champ_name, event_date, event_name)
        elif source == 'tsl':
            return _discover_tsl_event(config, champ_name, event_date, event_name)
        elif source == 'motogp':
            return _discover_motogp_event(config, champ_name, event_date, event_name)
        elif source == 'moto4asia':
            return _discover_moto4asia_event(config, champ_name, event_date, event_name)
    except Exception as e:
        return {'found': False, 'reason': f'Error: {e}'}
    return {'found': False, 'reason': 'Unknown timing source'}


def _discover_speedhive_event(config, champ_name, event_date, event_name, settings):
    """Find a Speedhive event by org + date match."""
    try:
        from speedhive_client import SpeedhiveClient
    except ImportError:
        return {'found': False, 'reason': 'Speedhive client not available'}

    _sh = SpeedhiveClient()

    # Try org_id from config, then from linked orgs
    org_id = config.get('org_id')
    if not org_id:
        linked = st.session_state.get('sh_linked_orgs', {})
        org_data = linked.get(champ_name, {})
        org_id = org_data.get('org_id')

    if not org_id:
        return {'found': False, 'reason': f'No Speedhive org linked for {champ_name}. Link one via "Add Event Manually" → Speedhive → Paste URL.'}

    # Fetch events and match by date
    events = _sh.fetch_organization_events(org_id)
    if not events:
        return {'found': False, 'reason': f'No events found for org {org_id}'}

    # Match by date (event_date is start date like "2026-02-20")
    for ev in events:
        ev_start = ev.get('startDate', '')[:10]
        if ev_start == event_date:
            return {
                'found': True,
                'source': 'speedhive',
                'source_id': ev['id'],
                'event_title': ev.get('name', event_name),
            }

    # Also try fuzzy date match (within 2 days)
    from datetime import datetime as _dt, timedelta
    try:
        target = _dt.strptime(event_date, "%Y-%m-%d")
        for ev in events:
            ev_start = ev.get('startDate', '')[:10]
            try:
                ev_dt = _dt.strptime(ev_start, "%Y-%m-%d")
                if abs((ev_dt - target).days) <= 2:
                    return {
                        'found': True,
                        'source': 'speedhive',
                        'source_id': ev['id'],
                        'event_title': ev.get('name', event_name),
                    }
            except ValueError:
                continue
    except ValueError:
        pass

    return {'found': False, 'reason': f'No event on {event_date} found in Speedhive for {champ_name}'}


def _discover_computime_event(config, champ_name, event_date, event_name):
    """Find a Computime event by year + date match."""
    try:
        from computime_client import ComputimeClient
    except ImportError:
        return {'found': False, 'reason': 'Computime client not available'}

    _ct = ComputimeClient()
    year = int(event_date[:4])

    meetings = _ct.fetch_meetings(year)
    if not meetings:
        return {'found': False, 'reason': f'No Computime meetings found for {year}'}

    # Match by date
    for m in meetings:
        if m.get('date') == event_date:
            meet_id = m.get('meet_id')
            if not meet_id:
                # Discover via postback
                meet_id = _ct.discover_meet_id(year, m['row_index'])
            if meet_id:
                return {
                    'found': True,
                    'source': 'computime',
                    'source_id': meet_id,
                    'event_title': m.get('name', event_name),
                }
            else:
                return {'found': False, 'reason': f'Found meeting but could not discover MeetID'}

    # Fuzzy date match (within 2 days)
    from datetime import datetime as _dt, timedelta
    try:
        target = _dt.strptime(event_date, "%Y-%m-%d")
        for m in meetings:
            try:
                m_dt = _dt.strptime(m.get('date', ''), "%Y-%m-%d")
                if abs((m_dt - target).days) <= 2:
                    meet_id = m.get('meet_id')
                    if not meet_id:
                        meet_id = _ct.discover_meet_id(year, m['row_index'])
                    if meet_id:
                        return {
                            'found': True,
                            'source': 'computime',
                            'source_id': meet_id,
                            'event_title': m.get('name', event_name),
                        }
            except ValueError:
                continue
    except ValueError:
        pass

    return {'found': False, 'reason': f'No Computime meeting on {event_date}'}


def _discover_tsl_event(config, champ_name, event_date, event_name):
    """Find a TSL Timing event.

    TSL event IDs follow a pattern: YY + sequential number.
    We search the TSL site for the event matching the championship + date.
    """
    try:
        from tsl_timing_client import TSLTimingClient
    except ImportError:
        return {'found': False, 'reason': 'TSL Timing client not available'}

    # TSL doesn't have a browsable API — events need manual linking
    # For BSB, the event IDs are typically "26XXXX" for 2026
    # The user will need to link the first event, then we can try to find subsequent ones
    return {
        'found': False,
        'reason': f'TSL Timing requires manual event linking. Use "Add Event Manually" → TSL to paste the event URL from tsl-timing.com.'
    }


def _discover_motogp_event(config, champ_name, event_date, event_name):
    """Find a MotoGP event by date match using the free PulseLive API."""
    try:
        from motogp_client import MotoGPClient
    except ImportError:
        return {'found': False, 'reason': 'MotoGP client not available'}

    _mgp = MotoGPClient()
    year = int(event_date[:4])

    event = _mgp.find_event_by_date(event_date, year)
    if event:
        return {
            'found': True,
            'source': 'motogp',
            'source_id': event['short_name'] + f':{year}',
            'event_title': event.get('sponsored_name') or event.get('name', event_name),
        }

    return {'found': False, 'reason': f'No MotoGP event found near {event_date}'}


def _discover_moto4asia_event(config, champ_name, event_date, event_name):
    """Find a Moto4 Asia Cup event by date match."""
    try:
        from moto4asia_client import Moto4AsiaClient, EVENT_SLUGS_2026
    except ImportError:
        return {'found': False, 'reason': 'Moto4 Asia client not available'}

    # Match event_date to a round key via the championship calendar
    from championship_calendars import CHAMPIONSHIP_CALENDARS
    champ = next((c for c in CHAMPIONSHIP_CALENDARS if c['name'] == 'Moto4 Asia Cup'), None)
    if champ:
        for ev in champ['events']:
            if ev['start'] == event_date and ev['round'] in EVENT_SLUGS_2026:
                round_key = ev['round']
                year = int(event_date[:4])

                # Verify PDFs are available
                _client = Moto4AsiaClient()
                pdfs = _client.discover_pdfs(year, round_key)
                if pdfs:
                    return {
                        'found': True,
                        'source': 'moto4asia',
                        'source_id': f'{round_key}:{year}',
                        'event_title': f'Moto4 Asia Cup — {ev["name"]}',
                    }
                else:
                    return {'found': False, 'reason': f'No PDFs available yet for {round_key}'}

    return {'found': False, 'reason': f'No Moto4 Asia Cup event found for {event_date}'}


# ─── SAVED EVENTS FLOW ───────────────────────────────────────

def _saved_events_flow(settings, saved_events, persist_fn):
    """Flow for selecting from previously saved events."""
    raw_results_list = []
    rider_results = {}
    event_info = None

    _SRC_ICONS = {"speedhive": "🌐", "computime": "🕐", "tsl": "🇬🇧", "motogp": "🏍️", "moto4asia": "🏆"}

    if not saved_events:
        st.info("No saved events yet. Use '🏆 Championship Calendar' or '➕ Add Event Manually' to add one.")
        return [], {}, None

    # Championship filter
    _all_champs = sorted(set(
        ev.get('championship', '').strip()
        for ev in saved_events
        if ev.get('championship', '').strip()
    ))

    _champ_filter = None
    if _all_champs and len(_all_champs) > 1:
        _champ_col, _event_col = st.columns([1, 2])
        with _champ_col:
            _champ_opts = ["All Championships"] + _all_champs
            _champ_filter = st.selectbox(
                "🏆 Filter by Championship",
                options=_champ_opts,
                key="saved_champ_filter",
            )
            if _champ_filter == "All Championships":
                _champ_filter = None
    else:
        _event_col = st

    # Build event labels
    _event_labels = {}
    for i, ev in enumerate(saved_events):
        if _champ_filter and ev.get('championship', '').strip() != _champ_filter:
            continue
        icon = _SRC_ICONS.get(ev.get('source', ''), '📋')
        _event_labels[i] = f"{icon}  {ev.get('event_name', 'Unknown')}  —  {ev.get('circuit', '')}  ({ev.get('championship', '')})"

    if not _event_labels:
        st.info("No events match the selected championship filter.")
        return [], {}, None

    with (_event_col if _all_champs and len(_all_champs) > 1 else st.container()):
        _sel = st.selectbox(
            "🏁 Select Event",
            options=list(_event_labels.keys()),
            format_func=lambda x: _event_labels[x],
            key="saved_event_select",
        )

    if _sel is not None and isinstance(_sel, int) and _sel < len(saved_events):
        ev = saved_events[_sel]
        raw_results_list, rider_results, event_info = _load_saved_event(ev)

        # Auto-fill circuit and championship
        if ev.get('circuit'):
            st.session_state['_prefill_circuit'] = ev['circuit']
        if ev.get('championship'):
            st.session_state['global_championship'] = ev['championship']

        # Delete button
        with st.expander("🗑️ Remove this event", expanded=False):
            if st.button("Remove from saved events", key="remove_event_btn"):
                saved_events.pop(_sel)
                st.session_state['_saved_events'] = saved_events
                persist_fn()
                st.toast("🗑️ Event removed")
                st.rerun()

    return raw_results_list, rider_results, event_info


# ─── ADD EVENT HELPERS ────────────────────────────────────────

def _add_speedhive_event(settings, saved_events, persist_fn):
    """UI for adding a Speedhive event."""
    try:
        from speedhive_client import SpeedhiveClient, KNOWN_ORGS
    except ImportError as e:
        st.error(f"Speedhive client not available: {e}")
        return

    _sh = SpeedhiveClient()

    # Linked orgs
    if 'sh_linked_orgs' not in st.session_state:
        _stored = {}
        if settings and settings.is_available:
            _stored = settings.get('speedhive_orgs', {}) or {}
        _merged = dict(KNOWN_ORGS)
        _merged.update(_stored)
        st.session_state.sh_linked_orgs = _merged
    _orgs = st.session_state.sh_linked_orgs

    _mode_opts = list(_orgs.keys()) + ["📋 Paste URL / Link New"]
    _mode = st.selectbox("Championship", options=_mode_opts, key="add_sh_champ")

    _event_id = None

    if _mode == "📋 Paste URL / Link New":
        _url = st.text_input("Speedhive Event URL or ID",
                             placeholder="https://speedhive.mylaps.com/events/3347675",
                             key="add_sh_url")
        if _url:
            _event_id = _sh.extract_event_id(_url)
            if not _event_id:
                st.error("Could not extract event ID.")
            else:
                _dk = f"sh_disc_{_event_id}"
                if _dk not in st.session_state:
                    with st.spinner("Discovering championship..."):
                        st.session_state[_dk] = _sh.discover_org_from_event(_event_id)
                _disc = st.session_state[_dk]
                if _disc and not any(v.get('org_id') == _disc['org_id'] for v in _orgs.values()):
                    st.info(f"🔗 Found: **{_disc['name']}**")
                    _ln = st.text_input("Short name", value=_disc['name'].split()[0], key="add_sh_ln")
                    if st.button("🔗 Link", key="add_sh_link"):
                        _orgs[_ln] = {"org_id": _disc['org_id'], "name": _disc['name'], "sport": _disc.get('sport', 'Bike')}
                        st.session_state.sh_linked_orgs = _orgs
                        if settings and settings.is_available:
                            settings.set('speedhive_orgs', _orgs)
                        st.success(f"✅ {_ln} linked!")
                        st.rerun()
    else:
        _oi = _orgs.get(_mode, {})
        _oid = _oi.get('org_id')
        if _oid:
            _ek = f"sh_org_events_{_oid}"
            if _ek not in st.session_state:
                with st.spinner(f"Loading {_mode} events..."):
                    st.session_state[_ek] = _sh.fetch_organization_events(_oid)
            _evts = st.session_state[_ek]
            if _evts:
                _recent = _evts[:15]
                _opts = {e['id']: f"{e['startDate']}  —  {e['name']}" for e in _recent}
                _sid = st.selectbox("📅 Event", options=list(_opts.keys()),
                                    format_func=lambda x: _opts[x], key="add_sh_ev")
                if _sid:
                    _event_id = _sid

    if _event_id:
        _ck = f"sh_sessions_{_event_id}"
        if _ck not in st.session_state:
            with st.spinner("Fetching event details..."):
                _ev = _sh.fetch_event(_event_id)
                _ss = _sh.fetch_sessions(_event_id)
                st.session_state[_ck] = (_ev, _ss)
        _ev, _ss = st.session_state[_ck]

        if _ev:
            _loc = _ev.get('location', {}).get('name', '')
            _en = _ev.get('name', '')
            _ed = _ev.get('startDate', '')
            _nr = len([s for s in _ss if s['type'] == 'race'])
            st.success(f"**{_en}** — {_loc} ({_nr} races)")

            _champ = st.text_input("Championship", value=_mode if _mode != "📋 Paste URL / Link New" else "", key="add_sh_cname")
            _circ = st.text_input("Circuit", value=_loc, key="add_sh_circ")

            if st.button("💾 Save Event", key="add_sh_save", type="primary"):
                saved_events.insert(0, {
                    "source": "speedhive", "source_id": str(_event_id),
                    "event_name": _en, "circuit": _circ,
                    "championship": _champ, "date": _ed,
                })
                st.session_state['_saved_events'] = saved_events
                persist_fn()
                st.toast(f"✅ Saved: {_en}")
                st.session_state['event_source_mode'] = "📋 Saved Events"
                st.session_state['saved_event_select'] = 0
                st.rerun()


def _add_computime_event(saved_events, persist_fn):
    """UI for adding a Computime event — browse by year or paste URL."""
    try:
        from computime_client import ComputimeClient
    except ImportError as e:
        st.error(f"Computime client not available: {e}")
        return

    _ct = ComputimeClient()

    _ct_mode = st.radio("Find event", ["📅 Browse by Year", "📋 Paste URL"], key="add_ct_mode", horizontal=True)

    _mid = None

    if _ct_mode == "📅 Browse by Year":
        _year = st.selectbox("Year", options=[2026, 2025, 2024], key="add_ct_year")

        _mk = f"ct_meetings_{_year}"
        if _mk not in st.session_state:
            with st.spinner(f"Loading {_year} meetings from Computime..."):
                st.session_state[_mk] = _ct.fetch_meetings(_year)
        _meetings = st.session_state[_mk]

        if not _meetings:
            st.warning(f"No meetings found for {_year}")
            return

        _opts = {i: f"{m['date']}  —  {m['name']}  ({m['location']})" for i, m in enumerate(_meetings)}
        _sel_idx = st.selectbox("📅 Event", options=list(_opts.keys()),
                                format_func=lambda x: _opts[x], key="add_ct_event_sel")

        if _sel_idx is not None:
            _sel_meeting = _meetings[_sel_idx]
            _mid = _sel_meeting.get('meet_id')

            # If no known MeetID, discover it via postback
            if not _mid:
                _disc_key = f"ct_discover_{_year}_{_sel_meeting['row_index']}"
                if _disc_key not in st.session_state:
                    with st.spinner("Discovering MeetID..."):
                        _discovered = _ct.discover_meet_id(_year, _sel_meeting['row_index'])
                        st.session_state[_disc_key] = _discovered
                _mid = st.session_state[_disc_key]

                if _mid:
                    st.caption(f"✅ Discovered MeetID: {_mid}")
                else:
                    st.error("Could not discover MeetID. Try pasting the URL instead.")
                    return

    else:  # Paste URL
        _url = st.text_input("Computime Results URL or MeetID",
                             placeholder="https://www.computime.com.au/.../Resultspage?MeetID=17437",
                             key="add_ct_url")
        _mid = ComputimeClient.extract_meet_id(_url) if _url else None

    if not _mid:
        return

    # Verify sessions exist
    _sk = f"ct_sessions_{_mid}"
    if _sk not in st.session_state:
        with st.spinner("Fetching sessions from Computime..."):
            try:
                st.session_state[_sk] = _ct.get_sessions(_mid)
            except Exception as e:
                st.error(f"Could not reach Computime: {e}")
                st.session_state[_sk] = []
    _sessions = st.session_state[_sk]
    if not _sessions:
        st.warning("No sessions found for this meeting.")
        return

    _nr = len([s for s in _sessions if s['type'] == 'race'])
    _nq = len([s for s in _sessions if s['type'] == 'qualify'])
    _en = _sessions[0].get('group', f'Computime Meet {_mid}')
    st.success(f"**{_nr} races, {_nq} qualifying sessions** found")

    # Pre-fill from browsed event data
    _default_name = ""
    _default_circuit = ""
    if _ct_mode == "📅 Browse by Year" and _sel_idx is not None:
        _m = _meetings[_sel_idx]
        _default_name = _m.get('name', '')
        _default_circuit = _m.get('location', '')

    _champ = st.text_input("Championship", key="add_ct_champ")
    _circ = st.text_input("Circuit", value=_default_circuit, key="add_ct_circ")
    _name = st.text_input("Event Name", value=_default_name or _en, key="add_ct_name")

    if st.button("💾 Save Event", key="add_ct_save", type="primary"):
        saved_events.insert(0, {
            "source": "computime", "source_id": str(_mid),
            "event_name": _name, "circuit": _circ,
            "championship": _champ, "date": datetime.now().strftime('%Y-%m-%d'),
        })
        st.session_state['_saved_events'] = saved_events
        persist_fn()
        st.toast(f"✅ Saved: {_name}")
        st.session_state['event_source_mode'] = "📋 Saved Events"
        st.session_state['saved_event_select'] = 0
        st.rerun()


def _add_tsl_event(saved_events, persist_fn):
    """UI for adding a TSL Timing event."""
    try:
        from tsl_timing_client import TSLTimingClient
    except ImportError as e:
        st.error(f"TSL Timing client not available: {e}")
        return

    _url = st.text_input("TSL Timing Event URL or ID",
                         placeholder="https://www.tsl-timing.com/event/251804",
                         key="add_tsl_url")
    _eid = TSLTimingClient.extract_event_id(_url) if _url else None
    if not _eid:
        return

    _tsl = TSLTimingClient()
    _sk = f"tsl_sessions_{_eid}"
    if _sk not in st.session_state:
        with st.spinner("Fetching sessions from TSL Timing..."):
            try:
                _title, _sessions = _tsl.get_sessions(_eid)
                st.session_state[_sk] = (_title, _sessions)
            except Exception as e:
                st.error(f"Could not reach TSL Timing: {e}")
                st.session_state[_sk] = ('', [])
    _title, _sessions = st.session_state[_sk]
    if not _sessions:
        return

    _nr = len([s for s in _sessions if s['type'] == 'race'])
    st.success(f"**{_title}** ({_nr} races)")

    _champ = st.text_input("Championship", key="add_tsl_champ")
    _circ = st.text_input("Circuit", key="add_tsl_circ")

    if st.button("💾 Save Event", key="add_tsl_save", type="primary"):
        saved_events.insert(0, {
            "source": "tsl", "source_id": str(_eid),
            "event_name": _title or f"TSL {_eid}",
            "circuit": _circ, "championship": _champ,
            "date": datetime.now().strftime('%Y-%m-%d'),
        })
        st.session_state['_saved_events'] = saved_events
        persist_fn()
        st.toast(f"✅ Saved: {_title}")
        st.session_state['event_source_mode'] = "📋 Saved Events"
        st.session_state['saved_event_select'] = 0
        st.rerun()


def _add_motogp_event(saved_events, persist_fn):
    """UI for adding a MotoGP event — browse by season."""
    try:
        from motogp_client import MotoGPClient
    except ImportError as e:
        st.error(f"MotoGP client not available: {e}")
        return

    _mgp = MotoGPClient()
    _year = st.selectbox("Season Year", options=[2026, 2025, 2024], key="add_mgp_year")

    _ek = f"mgp_events_{_year}"
    if _ek not in st.session_state:
        with st.spinner(f"Loading {_year} MotoGP events..."):
            st.session_state[_ek] = _mgp.list_events(_year, races_only=True)
    _events = st.session_state[_ek]

    if not _events:
        st.warning(f"No events found for {_year}")
        return

    _opts = {i: f"{e['date_start']}  —  {e['name']}  ({e['location']}, {e['country']})" for i, e in enumerate(_events)}
    _sel_idx = st.selectbox("📅 Grand Prix", options=list(_opts.keys()),
                            format_func=lambda x: _opts[x], key="add_mgp_event_sel")

    if _sel_idx is not None:
        _sel = _events[_sel_idx]

        # Show categories
        _ck = f"mgp_cats_{_sel['id']}"
        if _ck not in st.session_state:
            with st.spinner("Loading categories..."):
                event = _mgp.find_event_by_short_name(_sel['short_name'], _year)
                cats = _mgp.fetch_categories(event['id']) if event else []
                st.session_state[_ck] = cats
        cats = st.session_state[_ck]
        cat_names = [c['name'] for c in cats]
        st.success(f"**{_sel['name']}** — {_sel['circuit']} ({', '.join(cat_names)})")

        _src_id = f"{_sel['short_name']}:{_year}"
        if st.button("💾 Save Event", key="add_mgp_save", type="primary"):
            saved_events.insert(0, {
                "source": "motogp", "source_id": _src_id,
                "event_name": _sel['name'], "circuit": _sel['circuit'],
                "championship": "MotoGP", "date": _sel['date_start'],
            })
            st.session_state['_saved_events'] = saved_events
            persist_fn()
            st.toast(f"✅ Saved: {_sel['name']}")
            st.session_state['event_source_mode'] = "📋 Saved Events"
            st.session_state['saved_event_select'] = 0
            st.rerun()


def _add_moto4asia_event(saved_events, persist_fn):
    """UI for adding a Moto4 Asia Cup event."""
    try:
        from moto4asia_client import Moto4AsiaClient, EVENT_SLUGS_2026
    except ImportError as e:
        st.error(f"Moto4 Asia client not available: {e}")
        return

    _client = Moto4AsiaClient()

    # Show available rounds
    from championship_calendars import CHAMPIONSHIP_CALENDARS
    champ = next((c for c in CHAMPIONSHIP_CALENDARS if c['name'] == 'Moto4 Asia Cup'), None)
    if not champ:
        st.error("Moto4 Asia Cup not found in championship calendars")
        return

    race_events = [e for e in champ['events'] if e['round'] in EVENT_SLUGS_2026]
    _opts = {i: f"{e['round']}  —  {e['name']}  ({e['start']})" for i, e in enumerate(race_events)}
    _sel_idx = st.selectbox("📅 Round", options=list(_opts.keys()),
                            format_func=lambda x: _opts[x], key="add_m4a_round_sel")

    if _sel_idx is not None:
        _sel = race_events[_sel_idx]
        round_key = _sel['round']
        year = int(_sel['start'][:4])

        # Check for available PDFs
        _pk = f"m4a_pdfs_{round_key}_{year}"
        if _pk not in st.session_state:
            with st.spinner("Checking for results PDFs..."):
                st.session_state[_pk] = _client.discover_pdfs(year, round_key)
        pdfs = st.session_state[_pk]

        if pdfs:
            st.success(f"**{len(pdfs)} result PDFs** found for {_sel['name']}")
            for p in pdfs:
                st.caption(f"  📄 {p['session_name']}: {p['filename']}")
        else:
            st.warning("No result PDFs available yet for this round.")

        _src_id = f"{round_key}:{year}"
        if st.button("💾 Save Event", key="add_m4a_save", type="primary"):
            saved_events.insert(0, {
                "source": "moto4asia", "source_id": _src_id,
                "event_name": f"Moto4 Asia Cup — {_sel['name']}",
                "circuit": _sel['name'],
                "championship": "Moto4 Asia Cup",
                "date": _sel['start'],
            })
            st.session_state['_saved_events'] = saved_events
            persist_fn()
            st.toast(f"✅ Saved: Moto4 Asia Cup {_sel['name']}")
            st.session_state['event_source_mode'] = "📋 Saved Events"
            st.session_state['saved_event_select'] = 0
            st.rerun()


# ─── LOAD SAVED EVENT ─────────────────────────────────────────

def _load_saved_event(ev):
    """Load results from a previously saved event.

    Returns (raw_results_list, rider_results_map, event_info)
    """
    _src = ev.get('source', '')
    _src_id = ev.get('source_id', '')

    st.info(f"📡 Loading **{ev.get('event_name', '')}** from **{_src.title()}**...")

    if _src == 'speedhive':
        return _load_speedhive_event(_src_id)
    elif _src == 'computime':
        return _load_computime_event(_src_id)
    elif _src == 'tsl':
        return _load_tsl_event(_src_id)
    elif _src == 'motogp':
        return _load_motogp_event(_src_id)
    elif _src == 'moto4asia':
        return _load_moto4asia_event(_src_id)

    return [], {}, None


def _load_speedhive_event(event_id_str):
    """Load Speedhive event results."""
    try:
        from speedhive_client import SpeedhiveClient
    except ImportError as e:
        st.error(f"Speedhive client not available: {e}")
        return [], {}, None

    _sh = SpeedhiveClient()
    _eid = int(event_id_str)

    _ck = f"sh_sessions_{_eid}"
    if _ck not in st.session_state:
        with st.spinner("Fetching sessions from Speedhive..."):
            _ev = _sh.fetch_event(_eid)
            _ss = _sh.fetch_sessions(_eid)
            st.session_state[_ck] = (_ev, _ss)
    _ev, _ss = st.session_state[_ck]

    if not _ss:
        st.warning("No sessions found for this event.")
        return [], {}, _ev

    _races = [s for s in _ss if s['type'] == 'race']
    _quals = [s for s in _ss if s['type'] == 'qualify']
    st.success(f"**{len(_races)} races, {len(_quals)} qualifying sessions** found")

    # Auto-select ALL sessions by default (races + qualifying)
    _all_session_ids = [s['id'] for s in _races] + [s['id'] for s in _quals]

    # Optional fine-tuning inside expander
    with st.expander("⚙️ Fine-tune session selection", expanded=False):
        _selected = []
        if _races:
            _sel = st.multiselect("🏁 Races", options=[s['id'] for s in _races],
                                  format_func=lambda x: next((s['group'] for s in _races if s['id'] == x), str(x)),
                                  default=[s['id'] for s in _races], key="ev_race_sel")
            _selected.extend(_sel)
        if _quals:
            _sel_q = st.multiselect("⏱️ Qualifying", options=[s['id'] for s in _quals],
                                    format_func=lambda x: next((s['group'] for s in _quals if s['id'] == x), str(x)),
                                    default=[s['id'] for s in _quals], key="ev_qual_sel")
            _selected.extend(_sel_q)

    # Use fine-tuned selection if expander was used, otherwise all sessions
    if not _selected:
        _selected = _all_session_ids

    if not _selected:
        return [], {}, _ev

    _rk = f"sh_results_{_eid}_{'_'.join(map(str, sorted(_selected)))}"
    if _rk not in st.session_state:
        with st.spinner(f"Fetching results for {len(_selected)} sessions..."):
            _names = set()
            _map = {}
            for sid in _selected:
                _cls = _sh.fetch_session_results(sid)
                if _cls and _cls.get('rows'):
                    _si = next((s for s in _ss if s['id'] == sid), {})
                    for row in _cls['rows']:
                        _n = row.get('name', '').strip()
                        if not _n or _n.startswith('-'):
                            continue
                        _names.add(_n)
                        if _n not in _map:
                            _map[_n] = []
                        _map[_n].append({
                            'session_name': _si.get('name', ''),
                            'session_type': _si.get('type', ''),
                            'session_group': _si.get('group', ''),
                            'position': row.get('position'),
                            'position_in_class': row.get('positionInClass'),
                            'best_lap': row.get('bestTime', ''),
                            'total_time': row.get('totalTime', ''),
                            'laps': row.get('numberOfLaps', 0),
                            'best_speed': row.get('bestSpeed', 0),
                            'result_class': row.get('resultClass', ''),
                            'status': row.get('status', 'Normal'),
                            'start_number': row.get('startNumber', ''),
                        })
            st.session_state[_rk] = (sorted(_names), _map)

    _names, _map = st.session_state[_rk]
    st.session_state['uploaded_timing_names'] = _names
    st.success(f"✅ {len(_names)} riders loaded from Speedhive!")

    # Class filter
    _names, _map = _apply_class_filter(_names, _map, "ev")

    return _names, _map, _ev


def _load_computime_event(meet_id):
    """Load Computime event results."""
    try:
        from computime_client import ComputimeClient
    except ImportError as e:
        st.error(f"Computime client not available: {e}")
        return [], {}, None

    _ct = ComputimeClient()

    _sk = f"ct_sessions_{meet_id}"
    if _sk not in st.session_state:
        with st.spinner("Fetching sessions from Computime..."):
            try:
                st.session_state[_sk] = _ct.get_sessions(meet_id)
            except Exception as e:
                st.error(f"Could not reach Computime: {e}")
                st.session_state[_sk] = []
    _sessions = st.session_state[_sk]

    if not _sessions:
        st.warning("No sessions found.")
        return [], {}, None

    _races = [s for s in _sessions if s['type'] == 'race']
    _quals = [s for s in _sessions if s['type'] == 'qualify']
    st.success(f"**{len(_races)} races, {len(_quals)} qualifying sessions** found")

    # Auto-select ALL sessions by default (races + qualifying)
    _all_session_ids = [s['id'] for s in _races] + [s['id'] for s in _quals]

    # Optional fine-tuning inside expander
    with st.expander("⚙️ Fine-tune session selection", expanded=False):
        _selected = []
        if _races:
            _sel = st.multiselect("🏁 Races", options=[s['id'] for s in _races],
                                  format_func=lambda x: next((s['name'] for s in _races if s['id'] == x), str(x)),
                                  default=[s['id'] for s in _races], key="ev_ct_race_sel")
            _selected.extend(_sel)
        if _quals:
            _sel_q = st.multiselect("⏱️ Qualifying", options=[s['id'] for s in _quals],
                                    format_func=lambda x: next((s['name'] for s in _quals if s['id'] == x), str(x)),
                                    default=[s['id'] for s in _quals], key="ev_ct_qual_sel")
            _selected.extend(_sel_q)

    # Use fine-tuned selection if expander was used, otherwise all sessions
    if not _selected:
        _selected = _all_session_ids

    if not _selected:
        return [], {}, None

    _rk = f"ct_results_{meet_id}_{'_'.join(sorted(_selected))}"
    if _rk not in st.session_state:
        with st.spinner(f"Downloading & parsing {len(_selected)} timing sheets..."):
            _ev, _names, _map = _ct.extract_rider_results(meet_id, selected_sessions=_selected)
            st.session_state[_rk] = (_ev, _names, _map)

    _ev, _names, _map = st.session_state[_rk]
    st.session_state['uploaded_timing_names'] = _names
    st.success(f"✅ {len(_names)} riders loaded from Computime!")

    _names, _map = _apply_class_filter(_names, _map, "ev_ct")

    return _names, _map, _ev


def _load_tsl_event(event_id):
    """Load TSL Timing event results."""
    try:
        from tsl_timing_client import TSLTimingClient
    except ImportError as e:
        st.error(f"TSL Timing client not available: {e}")
        return [], {}, None

    _tsl = TSLTimingClient()

    _sk = f"tsl_sessions_{event_id}"
    if _sk not in st.session_state:
        with st.spinner("Fetching sessions from TSL Timing..."):
            try:
                _title, _sessions = _tsl.get_sessions(event_id)
                st.session_state[_sk] = (_title, _sessions)
            except Exception as e:
                st.error(f"Could not reach TSL Timing: {e}")
                st.session_state[_sk] = ('', [])
    _title, _sessions = st.session_state[_sk]

    if not _sessions:
        st.warning("No sessions found.")
        return [], {}, None

    _races = [s for s in _sessions if s['type'] == 'race']
    st.success(f"**{len(_races)} race sessions** found")

    # Auto-select ALL race sessions by default
    _all_session_ids = [s['id'] for s in _races]

    # Optional fine-tuning inside expander
    with st.expander("⚙️ Fine-tune session selection", expanded=False):
        _selected = []
        if _races:
            _sel = st.multiselect("🏁 Races", options=[s['id'] for s in _races],
                                  format_func=lambda x: next(
                                      (f"{s['class_name'][:30]} — {s['name']}" for s in _races if s['id'] == x), str(x)),
                                  default=[s['id'] for s in _races], key="ev_tsl_race_sel")
            _selected.extend(_sel)

    # Use fine-tuned selection if expander was used, otherwise all sessions
    if not _selected:
        _selected = _all_session_ids

    if not _selected:
        return [], {}, None

    _rk = f"tsl_results_{event_id}_{'_'.join(sorted(_selected))}"
    if _rk not in st.session_state:
        with st.spinner(f"Downloading & parsing {len(_selected)} timing sheets..."):
            _ev, _names, _map = _tsl.extract_rider_results(event_id, selected_sessions=_selected)
            st.session_state[_rk] = (_ev, _names, _map)

    _ev, _names, _map = st.session_state[_rk]
    st.session_state['uploaded_timing_names'] = _names
    st.success(f"✅ {len(_names)} riders loaded from TSL Timing!")

    _names, _map = _apply_class_filter(_names, _map, "ev_tsl")

    return _names, _map, _ev


def _load_motogp_event(source_id_str):
    """Load MotoGP event results.

    source_id_str format: 'THA:2026' (short_name:year)
    """
    try:
        from motogp_client import MotoGPClient
    except ImportError as e:
        st.error(f"MotoGP client not available: {e}")
        return [], {}, None

    _mgp = MotoGPClient()

    # Parse source_id
    parts = source_id_str.split(':')
    short_name = parts[0]
    year = int(parts[1]) if len(parts) > 1 else 2026

    _ck = f"mgp_sessions_{source_id_str}"
    if _ck not in st.session_state:
        with st.spinner("Fetching MotoGP sessions..."):
            event = _mgp.find_event_by_short_name(short_name, year)
            if event:
                sessions = _mgp.get_sessions(event['id'])
                st.session_state[_ck] = (event, sessions)
            else:
                st.session_state[_ck] = (None, [])
    _event, _sessions = st.session_state[_ck]

    if not _sessions:
        st.warning("No sessions found for this event.")
        return [], {}, None

    # Group by category
    _cat_names = sorted(set(s['category_name'] for s in _sessions))

    # Category filter
    selected_cats = st.multiselect(
        "🏍️ Categories",
        options=_cat_names,
        default=_cat_names,
        key=f"mgp_cat_filter_{source_id_str}"
    )

    _filtered = [s for s in _sessions if s['category_name'] in selected_cats]
    _races = [s for s in _filtered if s['type'] == 'race' and s['status'] == 'FINISHED']
    _quals = [s for s in _filtered if s['type'] == 'qualify' and s['status'] == 'FINISHED']
    _finished = [s for s in _filtered if s['status'] == 'FINISHED']
    st.success(f"**{len(_races)} races, {len(_quals)} qualifying** finished ({len(_finished)} total)")

    # Session selection
    with st.expander("⚙️ Fine-tune session selection", expanded=False):
        _selected = []
        if _races:
            _sel = st.multiselect("🏁 Races", options=[s['id'] for s in _races],
                                  format_func=lambda x: next(
                                      (f"{s['category_name']} — {s['name']}" for s in _races if s['id'] == x), str(x)),
                                  default=[s['id'] for s in _races], key="mgp_race_sel")
            _selected.extend(_sel)
        if _quals:
            _sel_q = st.multiselect("⏱️ Qualifying", options=[s['id'] for s in _quals],
                                    format_func=lambda x: next(
                                        (f"{s['category_name']} — {s['name']}" for s in _quals if s['id'] == x), str(x)),
                                    default=[s['id'] for s in _quals], key="mgp_qual_sel")
            _selected.extend(_sel_q)

    # Default: all finished race + qualifying sessions
    if not _selected:
        _selected = [s['id'] for s in _races] + [s['id'] for s in _quals]

    if not _selected:
        # Fallback: all finished sessions
        _selected = [s['id'] for s in _finished]

    if not _selected:
        return [], {}, _event

    _rk = f"mgp_results_{source_id_str}_{'_'.join(sorted(_selected))}"
    if _rk not in st.session_state:
        with st.spinner(f"Fetching results for {len(_selected)} sessions..."):
            _ev_info, _names, _map = _mgp.extract_rider_results(
                short_name, year=year,
                selected_sessions=_selected,
                categories=selected_cats if selected_cats != _cat_names else None,
            )
            st.session_state[_rk] = (_names, _map, _ev_info)

    _names, _map, _ev_info = st.session_state[_rk]
    st.session_state['uploaded_timing_names'] = _names
    st.success(f"✅ {len(_names)} riders loaded from MotoGP!")

    # Class filter
    _names, _map = _apply_class_filter(_names, _map, "mgp")

    return _names, _map, _ev_info


def _load_moto4asia_event(source_id_str):
    """Load Moto4 Asia Cup event results from free PDF timing sheets.

    source_id_str format: 'R1:2026' (round_key:year)
    """
    try:
        from moto4asia_client import Moto4AsiaClient
    except ImportError as e:
        st.error(f"Moto4 Asia client not available: {e}")
        return [], {}, None

    _client = Moto4AsiaClient()

    # Parse source_id
    parts = source_id_str.split(':')
    round_key = parts[0]
    year = int(parts[1]) if len(parts) > 1 else 2026

    _ck = f"m4a_pdfs_{source_id_str}"
    if _ck not in st.session_state:
        with st.spinner("Discovering Moto4 Asia Cup PDFs..."):
            st.session_state[_ck] = _client.discover_pdfs(year, round_key)
    pdfs = st.session_state[_ck]

    if not pdfs:
        st.warning("No result PDFs found for this round.")
        return [], {}, None

    _races = [p for p in pdfs if p['session_type'] == 'race']
    _quals = [p for p in pdfs if p['session_type'] == 'qualify']
    _pracs = [p for p in pdfs if p['session_type'] == 'practice']
    st.success(f"**{len(_races)} races, {len(_quals)} qualifying, {len(_pracs)} practice** PDFs found")

    # Session type filter
    _types = st.multiselect(
        "📄 Session types",
        options=['race', 'qualify', 'practice'],
        default=['race', 'qualify'] if _races else ['practice'],
        key=f"m4a_types_{source_id_str}"
    )

    _rk = f"m4a_results_{source_id_str}_{'_'.join(sorted(_types))}"
    if _rk not in st.session_state:
        with st.spinner("Parsing PDFs..."):
            _ev_info, _names, _map = _client.extract_rider_results(
                round_key=round_key, year=year, session_types=_types
            )
            st.session_state[_rk] = (_names, _map, _ev_info)

    _names, _map, _ev_info = st.session_state[_rk]
    st.session_state['uploaded_timing_names'] = _names
    st.success(f"✅ {len(_names)} riders loaded from Moto4 Asia Cup!")

    return _names, _map, _ev_info


# ─── CLASS FILTER (shared) ────────────────────────────────────

def _apply_class_filter(names, rider_map, prefix):
    """Add a class filter dropdown if multiple classes exist.

    Returns potentially filtered (names, rider_map).
    """
    _class_counts = {}
    for _rname, _rdata in rider_map.items():
        _classes = set()
        for _sd in _rdata:
            _rc = _sd.get('result_class', '').strip()
            _sg = _sd.get('session_group', '').strip()
            _cls = _rc or _sg or 'Unknown'
            _classes.add(_cls)
        for _c in _classes:
            _class_counts[_c] = _class_counts.get(_c, 0) + 1

    if not _class_counts or len(_class_counts) < 2:
        return names, rider_map

    _parts = [f"**{c}** ({n})" for c, n in sorted(_class_counts.items(), key=lambda x: -x[1])]
    st.caption("📋 Categories: " + " · ".join(_parts))

    _opts = ["All Classes"] + sorted(_class_counts.keys(), key=lambda x: _class_counts[x], reverse=True)
    _sel = st.selectbox("🏷️ Filter by class", options=_opts, key=f"{prefix}_class_filter")

    if _sel and _sel != "All Classes":
        _filtered = [n for n in names if any(
            (_sd.get('result_class', '').strip() or _sd.get('session_group', '').strip() or 'Unknown') == _sel
            for _sd in rider_map.get(n, [])
        )]
        st.session_state['uploaded_timing_names'] = _filtered
        st.info(f"🏷️ Showing **{len(_filtered)}** riders in **{_sel}**")
        return _filtered, rider_map

    return names, rider_map
