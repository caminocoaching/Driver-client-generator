
import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from urllib.parse import unquote_plus
from funnel_manager import FunnelDashboard, FunnelStage, Driver
from strategy_call import (
    APPLICATION_QUESTIONS, generate_script_overlay, analyze_call_transcript,
    format_analysis_report, CALL_1_FRAMEWORK, CALL_2_FRAMEWORK, swap_terminology,
    analyze_candidate_data, GOLD_STANDARD
)
try:
    from airtable_manager import AirtableSettingsStore
except ImportError:
    AirtableSettingsStore = None  # Fallback if module cache is stale

# --- CONFIGURATION ---
st.set_page_config(page_title="Driver Pipeline", page_icon="🏎️", layout="wide")


# --- VERSION BADGE ---
st.sidebar.caption("✅ v2.11.0 (Live)")
if st.sidebar.button("🔄 Refresh App", key="top_refresh"):
    st.cache_resource.clear()
    st.cache_data.clear()
    st.rerun()




# Directory setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../Client Generator "))

if not os.path.exists(DATA_DIR):
    # Fallback to current dir if sibling not found
    DATA_DIR = BASE_DIR

# =====================================================================
# 2026 RACE CALENDARS (Shared: Calendar view + Race Outreach)
# =====================================================================
RACE_CALENDARS = {
    "BTCC": {
        "color": "#1565C0",  # Blue
        "rounds": [
            {"round": "R1", "name": "Donington Park (National)", "start": "2026-04-18", "end": "2026-04-20"},
            {"round": "R2", "name": "Brands Hatch (Indy)", "start": "2026-05-09", "end": "2026-05-11"},
            {"round": "R3", "name": "Snetterton (300)", "start": "2026-05-24", "end": "2026-05-26"},
            {"round": "R4", "name": "Oulton Park", "start": "2026-06-06", "end": "2026-06-08"},
            {"round": "R5", "name": "Thruxton", "start": "2026-07-25", "end": "2026-07-27"},
            {"round": "R6", "name": "Knockhill", "start": "2026-08-08", "end": "2026-08-10"},
            {"round": "R7", "name": "Donington Park (GP)", "start": "2026-08-22", "end": "2026-08-24"},
            {"round": "R8", "name": "Croft", "start": "2026-09-05", "end": "2026-09-07"},
            {"round": "R9", "name": "Silverstone", "start": "2026-09-26", "end": "2026-09-28"},
            {"round": "R10", "name": "Brands Hatch (GP)", "start": "2026-10-10", "end": "2026-10-12"},
        ]
    },
    "British F4": {
        "color": "#E65100",  # Deep Orange
        "rounds": [
            {"round": "R1", "name": "Donington Park (National)", "start": "2026-04-18", "end": "2026-04-20"},
            {"round": "R2", "name": "Brands Hatch (Indy)", "start": "2026-05-09", "end": "2026-05-11"},
            {"round": "R3", "name": "Snetterton (300)", "start": "2026-05-23", "end": "2026-05-25"},
            {"round": "R4", "name": "Silverstone (GP)", "start": "2026-05-30", "end": "2026-06-01"},
            {"round": "R5", "name": "Zandvoort 🇳🇱", "start": "2026-07-11", "end": "2026-07-13"},
            {"round": "R6", "name": "Thruxton", "start": "2026-07-25", "end": "2026-07-27"},
            {"round": "R7", "name": "Donington Park (GP)", "start": "2026-08-15", "end": "2026-08-17"},
            {"round": "R8", "name": "Croft", "start": "2026-08-29", "end": "2026-08-31"},
            {"round": "R9", "name": "Silverstone (National)", "start": "2026-09-26", "end": "2026-09-28"},
            {"round": "R10", "name": "Brands Hatch (GP)", "start": "2026-10-10", "end": "2026-10-12"},
        ]
    },
    "GB3": {
        "color": "#6A1B9A",  # Purple
        "rounds": [
            {"round": "R1", "name": "Silverstone (GP) 🇬🇧", "start": "2026-04-25", "end": "2026-04-27"},
            {"round": "R2", "name": "Spa-Francorchamps 🇧🇪", "start": "2026-05-30", "end": "2026-06-01"},
            {"round": "R3", "name": "Hungaroring 🇭🇺", "start": "2026-07-04", "end": "2026-07-06"},
            {"round": "R4", "name": "Red Bull Ring 🇦🇹", "start": "2026-07-11", "end": "2026-07-13"},
            {"round": "R5", "name": "Silverstone (GP) 🇬🇧", "start": "2026-08-01", "end": "2026-08-03"},
            {"round": "R6", "name": "Donington Park 🇬🇧", "start": "2026-09-05", "end": "2026-09-07"},
            {"round": "R7", "name": "Brands Hatch (GP) 🇬🇧", "start": "2026-09-26", "end": "2026-09-28"},
            {"round": "R8", "name": "Barcelona 🇪🇸", "start": "2026-11-07", "end": "2026-11-09"},
        ]
    },
    "British GT": {
        "color": "#00838F",  # Teal
        "rounds": [
            {"round": "R1", "name": "Silverstone 500 🇬🇧", "start": "2026-04-25", "end": "2026-04-27"},
            {"round": "R2", "name": "Oulton Park 🇬🇧", "start": "2026-05-23", "end": "2026-05-25"},
            {"round": "R3", "name": "Spa-Francorchamps 🇧🇪", "start": "2026-06-20", "end": "2026-06-22"},
            {"round": "R4", "name": "Snetterton 300 🇬🇧", "start": "2026-08-15", "end": "2026-08-17"},
            {"round": "R5", "name": "Donington Park 🇬🇧", "start": "2026-09-05", "end": "2026-09-07"},
            {"round": "R6", "name": "Brands Hatch (GP) 🇬🇧", "start": "2026-09-26", "end": "2026-09-28"},
        ]
    },
    "UAE F4": {
        "color": "#F9A825",  # Amber/Gold
        "rounds": [
            {"round": "R1", "name": "Yas Marina 🇦🇪", "start": "2026-01-16", "end": "2026-01-19"},
            {"round": "R2", "name": "Yas Marina 🇦🇪", "start": "2026-01-23", "end": "2026-01-26"},
            {"round": "R3", "name": "Dubai Autodrome 🇦🇪", "start": "2026-01-30", "end": "2026-02-02"},
            {"round": "R4", "name": "Lusail 🇶🇦", "start": "2026-02-11", "end": "2026-02-14"},
        ]
    },
    "Porsche Cup GB": {
        "color": "#C62828",  # Red
        "rounds": [
            {"round": "R1", "name": "Donington Park (National)", "start": "2026-04-16", "end": "2026-04-20"},
            {"round": "R2", "name": "Brands Hatch (Indy)", "start": "2026-05-08", "end": "2026-05-11"},
            {"round": "R3", "name": "Snetterton", "start": "2026-05-22", "end": "2026-05-25"},
            {"round": "R4", "name": "Thruxton", "start": "2026-07-24", "end": "2026-07-27"},
            {"round": "R5", "name": "Donington Park (GP)", "start": "2026-08-20", "end": "2026-08-24"},
            {"round": "R6", "name": "Croft", "start": "2026-09-04", "end": "2026-09-07"},
            {"round": "R7", "name": "Silverstone", "start": "2026-09-25", "end": "2026-09-28"},
            {"round": "R8", "name": "Brands Hatch (GP)", "start": "2026-10-09", "end": "2026-10-12"},
        ]
    },
    "Porsche Cup NA": {
        "color": "#AD1457",  # Pink
        "rounds": [
            {"round": "R1", "name": "Sebring 🇺🇸", "start": "2026-03-18", "end": "2026-03-21"},
            {"round": "R2", "name": "Long Beach 🇺🇸", "start": "2026-04-17", "end": "2026-04-20"},
            {"round": "R3", "name": "Miami 🇺🇸", "start": "2026-05-01", "end": "2026-05-04"},
            {"round": "R4", "name": "Watkins Glen 🇺🇸", "start": "2026-06-25", "end": "2026-06-28"},
            {"round": "R5", "name": "Road America 🇺🇸", "start": "2026-07-30", "end": "2026-08-02"},
            {"round": "R6", "name": "Indianapolis 🇺🇸", "start": "2026-09-17", "end": "2026-09-20"},
            {"round": "R7", "name": "Road Atlanta 🇺🇸", "start": "2026-09-30", "end": "2026-10-03"},
            {"round": "R8", "name": "COTA 🇺🇸", "start": "2026-10-23", "end": "2026-10-26"},
        ]
    },
    "Porsche Cup AU": {
        "color": "#2E7D32",  # Green
        "rounds": [
            {"round": "R1", "name": "Melbourne (F1 GP) 🇦🇺", "start": "2026-03-05", "end": "2026-03-09"},
            {"round": "R2", "name": "Hidden Valley, Darwin 🇦🇺", "start": "2026-06-19", "end": "2026-06-22"},
            {"round": "R3", "name": "Queensland Raceway 🇦🇺", "start": "2026-08-21", "end": "2026-08-24"},
            {"round": "R4", "name": "The Bend, SA 🇦🇺", "start": "2026-09-11", "end": "2026-09-14"},
            {"round": "R5", "name": "Bathurst 1000 🇦🇺", "start": "2026-10-08", "end": "2026-10-12"},
            {"round": "R6", "name": "Gold Coast 500 🇦🇺", "start": "2026-10-23", "end": "2026-10-26"},
            {"round": "R7", "name": "Adelaide 🇦🇺", "start": "2026-12-03", "end": "2026-12-07"},
        ]
    },
    "Porsche Cup NZ": {
        "color": "#00695C",  # Dark Teal
        "rounds": [
            {"round": "R1", "name": "Hampton Downs 🇳🇿", "start": "2026-01-09", "end": "2026-01-12"},
            {"round": "R2", "name": "Manfeild 🇳🇿", "start": "2026-02-27", "end": "2026-03-02"},
            {"round": "R3", "name": "Taupo 🇳🇿", "start": "2026-03-27", "end": "2026-03-29"},
            {"round": "R4", "name": "Taupo (ANZAC) 🇳🇿", "start": "2026-04-24", "end": "2026-04-26"},
        ]
    },
    "Porsche Sprint NA": {
        "color": "#4527A0",  # Deep Purple
        "rounds": [
            {"round": "R1", "name": "Sebring 🇺🇸", "start": "2026-03-06", "end": "2026-03-09"},
            {"round": "R2", "name": "Barber Motorsports Park 🇺🇸", "start": "2026-03-27", "end": "2026-03-30"},
            {"round": "R3", "name": "Sonoma 🇺🇸", "start": "2026-04-10", "end": "2026-04-13"},
            {"round": "R4", "name": "COTA 🇺🇸", "start": "2026-05-07", "end": "2026-05-10"},
            {"round": "R5", "name": "VIR 🇺🇸", "start": "2026-06-19", "end": "2026-06-22"},
            {"round": "R6", "name": "Road America 🇺🇸", "start": "2026-08-14", "end": "2026-08-17"},
            {"round": "R7", "name": "Road Atlanta 🇺🇸", "start": "2026-09-11", "end": "2026-09-14"},
        ]
    },
    "DTM": {
        "color": "#37474F",  # Dark Grey-Blue
        "rounds": [
            {"round": "R1", "name": "Red Bull Ring 🇦🇹", "start": "2026-04-24", "end": "2026-04-28"},
            {"round": "R2", "name": "Lausitzring 🇩🇪", "start": "2026-06-19", "end": "2026-06-23"},
            {"round": "R3", "name": "Norisring 🇩🇪", "start": "2026-07-03", "end": "2026-07-06"},
            {"round": "R4", "name": "Oschersleben 🇩🇪", "start": "2026-07-24", "end": "2026-07-27"},
            {"round": "R5", "name": "Nürburgring 🇩🇪", "start": "2026-08-14", "end": "2026-08-18"},
            {"round": "R6", "name": "Sachsenring 🇩🇪", "start": "2026-09-11", "end": "2026-09-14"},
            {"round": "R7", "name": "Zandvoort 🇳🇱", "start": "2026-09-25", "end": "2026-09-28"},
            {"round": "R8", "name": "Hockenheimring 🇩🇪", "start": "2026-10-09", "end": "2026-10-12"},
        ]
    },
    "IndyNXT": {
        "color": "#D32F2F",  # IndyCar Red
        "rounds": [
            {"round": "R1", "name": "St. Petersburg 🇺🇸", "start": "2026-02-27", "end": "2026-03-01"},
            {"round": "R2", "name": "Arlington 🇺🇸", "start": "2026-03-13", "end": "2026-03-15"},
            {"round": "R3", "name": "Barber Motorsports Park 🇺🇸", "start": "2026-03-27", "end": "2026-03-29"},
            {"round": "R4", "name": "Indianapolis (Road) 🇺🇸", "start": "2026-05-07", "end": "2026-05-09"},
            {"round": "R5", "name": "Detroit 🇺🇸", "start": "2026-05-29", "end": "2026-05-31"},
            {"round": "R6", "name": "WWT Raceway 🇺🇸", "start": "2026-06-05", "end": "2026-06-07"},
            {"round": "R7", "name": "Road America 🇺🇸", "start": "2026-06-19", "end": "2026-06-21"},
            {"round": "R8", "name": "Mid-Ohio 🇺🇸", "start": "2026-07-03", "end": "2026-07-05"},
            {"round": "R9", "name": "Nashville 🇺🇸", "start": "2026-07-17", "end": "2026-07-19"},
            {"round": "R10", "name": "Portland 🇺🇸", "start": "2026-08-07", "end": "2026-08-09"},
            {"round": "R11", "name": "Milwaukee 🇺🇸", "start": "2026-08-28", "end": "2026-08-30"},
            {"round": "R12", "name": "Laguna Seca 🇺🇸", "start": "2026-09-04", "end": "2026-09-06"},
        ]
    },
    "GR86 Championship NZ": {
        "color": "#B71C1C",  # Dark Red (Toyota GR Racing)
        "rounds": [
            {"round": "R1", "name": "Hampton Downs Motorsport Park 🇳🇿", "start": "2025-10-31", "end": "2025-11-02"},
            {"round": "R2", "name": "Hampton Downs Motorsport Park 🇳🇿", "start": "2026-01-07", "end": "2026-01-11"},
            {"round": "R3", "name": "Teretonga Park, Invercargill 🇳🇿", "start": "2026-01-23", "end": "2026-01-25"},
            {"round": "R4", "name": "Highlands Motorsport Park, Cromwell 🇳🇿", "start": "2026-01-30", "end": "2026-02-01"},
            {"round": "R5", "name": "Manfeild – Circuit Chris Amon, Feilding 🇳🇿", "start": "2026-02-27", "end": "2026-03-01"},
            {"round": "R6", "name": "Taupo International Motorsport Park 🇳🇿", "start": "2026-04-10", "end": "2026-04-12"},
        ]
    },
    "CTFROC (Formula Regional Oceania)": {
        "color": "#1A237E",  # Navy (FIA single-seater)
        "rounds": [
            {"round": "R1", "name": "Hampton Downs Motorsport Park 🇳🇿", "start": "2026-01-08", "end": "2026-01-11"},
            {"round": "R2", "name": "Taupo International Motorsport Park 🇳🇿", "start": "2026-01-16", "end": "2026-01-18"},
            {"round": "R3", "name": "Teretonga Park, Invercargill 🇳🇿", "start": "2026-01-22", "end": "2026-01-25"},
            {"round": "R4", "name": "Highlands Motorsport Park, Cromwell 🇳🇿", "start": "2026-01-29", "end": "2026-02-01"},
        ]
    },
    "Summerset GT NZ": {
        "color": "#FF6F00",  # Amber
        "rounds": [
            {"round": "R1", "name": "Hampton Downs 🇳🇿", "start": "2025-10-31", "end": "2025-11-02"},
            {"round": "R2", "name": "Teretonga Park, Invercargill 🇳🇿", "start": "2026-01-23", "end": "2026-01-25"},
            {"round": "R3", "name": "Highlands Motorsport Park, Cromwell 🇳🇿", "start": "2026-01-30", "end": "2026-02-01"},
            {"round": "R4", "name": "Manfeild Grand Finale 🇳🇿", "start": "2026-02-27", "end": "2026-03-01"},
        ]
    },
    "GTRNZ": {
        "color": "#004D40",  # Dark Teal-Green
        "rounds": [
            {"round": "R1", "name": "Hampton Downs 🇳🇿", "start": "2026-01-09", "end": "2026-01-11"},
        ]
    },
    "TA2 NZ": {
        "color": "#E53935",  # Muscle Car Red
        "rounds": [
            {"round": "R1", "name": "Hampton Downs International 🇳🇿", "start": "2025-10-31", "end": "2025-11-02"},
            {"round": "R2", "name": "Hampton Downs National 🇳🇿", "start": "2025-11-22", "end": "2025-11-23"},
            {"round": "R3", "name": "Teretonga Park 🇳🇿", "start": "2026-01-23", "end": "2026-01-25"},
            {"round": "R4", "name": "Highlands Motorsport Park 🇳🇿", "start": "2026-01-30", "end": "2026-02-01"},
            {"round": "R5", "name": "Manfeild 🇳🇿", "start": "2026-02-27", "end": "2026-03-01"},
            {"round": "R6", "name": "Taupo (Supercars) 🇳🇿", "start": "2026-04-10", "end": "2026-04-12"},
        ]
    },
    "NZ Formula Ford": {
        "color": "#5C6BC0",  # Indigo
        "rounds": [
            {"round": "R1", "name": "Hampton Downs 🇳🇿", "start": "2025-10-31", "end": "2025-11-02"},
            {"round": "R2", "name": "Manfeild Grand Finale 🇳🇿", "start": "2026-02-27", "end": "2026-03-01"},
        ]
    },
}


def _get_last_finished_round(series_name, today=None):
    """Return the most recently finished round for a championship, or None.

    Returns (round_dict, days_ago) where days_ago is how many days since the
    round ended.  Always returns the most recent completed round — no time
    limit, so on the Monday after a race weekend you'll always get the right one.
    """
    if today is None:
        today = datetime.now().date()
    data = RACE_CALENDARS.get(series_name)
    if not data:
        return None
    best = None
    for rd in data["rounds"]:
        end = datetime.strptime(rd["end"], "%Y-%m-%d").date()
        if end <= today:
            days_ago = (today - end).days
            if best is None or days_ago < best[1]:
                best = (rd, days_ago)
    return best


# --- DATA LOADING ---
# --- DATA LOADING ---
# --- DATA LOADING ---
try:
    import gsheets_loader
    from gsheets_loader import load_google_sheet
    HAS_GSHEETS = True
except ImportError:
    HAS_GSHEETS = False


# --- CONSTANTS ---
from ui_components import REPLY_TEMPLATES, render_unified_card_content




@st.cache_resource(ttl=300) # 5 min — auto-refresh JS handles 60s updates
def load_dashboard_data(overrides=None):
    dashboard = FunnelDashboard(DATA_DIR, overrides=overrides)
    return dashboard

@st.cache_resource
def load_settings_store():
    """Persistent settings store via Airtable 'Settings' table.
    Survives Streamlit Cloud container recycles (unlike JSON files on disk)."""
    if AirtableSettingsStore is None:
        print("AirtableSettingsStore not available (module cache stale?)")
        return None
    if "airtable" in st.secrets:
        try:
            return AirtableSettingsStore(
                api_key=st.secrets["airtable"]["api_key"],
                base_id=st.secrets["airtable"]["base_id"],
                table_name="Settings"
            )
        except Exception as e:
            print(f"Settings store init failed: {e}")
    return None




# ==============================================================================
# VIEW FUNCTIONS
# ==============================================================================


# ==============================================================================
# render_unified_card_content Imported from ui_components



@st.dialog("Driver Details", width="large")
def view_unified_dialog(r, dashboard):
    # Dialog is actively rendering — reset stale counter
    st.session_state['_dash_stale_count'] = 0
    render_unified_card_content(r, dashboard, key_suffix="_dialog")
    if st.button("✖ Close", key="close_unified_dialog", use_container_width=True):
        if '_open_driver_card' in st.session_state:
            del st.session_state['_open_driver_card']
        st.rerun(scope="app")

# ==============================================================================
# DEEP LINKING: Find driver by name, email, or social URL
# ==============================================================================
def find_driver_by_identifier(dashboard, identifier: str):
    """Search for a driver by email, name, or social URL fragment.
    Returns (driver, match_type) or (None, None). If multiple fuzzy matches,
    returns (list_of_tuples, 'multiple')."""
    if not identifier:
        return None, None
    identifier = unquote_plus(identifier).strip()
    drivers_list = list(dashboard.drivers.values())
    id_lower = identifier.lower()

    # 1. Exact email match
    for driver in drivers_list:
        if driver.email.lower() == id_lower:
            return driver, 'email'

    # 2. Exact name match (case-insensitive)
    for driver in drivers_list:
        if driver.full_name.lower() == id_lower:
            return driver, 'name'

    # 3. Fuzzy name match
    def _norm(s):
        return ''.join(c for c in s.lower() if c.isalnum() or c == ' ').strip()

    matches = []
    norm_id = _norm(identifier)
    for driver in drivers_list:
        score = SequenceMatcher(None, _norm(driver.full_name), norm_id).ratio()
        if score >= 0.75:
            matches.append((driver, score))

    if len(matches) == 1:
        return matches[0][0], 'fuzzy'
    elif len(matches) > 1:
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches, 'multiple'

    # 4. Social URL path matching
    def _url_username(url):
        if not url:
            return ''
        return str(url).rstrip('/').split('/')[-1].lower().replace('.', ' ').replace('_', ' ')

    for driver in drivers_list:
        for url in [driver.facebook_url, driver.instagram_url]:
            uname = _url_username(url)
            if uname and (uname == id_lower or uname == id_lower.replace(' ', '')):
                return driver, 'url'

    return None, None

# HELPER: Social URL Formatter
def _make_clickable_url(val, platform):
    if not val: return None
    s_val = str(val).strip()
    if s_val.lower().startswith("http"): return s_val
    if platform == "fb": return f"https://www.facebook.com/{s_val}"
    if platform == "ig": return f"https://www.instagram.com/{s_val}"
    return s_val

@st.dialog("Calendar – Driver Details", width="large")
def view_calendar_dialog(r, dashboard):
    # Dialog is actively rendering — reset stale counter
    st.session_state['_cal_stale_count'] = 0
    # Quick header with social links for fast workflow
    hdr_cols = st.columns([3, 1, 1, 1])
    with hdr_cols[0]:
        stage_label = r.current_stage.value if hasattr(r.current_stage, 'value') else str(r.current_stage)
        st.caption(f"Stage: **{stage_label}** · {r.days_in_current_stage}d")
    with hdr_cols[1]:
        if r.facebook_url:
            fb_url = r.facebook_url if str(r.facebook_url).startswith('http') else f"https://www.facebook.com/{r.facebook_url}"
            from urllib.parse import quote
            fb_url += f"#ag_driver={quote(r.full_name)}"
            st.markdown(f"[Open FB]({fb_url})")
    with hdr_cols[2]:
        if r.instagram_url:
            ig_url = r.instagram_url if str(r.instagram_url).startswith('http') else f"https://www.instagram.com/{r.instagram_url}"
            from urllib.parse import quote
            ig_url += f"#ag_driver={quote(r.full_name)}"
            st.markdown(f"[Open IG]({ig_url})")
    with hdr_cols[3]:
        if st.button("✕ Close", key="cal_close_top_btn", type="primary"):
            if 'calendar_selected_driver' in st.session_state:
                del st.session_state['calendar_selected_driver']
            st.session_state.pop('_cal_stale_count', None)
            st.session_state['_cal_dismissed'] = True
            st.rerun(scope="app")

    render_unified_card_content(r, dashboard, key_suffix="_cal")

    if st.button("← Back to Calendar", key="cal_close_main_btn", use_container_width=True):
        if 'calendar_selected_driver' in st.session_state:
             del st.session_state['calendar_selected_driver']
        st.session_state.pop('_cal_stale_count', None)
        st.session_state['_cal_dismissed'] = True
        st.rerun(scope="app")
# OLD CARD CODE REMOVED — all card rendering now uses render_unified_card_content
# from ui_components.py (imported at top). See view_unified_dialog (line 110).


def render_dashboard(dashboard, daily_metrics, drivers):
    def _normalize_dt(dt):
        from datetime import date as _date
        if isinstance(dt, datetime):
            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                return dt.astimezone().replace(tzinfo=None)
            return dt
        if isinstance(dt, _date):
            return datetime(dt.year, dt.month, dt.day)
        if isinstance(dt, str):
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'):
                try:
                    return datetime.strptime(dt, fmt)
                except (ValueError, TypeError):
                    continue
        return None

    # =========================================================================
    # DASHBOARD — Position vs Plan
    # =========================================================================
    now = datetime.now()
    import calendar
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    days_remaining = max(1, days_in_month - now.day + 1)  # include today
    days_elapsed = now.day

    SALES_TARGET = 10

    # --- Count MTD activity: driver must be AT that stage AND have the date this month ---
    # This matches exactly what the pipeline board shows.
    def _is_this_month(dt_val):
        d = _normalize_dt(dt_val)
        return d and d.month == now.month and d.year == now.year
    _STAGE_MAP = [
        ('messaged', [FunnelStage.MESSAGED, FunnelStage.OUTREACH], 'outreach_date'),
        ('replied',  [FunnelStage.REPLIED], 'replied_date'),
        ('link_sent', [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT], 'link_sent_date'),
        ('registered', [FunnelStage.BLUEPRINT_STARTED, FunnelStage.REGISTERED], 'registered_date'),
        ('day1', [FunnelStage.DAY1_COMPLETE], 'day1_complete_date'),
        ('day2', [FunnelStage.DAY2_COMPLETE], 'day2_complete_date'),
        ('calls', [FunnelStage.STRATEGY_CALL_BOOKED], 'strategy_call_booked_date'),
        ('sales', [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED], 'sale_closed_date'),
    ]

    mtd = {k: 0 for k, _, _ in _STAGE_MAP}
    for r in drivers.values():
        # Skip legacy drivers whose dates are inferred from createdTime, not real Airtable dates
        if getattr(r, '_date_is_fallback', False):
            continue
        for key, stages, date_attr in _STAGE_MAP:
            if r.current_stage in stages and _is_this_month(getattr(r, date_attr, None)):
                mtd[key] += 1

    sales_needed = max(0, SALES_TARGET - mtd['sales'])

    # --- Conversion rates (actual MTD, with sensible defaults) ---
    def _rate(num, denom, default):
        return num / denom if denom > 0 else default

    r_msg_reply = _rate(mtd['replied'], mtd['messaged'], 0.10)
    r_reply_link = _rate(mtd['link_sent'], mtd['replied'], 0.50)
    r_link_reg = _rate(mtd['registered'], mtd['link_sent'], 0.15)
    r_reg_day1 = _rate(mtd['day1'], mtd['registered'], 0.70)
    r_day1_day2 = _rate(mtd['day2'], mtd['day1'], 0.60)
    r_day2_call = _rate(mtd['calls'], mtd['day2'], 0.40)
    r_call_sale = _rate(mtd['sales'], mtd['calls'], 0.25)

    # Reverse funnel: messages needed to close remaining sales
    _total_rate = r_msg_reply * r_reply_link * r_link_reg * r_reg_day1 * r_day1_day2 * r_day2_call * r_call_sale
    messages_needed = int(sales_needed / _total_rate) if _total_rate > 0 else 0
    messages_per_day = int(messages_needed / days_remaining) if days_remaining > 0 else 0

    # On-pace check
    expected_sales_by_now = round(SALES_TARGET * (days_elapsed / days_in_month), 1)
    pace = "ahead" if mtd['sales'] >= expected_sales_by_now else "behind"

    # ── ROW 1: Position vs Plan ──
    st.markdown("### 🎯 Position vs Plan")
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Sales Target", f"{mtd['sales']} / {SALES_TARGET}",
              f"{sales_needed} to go" if sales_needed > 0 else "TARGET HIT! 🎉")
    p2.metric("Days Left", days_remaining, f"Day {days_elapsed} of {days_in_month}")
    p3.metric("Pace", f"{pace.upper()}", f"expect {expected_sales_by_now} by today")
    p4.metric("Pipeline", f"{mtd['replied'] + mtd['link_sent'] + mtd['registered']} active",
              "replied + link sent + registered")

    if mtd['sales'] >= SALES_TARGET:
        st.success(f"🎉 **TARGET HIT!** {mtd['sales']} sales this month!")

    st.divider()

    # ── ROW 2: Today's Actions ──
    st.markdown("### ⚡ Today's Actions to Get on Plan")
    a1, a2, a3 = st.columns(3)
    a1.metric("📨 Messages to Send Today", messages_per_day,
              f"{int(messages_per_day/2)} FB + {int(messages_per_day/2)} IG")
    a2.metric("📩 Follow-ups Due", sum(
        1 for r in drivers.values()
        if r.current_stage in [FunnelStage.MESSAGED, FunnelStage.OUTREACH, FunnelStage.LINK_SENT,
                               FunnelStage.BLUEPRINT_LINK_SENT, FunnelStage.REPLIED]
        and _is_this_month(r.outreach_date or r.last_activity)
    ))
    a3.metric("🔥 Conversion Rate", f"{round(r_msg_reply * 100)}% reply",
              f"msg→sale: {round(_total_rate * 100, 2)}%")

    st.divider()

    # ── ROW 3: Today's Activity ──
    st.markdown("### 📈 Today's Activity")
    def _is_today(dt_val):
        d = _normalize_dt(dt_val)
        return d and d.date() == now.date() if d else False

    # Count activity that happened TODAY
    today_counts = {k: 0 for k, _, _ in _STAGE_MAP}
    today_names = {k: [] for k, _, _ in _STAGE_MAP}
    for r in drivers.values():
        if getattr(r, '_date_is_fallback', False):
            continue
        for key, stages, date_attr in _STAGE_MAP:
            stage_date = getattr(r, date_attr, None)
            if r.current_stage in stages and _is_today(stage_date):
                today_counts[key] += 1
                today_names[key].append(r.first_name or r.full_name.split(' ')[0])

    t1, t2, t3, t4, t5, t6, t7, t8 = st.columns(8)
    for col, (key, _, _) in zip([t1, t2, t3, t4, t5, t6, t7, t8], _STAGE_MAP):
        label = key.replace('_', ' ').title()
        count = today_counts[key]
        names = today_names[key]
        delta = ', '.join(names[:3]) + ('...' if len(names) > 3 else '') if names else None
        col.metric(label, count, delta=delta, delta_color="normal" if count > 0 else "off")

    st.divider()

    # ── ROW 4: Pipeline Snapshot ──
    st.markdown("### 📊 Pipeline This Month")
    s1, s2, s3, s4, s5, s6, s7, s8 = st.columns(8)
    s1.metric("Messaged", mtd['messaged'])
    s2.metric("Replied", mtd['replied'])
    s3.metric("Link Sent", mtd['link_sent'])
    s4.metric("Registered", mtd['registered'])
    s5.metric("Day 1", mtd['day1'])
    s6.metric("Day 2", mtd['day2'])
    s7.metric("Calls", mtd['calls'])
    s8.metric("Sales", mtd['sales'])

    st.divider()

    # DRIVER SEARCH BAR
    # ---------------------------------------------------------
    with st.expander("🔍 **Search All Drivers**", expanded=False):
        search_col1, search_col2 = st.columns([3, 1])
        with search_col1:
            driver_search = st.text_input("Search by name, email, or notes...", key="dashboard_driver_search", label_visibility="collapsed", placeholder="Search drivers...")

        if driver_search:
            # Search across all drivers
            search_results = []
            for r in drivers.values():
                search_lower = driver_search.lower()
                if (search_lower in r.full_name.lower() or
                    search_lower in str(r.email).lower() or
                    search_lower in str(r.notes or '').lower() or
                    search_lower in str(r.facebook_url or '').lower() or
                    search_lower in str(r.instagram_url or '').lower()):
                    search_results.append(r)

            st.caption(f"Found {len(search_results)} matches")

            if search_results:
                for r in search_results[:20]:  # Limit to 20 results
                    with st.container():
                        cols = st.columns([3, 2, 2, 1])
                        cols[0].write(f"**{r.full_name}**")
                        cols[1].caption(r.current_stage.value if hasattr(r.current_stage, 'value') else str(r.current_stage))
                        if r.facebook_url:
                            cols[2].markdown(f"[FB]({r.facebook_url})", unsafe_allow_html=True)
                        elif r.instagram_url:
                            cols[2].markdown(f"[IG]({r.instagram_url})", unsafe_allow_html=True)
                        if cols[3].button("📇", key=f"search_open_{r.email}", help="Open Contact Card"):
                            st.session_state['search_selected_driver'] = r.email
                            st.rerun()

                if len(search_results) > 20:
                    st.info(f"Showing first 20 of {len(search_results)} results")

    # Show contact card if selected from search — routed through early check
    if 'search_selected_driver' in st.session_state:
        st.session_state['_open_driver_card'] = st.session_state.pop('search_selected_driver')

    # PIPELINE BOARD
    # ---------------------------------------------------------
    c_head, c_refresh = st.columns([3, 0.5])
    c_head.subheader("📅 Pipeline — Current Month")
    if c_refresh.button("🔄", help="Refresh pipeline — fetch latest from Airtable", use_container_width=True):
        load_dashboard_data.clear()
        st.rerun()
    
    enable_wide = c_head.checkbox("↔️ Wide View", value=False, help="Enable horizontal scrolling for wider columns.")

    if enable_wide:
        st.markdown("""
            <style>
            /* Force horizontal scroll for column containers */
            div[data-testid="stHorizontalBlock"] {
                flex-wrap: nowrap !important;
                overflow-x: auto !important;
                padding-bottom: 10px;
            }
            /* Force minimum width on columns within the block */
            div[data-testid="column"] {
                min-width: 300px !important;
                flex: 0 0 auto !important;
            }
            /* Adjust main container to not clip */
            .block-container {
                max-width: 100% !important;
            }
            </style>
        """, unsafe_allow_html=True)





    # Race review debug removed — was firing on every rerun and bloating logs

    now = datetime.now()
    
    # Logic

    def _date_matches_filter(d):
        """Check if a date falls within the visible pipeline window.
        
        Rules:
        - Show all activity from the last 14 days (rolling window)
        - No calendar month boundaries — drivers never vanish overnight
        """
        if not d:
            return False
        age_days = (now - d).days if isinstance(d, datetime) else 999
        return age_days <= 14

    # ═══════════════════════════════════════════════════════════════════
    # PIPELINE VISIBILITY — Airtable is the SINGLE source of truth.
    # Every stage uses its stage-specific date. ZERO fallbacks.
    # Rolling 14-day window for ALL stages.
    # Google Sheets feed Airtable. Airtable feeds the pipeline.
    # ═══════════════════════════════════════════════════════════════════

    def is_in_timeframe(r, date_attr):
        """Show contact only if its STAGE DATE is within the last 14 days.
        No fallbacks. If the stage date is missing, the contact is not visible.
        Airtable is the bible — if the date isn't there, fix the data."""
        d = _normalize_dt(getattr(r, date_attr, None))
        if not d:
            return False
        return _date_matches_filter(d)

    def _driver_sort_key(r, date_attr):
        """Sort by stage date only. Most recent first."""
        d = _normalize_dt(getattr(r, date_attr, None))
        return (d or datetime.min, r.full_name or r.email or '')

    # 1. Pipeline Stages Definition
    # --- MAIN PIPELINE STAGES (9 columns) ---
    # Each driver stays in their actual stage column.
    # Cards change colour when a driver needs follow-up (24h+ stale).
    STAGES = [
        {"label": "Messaged", "emoji": "📨", "val": [FunnelStage.MESSAGED, FunnelStage.OUTREACH], "date_attr": 'outreach_date', "next_action": "Wait for reply", "color": "#3b82f6"},
        {"label": "Replied", "emoji": "↩️", "val": [FunnelStage.REPLIED], "date_attr": 'replied_date', "next_action": "Send review link", "color": "#6366f1"},
        {"label": "Link Sent", "emoji": "🔗", "val": [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT], "date_attr": 'link_sent_date', "next_action": "Chase review completion", "color": "#8b5cf6"},
        {"label": "Race Review", "emoji": "📊", "val": [FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE], "date_attr": 'race_weekend_review_date', "next_action": "Send Blueprint link", "color": "#a855f7"},
        {"label": "Registered", "emoji": "📋", "val": [FunnelStage.BLUEPRINT_STARTED, FunnelStage.REGISTERED], "date_attr": 'registered_date', "next_action": "Nudge to start Day 1", "color": "#0d9488"},
        {"label": "Day 1", "emoji": "1️⃣", "val": [FunnelStage.DAY1_COMPLETE], "date_attr": 'day1_complete_date', "next_action": "Nudge to do Day 2", "color": "#059669"},
        {"label": "Day 2", "emoji": "2️⃣", "val": [FunnelStage.DAY2_COMPLETE], "date_attr": 'day2_complete_date', "next_action": "Book strategy call", "color": "#16a34a"},
        {"label": "Call Booked", "emoji": "📞", "val": [FunnelStage.STRATEGY_CALL_BOOKED, FunnelStage.STRATEGY_CALL_NO_SHOW], "date_attr": 'strategy_call_booked_date', "next_action": "Close the sale", "color": "#ca8a04"},
        {"label": "Clients", "emoji": "🏆", "val": [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED], "date_attr": 'sale_closed_date', "next_action": "Onboard", "color": "#16a34a"},
    ]

    def _needs_follow_up(r, date_attr):
        """True if driver has been at this stage for 24h+. Stage date only."""
        if r.current_stage in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED]:
            return False
        d = getattr(r, date_attr, None)
        if d and isinstance(d, datetime):
            return (now - d).total_seconds() / 3600 >= 24
        return False

    # Stall thresholds (hours) — drivers past this are flagged with urgency colors
    STALL_THRESHOLDS = {
        FunnelStage.MESSAGED: 48,           # 48h no reply
        FunnelStage.LINK_SENT: 24,          # 24h no action
        FunnelStage.BLUEPRINT_LINK_SENT: 24,
        FunnelStage.RACE_REVIEW_COMPLETE: 24,
        FunnelStage.RACE_WEEKEND: 24,
        FunnelStage.DAY1_COMPLETE: 24,
        FunnelStage.DAY2_COMPLETE: 24,
    }

    # Lead magnet stages (shown in separate section below pipeline)
    LEAD_MAGNET_STAGES = [
        {"label": "Sleep Test", "val": [FunnelStage.SLEEP_TEST_COMPLETED], "date_attr": 'sleep_test_date'},
        {"label": "Mindset Quiz", "val": [FunnelStage.MINDSET_QUIZ_COMPLETED], "date_attr": 'mindset_quiz_date'},
        {"label": "Flow Profile", "val": [FunnelStage.FLOW_PROFILE_COMPLETED], "date_attr": 'flow_profile_date'},
    ]

    # Disqualified statuses - drivers with these are filtered out of pipeline view
    DISQUALIFIED_STATUSES = [FunnelStage.NOT_A_FIT, FunnelStage.DOES_NOT_REPLY, FunnelStage.NO_SOCIALS]

    # 2. Render Board

    # SELF-HEALING AUDIT REMOVED — was auto-promoting thousands of Contact drivers
    # to Messaged based on having a social URL, flooding the pipeline with false entries.
    # Airtable is the source of truth. Stages only change via actual messaging activity.

    def _days_at_stage(r, date_attr):
        """Days since entering current stage. Stage date only. No fallbacks."""
        d = getattr(r, date_attr, None)
        if d and isinstance(d, datetime):
            return max(0, (now - d).days)
        return 0

    def _hours_at_stage(r, date_attr):
        """Hours since entering current stage."""
        d = getattr(r, date_attr, None)
        if d and isinstance(d, datetime):
            return max(0, (now - d).total_seconds() / 3600)
        return 0

    # Helper: is this driver stalled? (past their threshold)
    def _is_stalled(r):
        threshold = STALL_THRESHOLDS.get(r.current_stage)
        if not threshold:
            return r.days_in_current_stage > 3 and r.current_stage not in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED]
        # Find the matching stage date_attr
        for s in STAGES:
            if r.current_stage in s['val']:
                d = getattr(r, s['date_attr'], None)
                if d and isinstance(d, datetime):
                    return (now - d).total_seconds() / 3600 >= threshold
                break
        return False

    # ---- GLOBAL PIPELINE CSS ----
    st.markdown("""
        <style>
        .pipeline-card {
            border-radius: 8px;
            padding: 10px 12px;
            margin-bottom: 6px;
            border-left: 4px solid transparent;
            transition: transform 0.15s ease;
            cursor: pointer;
            font-size: 13px;
            line-height: 1.4;
        }
        .pipeline-card:hover {
            transform: translateX(2px);
        }
        .card-fresh {
            background: rgba(16, 185, 129, 0.08);
            border-left-color: #10b981;
        }
        .card-waiting {
            background: rgba(245, 158, 11, 0.10);
            border-left-color: #f59e0b;
        }
        .card-urgent {
            background: rgba(239, 68, 68, 0.10);
            border-left-color: #ef4444;
        }
        .card-won {
            background: rgba(16, 185, 129, 0.15);
            border-left-color: #10b981;
        }
        .action-ok { color: #10b981; font-size: 11px; }
        .action-needed { color: #f59e0b; font-size: 11px; }
        .action-urgent { color: #ef4444; font-size: 11px; font-weight: 600; }
        .stage-header {
            padding: 6px 10px;
            border-radius: 6px;
            color: white;
            font-weight: 700;
            font-size: 13px;
            text-align: center;
            margin-bottom: 4px;
        }
        .stage-count {
            text-align: center;
            font-size: 12px;
            color: #9ca3af;
            margin-bottom: 4px;
        }
        .fu-badge {
            background: #f59e0b;
            color: #000;
            padding: 1px 6px;
            border-radius: 10px;
            font-size: 11px;
            font-weight: 600;
            margin-left: 4px;
        }
        </style>
    """, unsafe_allow_html=True)

    # ---- FOLLOW-UP ALERT BANNER ----
    _followup_count = sum(
        1 for _r in drivers.values()
        if _r.current_stage not in DISQUALIFIED_STATUSES
        and any(
            _r.current_stage in s['val'] and _needs_follow_up(_r, s['date_attr'])
            for s in STAGES
        )
    )
    if _followup_count > 0:
        st.warning(f"⚠️ **{_followup_count} driver{'s' if _followup_count != 1 else ''} need follow-up** — look for 🟠🔴 cards below")

    # --- PIPELINE VIEW ---
    cols = st.columns(len(STAGES))

    for idx, stage in enumerate(STAGES):
        with cols[idx]:
            # Collect ALL drivers at this stage
            target_vals = [s.value for s in stage['val']]
            all_stage_drivers = [
                r for r in drivers.values()
                if (r.current_stage in stage['val'] or r.current_stage.value in target_vals)
                and r.current_stage not in DISQUALIFIED_STATUSES
            ]

            # FILTER — only show drivers whose stage date is in current month
            stage_drivers_raw = [r for r in all_stage_drivers if is_in_timeframe(r, stage['date_attr'])]

            # DEDUPLICATE — same person can appear under multiple keys (email + no_email_ slug)
            seen_ids = set()
            stage_drivers = []
            for r in stage_drivers_raw:
                # Prefer Airtable record ID, fall back to full name
                dedup_key = getattr(r, 'airtable_record_id', None) or r.full_name.lower().strip()
                if dedup_key and dedup_key in seen_ids:
                    continue
                if dedup_key:
                    seen_ids.add(dedup_key)
                stage_drivers.append(r)

            # SORT: Messaged stage → most recent first (just waiting, no follow-ups).
            # All other stages → priority sort (stalled/needs action first, then by wait time).
            _is_messaged_stage = stage['label'] == 'Messaged'

            if _is_messaged_stage:
                # Most recently messaged at top
                def _recency_key(r):
                    d = _normalize_dt(getattr(r, stage['date_attr'], None))
                    return d or datetime.min
                stage_drivers.sort(key=_recency_key, reverse=True)
            else:
                def _priority_key(r):
                    is_stalled = _is_stalled(r)
                    needs_fu = _needs_follow_up(r, stage['date_attr'])
                    days = _days_at_stage(r, stage['date_attr'])
                    # Priority: stalled (0) > needs follow-up (1) > fresh (2)
                    if is_stalled:
                        priority = 0
                    elif needs_fu:
                        priority = 1
                    else:
                        priority = 2
                    return (priority, -days)

                stage_drivers.sort(key=_priority_key)

            # Count Badge + follow-up count
            fu_in_col = sum(1 for r in stage_drivers if _needs_follow_up(r, stage['date_attr']))

            # Color-coded header
            fu_html = f'<span class="fu-badge">📩 {fu_in_col}</span>' if fu_in_col > 0 else ""
            st.markdown(
                f'<div class="stage-header" style="background:{stage["color"]}">'
                f'{stage["emoji"]} {stage["label"]}'
                f'</div>',
                unsafe_allow_html=True
            )

            # Count display with follow-up badge
            count_html = f'<div class="stage-count">{len(stage_drivers)} driver{"s" if len(stage_drivers) != 1 else ""}{fu_html}</div>'
            st.markdown(count_html, unsafe_allow_html=True)

            st.divider()

            # ── DRIVER CARDS ──
            for ri, r in enumerate(stage_drivers):
                 days = _days_at_stage(r, stage['date_attr'])
                 hours = _hours_at_stage(r, stage['date_attr'])
                 needs_fu = _needs_follow_up(r, stage['date_attr'])

                 # Determine card class and action text
                 is_client = r.current_stage in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED]
                 is_no_show = r.current_stage == FunnelStage.STRATEGY_CALL_NO_SHOW
                 if is_client:
                     card_class = "card-won"
                     action_class = "action-ok"
                     action_text = "✅ Won"
                 elif is_no_show:
                     card_class = "card-urgent"
                     action_class = "action-urgent"
                     action_text = "📵 NO SHOW — Rebook call"
                 elif needs_fu and days >= 3:
                     card_class = "card-urgent"
                     action_class = "action-urgent"
                     action_text = f"🔴 {days}d — {stage['next_action']}"
                 elif needs_fu:
                     card_class = "card-waiting"
                     action_class = "action-needed"
                     action_text = f"🟡 {days}d — {stage['next_action']}"
                 else:
                     card_class = "card-fresh"
                     action_class = "action-ok"
                     if hours < 1:
                         action_text = "🟢 Just now"
                     elif hours < 24:
                         action_text = f"🟢 {int(hours)}h ago"
                     else:
                         action_text = f"🟢 {days}d ago"

                 # Name
                 real_name = f"{r.first_name} {r.last_name}".strip()
                 if real_name:
                     display_name = real_name
                 elif r.email.startswith("no_email_"):
                     display_name = r.email.replace("no_email_", "").replace("_", " ").title()
                 elif '@' in r.email:
                     display_name = r.email.split('@')[0]
                 elif r.email and not r.email.startswith("_unknown"):
                     display_name = r.email.replace("_", " ").title()
                 else:
                     display_name = "Unknown"

                 # Date display
                 stage_date = getattr(r, stage['date_attr'], None)
                 date_str = stage_date.strftime('%d %b') if stage_date else ""

                 # Follow-up sent recently?
                 import re as _re_pipe
                 _r_notes = r.notes or ""
                 _fu_sent_m = _re_pipe.search(r'\[(\d{2} \w{3} \d{2}:\d{2}) ✅\]', _r_notes)
                 _fu_sent_ok = False
                 if _fu_sent_m:
                     try:
                         _fu_ts = datetime.strptime(_fu_sent_m.group(1), "%d %b %H:%M").replace(year=now.year)
                         _fu_sent_ok = (now - _fu_ts).total_seconds() / 3600 <= 48
                     except ValueError:
                         pass

                 if _fu_sent_ok:
                     action_text = "✅ Followed up"
                     action_class = "action-ok"
                     card_class = "card-fresh"

                 # Build button label (keeping button for interactivity)
                 if is_client:
                     btn_icon = "🏆"
                 elif is_no_show:
                     btn_icon = "📵"
                 elif needs_fu and days >= 3:
                     btn_icon = "🔴"
                 elif needs_fu:
                     btn_icon = "🟡"
                 else:
                     btn_icon = "🟢"

                 # Check for missing social URLs
                 _has_socials = bool(getattr(r, 'facebook_url', None) or getattr(r, 'instagram_url', None))
                 _no_socials_html = '' if _has_socials or is_client else '<div style="font-size:10px;color:#ef4444;font-weight:600;">⚠️ No socials</div>'

                 # Render card HTML + button
                 st.markdown(
                     f'<div class="pipeline-card {card_class}">'
                     f'<div style="font-weight:600;">{btn_icon} {display_name}</div>'
                     f'<div class="{action_class}">{action_text}</div>'
                     f'<div style="font-size:11px;color:#6b7280;">📅 {date_str}</div>'
                     f'{_no_socials_html}'
                     f'</div>',
                     unsafe_allow_html=True
                 )

                 if st.button("⤴", key=f"btn_card_{r.email}_{idx}_{ri}", use_container_width=True):
                     st.session_state['_open_driver_card'] = r.email
                     st.rerun()

 
    # =================================================================
    # LEAD MAGNETS — Sleep Test / Mindset Quiz / Flow Profile
    # =================================================================
    st.divider()
    with st.expander("🧲 Lead Magnets — Sleep Test · Mindset Quiz · Flow Profile", expanded=False):
        lm_cols = st.columns(len(LEAD_MAGNET_STAGES))
        for lm_idx, lm_stage in enumerate(LEAD_MAGNET_STAGES):
            with lm_cols[lm_idx]:
                st.markdown(f"**{lm_stage['label']}**")

                lm_target_vals = [s.value for s in lm_stage['val']]
                lm_drivers = [
                    r for r in drivers.values()
                    if (r.current_stage in lm_stage['val'] or r.current_stage.value in lm_target_vals)
                    and r.current_stage not in DISQUALIFIED_STATUSES
                ]
                lm_drivers.sort(key=lambda x: _driver_sort_key(x, lm_stage['date_attr']), reverse=True)

                st.caption(f"{len(lm_drivers)} completed")
                st.divider()

                for r in lm_drivers:
                    name = r.full_name.strip() if r.full_name else ""
                    if not name:
                        if r.email.startswith("no_email_"):
                            name = r.email.replace("no_email_", "").replace("_", " ").title()
                        elif '@' in r.email:
                            name = r.email.split('@')[0]
                        else:
                            name = r.email or "Unknown"
                    d_val = getattr(r, lm_stage['date_attr'], None)
                    d_str = d_val.strftime('%d %b') if d_val else ""
                    btn_lbl = f"🟢 {name}"
                    if d_str:
                        btn_lbl += f"\n📅 {d_str}"
                    if st.button(btn_lbl, key=f"lm_{r.email}_{lm_idx}", use_container_width=True):
                        st.session_state['_open_driver_card'] = r.email
                        st.rerun()

    # FINANCIALS (Bottom)
    st.divider()
    rev_metrics = dashboard.get_revenue_metrics()
    st.header("💰 Monthly Targets & Forecast")
    
    f_col1, f_col2 = st.columns([3, 1])
    
    with f_col1:
        st.subheader(f"Revenue Progress: {rev_metrics['progress_pct']:.1f}%")
        st.progress(min(rev_metrics['progress_pct'] / 100, 1.0))
        
        fm1, fm2, fm3 = st.columns(3)
        fm1.metric("Actual Revenue", f"£{rev_metrics['actual']:,.0f}")
        fm2.metric("Target", f"£{rev_metrics['target']:,.0f}")
        fm3.metric("Remaining Needed", f"£{max(0, rev_metrics['target'] - rev_metrics['actual']):,.0f}")
    
    with f_col2:
        st.subheader("Calculator")
        needed = max(0, rev_metrics['target'] - rev_metrics['actual'])
        avg_sale = 4000
        sales_needed = needed / avg_sale if avg_sale > 0 else 0
        
        st.write(f"To hit target, you need **{sales_needed:.1f}** more sales.")
        
        # Simple conversion calc
        conv_rate_call_to_sale = 0.25 
        calls_needed = sales_needed / conv_rate_call_to_sale
        
        st.caption(f"Estimated Calls Needed: **{int(calls_needed)}**")

def render_race_outreach(dashboard):
    st.subheader("🏁 Race Outreach")
    
    import json

    # =====================================================================
    # PERSISTENT STORAGE FOR CIRCUITS & CHAMPIONSHIPS
    # Uses Airtable 'Settings' table (survives Streamlit Cloud restarts)
    # Falls back to JSON files + session state if Airtable unavailable
    # =====================================================================
    settings = load_settings_store()

    # --- CIRCUITS: Load from Airtable → file → session state (merge all) ---
    if 'saved_circuits' not in st.session_state:
        at_circuits = []
        if settings and settings.is_available:
            at_circuits = settings.get('circuits', []) or []
        file_circuits = dashboard.race_manager.get_all_circuits() or []
        st.session_state.saved_circuits = sorted(list(set(at_circuits + file_circuits)))

    rm_circuits = dashboard.race_manager.get_all_circuits() or []
    merged_circuits = sorted(list(set(st.session_state.saved_circuits + rm_circuits)))
    st.session_state.saved_circuits = merged_circuits
    saved_circuits = st.session_state.saved_circuits

    # --- CHAMPIONSHIPS: Load from Airtable → file → session state ---
    if 'saved_championships_loaded' not in st.session_state:
        at_champs = []
        if settings and settings.is_available:
            at_champs = settings.get('championships', []) or []
        c_file = os.path.join(DATA_DIR, "saved_championships.json")
        file_champs = []
        if os.path.exists(c_file):
            try:
                with open(c_file, 'r') as f:
                    file_champs = json.load(f)
            except:
                pass
        session_added = st.session_state.get('session_added_championships', [])
        all_champs = sorted(list(set(at_champs + file_champs + session_added)))
        st.session_state.session_added_championships = all_champs
        st.session_state.saved_championships_loaded = True

    session_added = st.session_state.get('session_added_championships', [])
    _calendar_champs = list(RACE_CALENDARS.keys())
    saved_champs = sorted(list(set(session_added + _calendar_champs)))

    # --- Helper: persist championships to Airtable + file (best-effort) ---
    def _persist_championships():
        if settings and settings.is_available:
            settings.set('championships', sorted(saved_champs))
        try:
            c_file = os.path.join(DATA_DIR, "saved_championships.json")
            with open(c_file, 'w') as f:
                json.dump(sorted(saved_champs), f)
        except:
            pass

    # --- Helper: persist circuits to Airtable + file + race_manager ---
    def _persist_circuit(name):
        if not name:
            return
        name = name.strip()
        if name not in st.session_state.saved_circuits:
            st.session_state.saved_circuits.append(name)
            st.session_state.saved_circuits = sorted(st.session_state.saved_circuits)
        if settings and settings.is_available:
            settings.set('circuits', st.session_state.saved_circuits)
        dashboard.race_manager.save_circuit(name)

    def _strip_flags(name):
        """Remove country flag emoji from circuit name."""
        import re as _re_flags
        return _re_flags.sub(r'[\U0001F1E0-\U0001F1FF]{2}', '', name).strip()

    def _format_round_dates(rd):
        """Format round dates as '18-20 Apr' or '30 Jan-2 Feb'."""
        try:
            s = datetime.strptime(rd['start'], "%Y-%m-%d")
            e = datetime.strptime(rd['end'], "%Y-%m-%d")
            if s.month == e.month:
                return f"{s.day}-{e.day} {s.strftime('%b')}"
            else:
                return f"{s.day} {s.strftime('%b')}-{e.day} {e.strftime('%b')}"
        except:
            return rd.get('start', '')

    # Sticky Global Championship
    if 'global_championship' not in st.session_state:
        st.session_state.global_championship = saved_champs[0] if saved_champs else ""

    # =====================================================================
    # 📢 THIS WEEK'S EVENTS — Alert banner
    # =====================================================================
    _today = datetime.now().date()
    _week_start = _today - timedelta(days=_today.weekday())  # Monday
    _week_end = _week_start + timedelta(days=6)  # Sunday

    _this_week = []
    _just_finished = []  # Ended in last 3 days (Mon outreach window)
    for _sname, _sdata in RACE_CALENDARS.items():
        for _rd in _sdata['rounds']:
            _rs = datetime.strptime(_rd['start'], "%Y-%m-%d").date()
            _re = datetime.strptime(_rd['end'], "%Y-%m-%d").date()
            # Currently happening or starts this week
            if _rs <= _week_end and _re >= _week_start:
                _this_week.append((_sname, _rd, _sdata['color']))
            # Ended within last 3 days (outreach window)
            elif 0 <= (_today - _re).days <= 3:
                _just_finished.append((_sname, _rd, _sdata['color']))

    if _just_finished:
        _fin_parts = []
        for _sn, _rd, _clr in _just_finished:
            _days_ago = (_today - datetime.strptime(_rd['end'], "%Y-%m-%d").date()).days
            _when = "today" if _days_ago == 0 else "yesterday" if _days_ago == 1 else f"{_days_ago}d ago"
            _fin_parts.append(f"**{_sn}** {_rd['round']} — {_strip_flags(_rd['name'])} (ended {_when})")
        st.success("🔥 **Ready for Outreach:** " + " · ".join(_fin_parts))

    if _this_week:
        _tw_parts = []
        for _sn, _rd, _clr in _this_week:
            _tw_parts.append(f"**{_sn}** {_rd['round']} — {_strip_flags(_rd['name'])} ({_format_round_dates(_rd)})")
        st.info("📅 **This Week:** " + " · ".join(_tw_parts))

    # =====================================================================
    # 🔬 RESEARCH NEW CHAMPIONSHIP (Tavily + Gemini Flash)
    # =====================================================================
    _has_research_keys = (
        "research" in st.secrets
        and st.secrets["research"].get("tavily_api_key")
        and st.secrets["research"].get("gemini_api_key")
    )

    # Map championships to their timing source (moved here so Import All can use it)
    _CHAMP_TIMING_SOURCE = {
        "BTCC": "tsl",  "British F4": "tsl", "GB3": "tsl",
        "British GT": "tsl", "Porsche Cup GB": "tsl",
        "Porsche Cup NA": "imsa",
        "Porsche Cup AU": "computime",
        "Porsche Sprint NA": "paste",
        "IndyNXT": "paste",
        "UAE F4": "paste", "Porsche Cup NZ": "paste", "DTM": "paste",
        "CTFROC (Formula Regional Oceania)": "paste",
        "GR86 Championship NZ": "paste",
        "Summerset GT NZ": "paste",
        "GTRNZ": "paste",
        "TA2 NZ": "paste",
        "NZ Formula Ford": "paste",
    }

    with st.expander("🔬 **Research New Championship**", expanded=False):
        if not _has_research_keys:
            st.info(
                "🔑 To enable AI championship research, add your free API keys to `.streamlit/secrets.toml`:\n\n"
                "```toml\n[research]\ntavily_api_key = \"tvly-...\"\ngemini_api_key = \"AIza...\"\n```\n\n"
                "• **Tavily** (free 1,000 searches/mo): [tavily.com](https://tavily.com)\n"
                "• **Gemini** (free 15 RPM): [aistudio.google.com/apikey](https://aistudio.google.com/apikey)"
            )
        else:
            _rtab_url, _rtab_search = st.tabs(["🔗 From URL", "🔍 Search by Name"])

            # ── Tab 1: Import from URL ──
            with _rtab_url:
                st.caption("Paste a championship website URL → AI scrapes calendar, drivers & results.")
                _url_col1, _url_col2 = st.columns([3, 1])
                with _url_col1:
                    _research_url = st.text_input(
                        "Championship URL",
                        placeholder="e.g. https://www.toyota.co.nz/toyota-racing/castrol-toyota-fr-oceania/",
                        key="research_champ_url",
                        label_visibility="collapsed",
                    )
                with _url_col2:
                    _do_url_research = st.button("🔗 Import", type="primary", use_container_width=True)

                if _do_url_research and _research_url:
                    _research_url = _research_url.strip()
                    if not _research_url.startswith("http"):
                        _research_url = "https://" + _research_url
                    from championship_researcher import ChampionshipResearcher
                    _researcher = ChampionshipResearcher(
                        tavily_api_key=st.secrets["research"]["tavily_api_key"],
                        gemini_api_key=st.secrets["research"]["gemini_api_key"],
                    )
                    _progress_bar = st.progress(0)
                    _status_text = st.empty()
                    def _url_research_progress(step, msg):
                        _progress_bar.progress(min(step / 6, 1.0))
                        _status_text.caption(f"Step {step}/6: {msg}")
                    with st.spinner("Scraping championship website..."):
                        _research_data = _researcher.research_from_url(
                            _research_url, progress_callback=_url_research_progress
                        )
                    _progress_bar.progress(1.0)
                    _status_text.empty()
                    if "error" in _research_data:
                        st.error(f"Import failed: {_research_data['error']}")
                    else:
                        st.session_state['_research_result'] = _research_data
                        st.rerun()

            # ── Tab 2: Search by Name (existing) ──
            with _rtab_search:
                st.caption("Enter a championship name → AI searches the web → extracts drivers, calendar & results.")
                _research_col1, _research_col2 = st.columns([3, 1])
                with _research_col1:
                    _research_query = st.text_input(
                        "Championship to research",
                        placeholder="e.g. GR86 Championship NZ 2025-2026",
                        key="research_champ_query",
                        label_visibility="collapsed",
                    )
                with _research_col2:
                    _do_research = st.button("🔍 Research", type="primary", use_container_width=True)

                if _do_research and _research_query:
                    from championship_researcher import ChampionshipResearcher
                    _researcher = ChampionshipResearcher(
                        tavily_api_key=st.secrets["research"]["tavily_api_key"],
                        gemini_api_key=st.secrets["research"]["gemini_api_key"],
                    )
                    _progress_bar = st.progress(0)
                    _status_text = st.empty()
                    def _research_progress(step, msg):
                        _progress_bar.progress(min(step / 6, 1.0))
                        _status_text.caption(f"Step {step}/6: {msg}")
                    with st.spinner("Researching..."):
                        _research_data = _researcher.research(_research_query, progress_callback=_research_progress)
                    _progress_bar.progress(1.0)
                    _status_text.empty()
                    if "error" in _research_data:
                        st.error(f"Research failed: {_research_data['error']}")
                    else:
                        st.session_state['_research_result'] = _research_data
                        st.rerun()

            # ── Display Research Results (shared by both tabs) ──
            if '_research_result' in st.session_state:
                _rdata = st.session_state['_research_result']
                _champ_name = _rdata.get('championship_name', _rdata.get('query', ''))

                st.success(f"✅ **{_champ_name}** — {_rdata.get('season', '')}")

                # Sources
                _sources = _rdata.get('sources', [])
                if _sources:
                    _src_links = " · ".join([f"[{s['title'][:30]}]({s['url']})" for s in _sources[:5]])
                    st.caption(f"📚 Sources: {_src_links}")

                # ── 🚀 ONE-CLICK IMPORT: Calendar + Drivers + Auto-select ──
                _has_cal = bool(_rdata.get('calendar'))
                _has_drivers = bool(_rdata.get('drivers'))
                if _has_cal or _has_drivers:
                    _n_rounds = len([e for e in _rdata.get('calendar', []) if e.get('start_date') and e.get('end_date')])
                    _n_drivers = len(_rdata.get('drivers', []))
                    _import_label = "🚀 Import All & Start Outreach"
                    _import_parts = []
                    if _n_rounds: _import_parts.append(f"{_n_rounds} rounds")
                    if _n_drivers: _import_parts.append(f"{_n_drivers} drivers")
                    if _import_parts:
                        _import_label += f" ({', '.join(_import_parts)})"

                    if st.button(_import_label, key="research_import_all", type="primary", use_container_width=True):
                        _msgs = []
                        # 1. Add calendar
                        if _has_cal:
                            from championship_researcher import research_to_calendar_dict
                            _new_cal = research_to_calendar_dict(_rdata, color="#607D8B")
                            if _new_cal["rounds"]:
                                RACE_CALENDARS[_champ_name] = _new_cal
                                _sa = st.session_state.get('session_added_championships', [])
                                if _champ_name not in _sa:
                                    _sa.append(_champ_name)
                                    st.session_state.session_added_championships = _sa
                                _persist_championships()
                                _msgs.append(f"📅 {len(_new_cal['rounds'])} rounds added")
                        # 2. Queue drivers for outreach list (NOT pipeline)
                        # They'll appear in the driver list below, ready to search & message
                        if _has_drivers:
                            _driver_names = []
                            for d in _rdata.get('drivers', []):
                                _first = d.get('first_name', '').strip()
                                _last = d.get('last_name', '').strip()
                                if _first and _last:
                                    _driver_names.append(f"{_first} {_last}")
                            if _driver_names:
                                # Store names for the outreach list to pick up
                                st.session_state['_research_driver_names'] = _driver_names
                                st.session_state['_run_analysis_on_update'] = True
                                _msgs.append(f"🏎️ {len(_driver_names)} drivers queued for outreach")
                        # 3. Auto-select championship — reset widget keys so UI updates
                        st.session_state.global_championship = _champ_name
                        st.session_state.pop('_outreach_champ', None)      # Force selectbox to re-read from global_championship
                        st.session_state.pop('_outreach_round_idx', None)  # Let round selector auto-pick last finished
                        # Sync to chrome extension
                        try:
                            st.query_params['championship'] = _champ_name
                        except Exception:
                            pass
                        # 4. Find last finished event → auto-fill circuit
                        _last_fin = _get_last_finished_round(_champ_name, _today)
                        if _last_fin:
                            _last_rd, _days = _last_fin
                            _circuit_name = _strip_flags(_last_rd['name'])
                            _persist_circuit(_circuit_name)
                            st.session_state['event_name_input'] = f"{_circuit_name} ({_format_round_dates(_last_rd)})"
                            _msgs.append(f"🏁 Circuit: **{_circuit_name}** ({_last_rd['round']}, {_days}d ago)")
                        elif _has_cal and RACE_CALENDARS.get(_champ_name, {}).get('rounds'):
                            _first_rd = RACE_CALENDARS[_champ_name]['rounds'][0]
                            _circuit_name = _strip_flags(_first_rd['name'])
                            _persist_circuit(_circuit_name)
                            st.session_state['event_name_input'] = f"{_circuit_name} ({_format_round_dates(_first_rd)})"
                            _msgs.append(f"🏁 Circuit: **{_circuit_name}** (next event)")

                        # 5. Auto-configure timing source if detected
                        _ts = _rdata.get('timing_source', {})
                        _ts_type = (_ts.get('type') or 'none').lower().strip()
                        if _ts_type and _ts_type != 'none':
                            _CHAMP_TIMING_SOURCE[_champ_name] = _ts_type
                            _ts_url = _ts.get('url', '')
                            _ts_labels = {
                                'speedhive': '🌐 Speedhive',
                                'tsl': '🇬🇧 TSL Timing',
                                'computime': '🕐 Computime',
                                'imsa': '🇺🇸 IMSA',
                                'natsoft': '🇦🇺 Natsoft',
                            }
                            _msgs.append(f"⏱️ Timing: **{_ts_labels.get(_ts_type, _ts_type)}**")
                            # If Speedhive, try to link the org for easy browsing
                            if _ts_type == 'speedhive' and _ts_url:
                                try:
                                    from speedhive_client import SpeedhiveClient
                                    _sh = SpeedhiveClient()
                                    _eid = _sh.extract_event_id(_ts_url)
                                    if _eid:
                                        _org = _sh.discover_org_from_event(int(_eid))
                                        if _org and _org.get('org_id'):
                                            _linked = st.session_state.get('sh_linked_orgs', {})
                                            _linked[_champ_name] = _org
                                            st.session_state.sh_linked_orgs = _linked
                                            if settings and settings.is_available:
                                                settings.set('speedhive_orgs', _linked)
                                            _msgs.append(f"🔗 Speedhive org linked")
                                except Exception as _sh_err:
                                    pass  # Non-critical — timing can still work via URL paste

                        st.success("✅ **All imported!** " + " · ".join(_msgs))
                        st.toast(f"🚀 {_champ_name} — ready for outreach!")
                        st.rerun()

                    st.divider()

                # ── Calendar ──
                _cal_events = _rdata.get('calendar', [])
                if _cal_events:
                    st.markdown("#### 📅 Calendar")
                    _cal_rows = []
                    for ev in _cal_events:
                        _cal_rows.append({
                            "Round": ev.get('round', ''),
                            "Venue": ev.get('venue') or ev.get('name', ''),
                            "Start": ev.get('start_date', ''),
                            "End": ev.get('end_date', ''),
                        })
                    import pandas as _pd_cal
                    st.dataframe(_pd_cal.DataFrame(_cal_rows), use_container_width=True, hide_index=True)

                    # Add to Calendar button (individual)
                    _cal_color = st.color_picker("Calendar colour", value="#607D8B", key="research_cal_color")
                    if st.button("📅 Add Calendar Only", key="research_add_cal"):
                        from championship_researcher import research_to_calendar_dict
                        _new_cal = research_to_calendar_dict(_rdata, color=_cal_color)
                        if _new_cal["rounds"]:
                            RACE_CALENDARS[_champ_name] = _new_cal
                            _sa = st.session_state.get('session_added_championships', [])
                            if _champ_name not in _sa:
                                _sa.append(_champ_name)
                                st.session_state.session_added_championships = _sa
                            _persist_championships()
                            st.success(f"✅ Added **{_champ_name}** ({len(_new_cal['rounds'])} rounds) to calendar!")
                            st.toast(f"📅 {_champ_name} added to calendar")
                        else:
                            st.warning("No valid dates found to add.")

                # ── Drivers ──
                _drivers = _rdata.get('drivers', [])
                if _drivers:
                    st.markdown(f"#### 🏎️ Drivers ({len(_drivers)})")
                    _drv_rows = []
                    for d in _drivers:
                        _drv_rows.append({
                            "#": d.get('number', ''),
                            "First Name": d.get('first_name', ''),
                            "Last Name": d.get('last_name', ''),
                            "Nationality": d.get('nationality', ''),
                            "Team": d.get('team', ''),
                        })
                    import pandas as _pd_drv
                    st.dataframe(_pd_drv.DataFrame(_drv_rows), use_container_width=True, hide_index=True)

                    # Send drivers to outreach list (individual button)
                    if st.button(f"⚡ Send {len(_drivers)} Drivers to Outreach", key="research_import_drivers"):
                        _driver_names = []
                        for d in _drivers:
                            _first = d.get('first_name', '').strip()
                            _last = d.get('last_name', '').strip()
                            if _first and _last:
                                _driver_names.append(f"{_first} {_last}")
                        if _driver_names:
                            st.session_state['_research_driver_names'] = _driver_names
                            st.session_state['_run_analysis_on_update'] = True
                            st.success(f"✅ {len(_driver_names)} drivers queued for outreach!")
                            st.toast(f"⚡ {len(_driver_names)} drivers → outreach list")
                            st.rerun()

                # ── Timing Source ──
                _ts_data = _rdata.get('timing_source', {})
                _ts_type_display = (_ts_data.get('type') or 'none').lower()
                if _ts_type_display and _ts_type_display != 'none':
                    _ts_label_map = {
                        'speedhive': '🌐 Speedhive (MYLAPS)',
                        'tsl': '🇬🇧 TSL Timing',
                        'computime': '🕐 Computime',
                        'imsa': '🇺🇸 IMSA / Al Kamel',
                        'natsoft': '🇦🇺 Natsoft',
                    }
                    _ts_url_display = _ts_data.get('url', '')
                    st.markdown(f"#### ⏱️ Timing Source: {_ts_label_map.get(_ts_type_display, _ts_type_display)}")
                    if _ts_url_display:
                        st.caption(f"[🔗 {_ts_url_display}]({_ts_url_display})")
                    st.info(f"✅ Timing API will be auto-configured when you click **Import All**")

                # ── Social Media ──
                _social_parts = []
                if _rdata.get('facebook'):
                    _social_parts.append(f"[📘 Facebook]({_rdata['facebook']})")
                if _rdata.get('instagram'):
                    _ig = _rdata['instagram']
                    if not _ig.startswith('http'):
                        _ig = f"https://instagram.com/{_ig.lstrip('@')}"
                    _social_parts.append(f"[📷 Instagram]({_ig})")
                if _rdata.get('website'):
                    _social_parts.append(f"[🌐 Website]({_rdata['website']})")
                if _social_parts:
                    st.markdown("#### 📱 Links")
                    st.markdown(" · ".join(_social_parts))

                # ── Results Summary ──
                _results_summary = _rdata.get('results_summary', '')
                if _results_summary:
                    st.markdown("#### 🏁 Results")
                    st.write(_results_summary)

                # Clear button
                if st.button("🗑️ Clear Research", key="research_clear"):
                    del st.session_state['_research_result']
                    st.rerun()

    # =====================================================================
    # STEP 1: SELECT CHAMPIONSHIP
    # =====================================================================

    # Sort: recently finished first, then alphabetical
    _champ_sort_key = {}
    for _cn in saved_champs:
        _fin = _get_last_finished_round(_cn, _today)
        if _fin:
            _champ_sort_key[_cn] = _fin[1]  # days ago (lower = more recent)
        else:
            _champ_sort_key[_cn] = 9999
    saved_champs = sorted(saved_champs, key=lambda x: (_champ_sort_key.get(x, 9999), x))

    col_champ, col_round = st.columns([1, 2])

    with col_champ:
        curr = st.session_state.global_championship
        opts = saved_champs + ["➕ Add New..."]
        idx = 0
        if curr in opts:
            idx = opts.index(curr)

        # Full display names for championships — shown in dropdown
        _CHAMP_DISPLAY_NAMES = {
            "BTCC": "British Touring Car Championship (BTCC)",
            "British F4": "British Formula 4 Championship",
            "GB3": "GB3 Championship",
            "British GT": "British GT Championship",
            "UAE F4": "UAE Formula 4 Championship",
            "Porsche Cup GB": "Porsche Carrera Cup Great Britain",
            "Porsche Cup NA": "Porsche Carrera Cup North America",
            "Porsche Cup AU": "Porsche Carrera Cup Australia",
            "Porsche Cup NZ": "Porsche Carrera Cup New Zealand",
            "Porsche Sprint NA": "Porsche Sprint Challenge North America",
            "DTM": "Deutsche Tourenwagen Masters (DTM)",
            "IndyNXT": "IndyNXT Series (Indy Lights)",
            "GR86 Championship NZ": "Toyota GR86 Championship New Zealand",
            "CTFROC (Formula Regional Oceania)": "Castrol Toyota Formula Regional Oceania Trophy",
            "Summerset GT NZ": "Summerset GT Championship New Zealand",
            "GTRNZ": "GT Racing New Zealand (GTRNZ)",
            "TA2 NZ": "TA2 Muscle Car Series New Zealand",
            "NZ Formula Ford": "New Zealand Formula Ford Championship",
        }

        def _format_champ_label(val):
            if val == "➕ Add New...":
                return val
            _display = _CHAMP_DISPLAY_NAMES.get(val, val)
            _fin = _get_last_finished_round(val, _today)
            if _fin:
                _rd, _days = _fin
                if _days <= 3:
                    return f"🔥 {_display}"
                elif _days <= 14:
                    return f"📅 {_display}"
            return _display

        def _on_champ_change():
            val = st.session_state._outreach_champ
            if val and val != "➕ Add New...":
                st.session_state.global_championship = val
                # Reset round selection when championship changes
                st.session_state.pop('_outreach_round_idx', None)

        selected_champ = st.selectbox(
            "🏆 Championship",
            options=opts,
            index=idx,
            format_func=_format_champ_label,
            key="_outreach_champ",
            on_change=_on_champ_change,
            help="Select a championship. 🔥 = race finished in last 3 days."
        )

        if selected_champ == "➕ Add New...":
            def save_new_champ_callback():
                val = st.session_state.new_champ_entry_seamless
                if val:
                    val = val.strip()
                    st.session_state.global_championship = val
                    session_added = st.session_state.get('session_added_championships', [])
                    if val not in session_added:
                        session_added.append(val)
                        st.session_state.session_added_championships = session_added
                    _persist_championships()
                    st.toast(f"Added & Set: {val}")
            st.text_input(
                "Name of New Championship",
                placeholder="Type & Press Enter...",
                key="new_champ_entry_seamless",
                on_change=save_new_champ_callback,
                label_visibility="collapsed"
            )

        # Show timing source for selected championship
        if selected_champ and selected_champ != "➕ Add New...":
            _source = _CHAMP_TIMING_SOURCE.get(selected_champ, "speedhive")
            _source_labels = {
                "tsl": "🇬🇧 TSL Timing",
                "imsa": "🇺🇸 IMSA / Al Kamel",
                "computime": "🕐 Computime",
                "speedhive": "🌐 Speedhive",
                "paste": "📋 Manual Paste",
            }
            st.caption(f"Data source: **{_source_labels.get(_source, _source)}**")

    # =====================================================================
    # STEP 2: SELECT ROUND (from calendar)
    # =====================================================================
    with col_round:
        _cal_data = RACE_CALENDARS.get(selected_champ)
        if _cal_data and selected_champ != "➕ Add New...":
            rounds = _cal_data['rounds']

            # Build round options with status indicators
            _round_opts = []
            _default_idx = 0
            _best_finished_days = 9999
            for i, rd in enumerate(rounds):
                _re = datetime.strptime(rd['end'], "%Y-%m-%d").date()
                _rs = datetime.strptime(rd['start'], "%Y-%m-%d").date()
                _circ = _strip_flags(rd['name'])
                _dates = _format_round_dates(rd)

                if _re < _today:
                    _days_ago = (_today - _re).days
                    if _days_ago <= 3:
                        _label = f"🔥 {rd['round']} — {_circ} ({_dates}) — {_days_ago}d ago"
                    else:
                        _label = f"✅ {rd['round']} — {_circ} ({_dates})"
                    if _days_ago < _best_finished_days:
                        _best_finished_days = _days_ago
                        _default_idx = i
                elif _rs <= _today <= _re:
                    _label = f"🏁 {rd['round']} — {_circ} ({_dates}) — LIVE NOW"
                    _default_idx = i
                else:
                    _label = f"⏳ {rd['round']} — {_circ} ({_dates})"

                _round_opts.append(_label)

            # Use stored index if available (user manually changed it)
            _stored_idx = st.session_state.get('_outreach_round_idx', _default_idx)
            try:
                _stored_idx = int(_stored_idx)
            except (TypeError, ValueError):
                _stored_idx = _default_idx
            if _stored_idx < 0 or _stored_idx >= len(_round_opts):
                _stored_idx = _default_idx

            def _on_round_change():
                st.session_state['_outreach_round_idx'] = st.session_state._outreach_round_sel

            _sel_round_idx = st.selectbox(
                f"📅 {_CHAMP_DISPLAY_NAMES.get(selected_champ, selected_champ)} — Select Round",
                options=list(range(len(_round_opts))),
                index=_stored_idx,
                format_func=lambda i: _round_opts[i],
                key="_outreach_round_sel",
                on_change=_on_round_change,
                help="Rounds with 🔥 just finished — ready for outreach."
            )

            # Set circuit + dates from selected round
            _selected_round = rounds[_sel_round_idx]
            _circ_name = _strip_flags(_selected_round['name'])
            _circ_dates = _format_round_dates(_selected_round)
            event_name_input = f"{_circ_name} ({_circ_dates})"
            st.session_state['event_name_input'] = event_name_input
            st.session_state.global_championship = selected_champ
        else:
            # Non-calendar championship — manual circuit entry
            event_name_input = st.text_input(
                "Circuit / Event Name",
                placeholder="e.g. Donington Park (18-20 Apr)",
                help="Type circuit name with dates.",
                key="event_name_input"
            )

    # ── Outreach Mode Selector ──
    # Auto-detect: if ALL rounds of the selected championship have finished → default to End of Season
    _season_over = False
    if selected_champ and selected_champ != "➕ Add New...":
        _cal = RACE_CALENDARS.get(selected_champ, {})
        if _cal and _cal.get('rounds'):
            _all_finished = all(
                datetime.strptime(rd['end'], "%Y-%m-%d").date() < _today
                for rd in _cal['rounds']
            )
            _season_over = _all_finished

    # Set default mode if not already set by user
    if '_outreach_mode' not in st.session_state and _season_over:
        st.session_state['_outreach_mode'] = "🏆 End of Season"

    _mode_col1, _mode_col2 = st.columns([3, 1])
    with _mode_col1:
        if _season_over:
            st.caption("🏆 Season complete — End of Season outreach enabled")
    with _mode_col2:
        _outreach_mode = st.radio(
            "Outreach Mode",
            options=["🏁 Race Weekend", "🏆 End of Season"],
            key="_outreach_mode",
            horizontal=True,
            label_visibility="collapsed",
        )
    _is_end_of_season = _outreach_mode == "🏆 End of Season"

    # ── Show selected event summary ──
    if event_name_input:
        event_name = event_name_input
    else:
        event_name = "the circuit"

    if selected_champ and selected_champ != "➕ Add New...":
        if _is_end_of_season:
            st.markdown(
                f'<div style="background:linear-gradient(135deg,#1a1a2e,#2d1b4e);padding:12px 20px;'
                f'border-radius:10px;margin:8px 0;border-left:4px solid #f59e0b;">'
                f'<span style="font-size:18px;font-weight:700;color:#fbbf24;">'
                f'🏆 End of Season Outreach</span>'
                f'<span style="color:#d4a0ff;margin-left:12px;font-size:15px;">{_CHAMP_DISPLAY_NAMES.get(selected_champ, selected_champ)}</span>'
                f'</div>',
                unsafe_allow_html=True
            )
        elif event_name_input:
            st.markdown(
                f'<div style="background:linear-gradient(135deg,#1a1a2e,#16213e);padding:12px 20px;'
                f'border-radius:10px;margin:8px 0;border-left:4px solid '
                f'{RACE_CALENDARS.get(selected_champ, {}).get("color", "#4CAF50")};">'
                f'<span style="font-size:18px;font-weight:700;color:#fff;">'
                f'🏁 {_CHAMP_DISPLAY_NAMES.get(selected_champ, selected_champ)}</span>'
                f'<span style="color:#aaa;margin-left:12px;font-size:15px;">{event_name}</span>'
                f'</div>',
                unsafe_allow_html=True
            )

    # ── Chrome extension sync helper ──
    def _render_ext_sync_div():
        """Inject hidden div for Chrome extension to read circuit/champ/AI message/outreach mode."""
        import json as _json_mod
        _c = (event_name_input or "").replace('"', '&quot;')
        _ch_key = st.session_state.get('global_championship', '')
        _ch = _CHAMP_DISPLAY_NAMES.get(_ch_key, _ch_key).replace('"', '&quot;')
        _ai = st.session_state.get('_ext_ai_outreach_msg', '').replace('"', '&quot;').replace('\n', '\\n')
        _ai_dict = st.session_state.get('_ext_ai_messages', {})
        _ai_json = _json_mod.dumps(_ai_dict).replace('&', '&amp;').replace('"', '&quot;').replace('<', '&lt;')
        _d = st.session_state.get('_ext_current_driver', '').replace('"', '&quot;')
        _mode = "end_of_season" if _is_end_of_season else "race_weekend"
        st.markdown(
            f'<div id="ag-active-circuit" data-circuit="{_c}" data-champ="{_ch}" '
            f'data-driver="{_d}" data-outreach-mode="{_mode}" '
            f'data-ai-msg="{_ai}" data-ai-messages="{_ai_json}" style="display:none;"></div>',
            unsafe_allow_html=True
        )

    # =====================================================================
    # STEP 3: DATA SOURCE — automatic based on championship
    # =====================================================================
    _auto_source = _CHAMP_TIMING_SOURCE.get(selected_champ, "speedhive") if selected_champ != "➕ Add New..." else "paste"

    _source_to_method = {
        "tsl": "🇬🇧 Import from TSL Timing",
        "imsa": "🇺🇸 Import from IMSA",
        "computime": "🕐 Import from Computime",
        "speedhive": "🌐 Import from Speedhive",
        "paste": "Paste Text",
    }

    # Default: use auto-detected source
    input_method = _source_to_method.get(_auto_source, "Paste Text")

    # Allow user to override with manual paste
    _col_source, _col_paste = st.columns([3, 1])
    with _col_source:
        _source_labels = {
            "tsl": "🇬🇧 TSL Timing — paste event URL below",
            "imsa": "🇺🇸 IMSA (Al Kamel) — paste event URL below",
            "computime": "🕐 Computime — paste meeting URL below",
            "speedhive": "🌐 Speedhive — browse or paste event URL below",
            "paste": "📋 Paste driver names manually",
        }
        st.caption(_source_labels.get(_auto_source, "📋 Paste driver names"))
    with _col_paste:
        if _auto_source != "paste":
            if st.checkbox("📋 Manual paste", key="_use_manual_paste", help="Override auto-import and paste names manually"):
                input_method = "Paste Text"

    raw_results_list = []
    _speedhive_driver_results = {}  # name -> list of session results (populated by Speedhive import)
    _speedhive_event = None

    # If research queued driver names → inject them into the outreach list
    if '_research_driver_names' in st.session_state:
        raw_results_list = st.session_state.pop('_research_driver_names')
        input_method = "Paste Text"  # Skip timing API UI — we already have names

    if input_method == "🌐 Import from Speedhive":
        try:
            from speedhive_client import SpeedhiveClient, KNOWN_ORGS
            _sh_import_ok = True
        except ImportError as _imp_err:
            st.error(f"Speedhive client not available: {_imp_err}")
            SpeedhiveClient = None
            KNOWN_ORGS = {}
            _sh_import_ok = False

        if _sh_import_ok:
            _sh_client = SpeedhiveClient()

            # Load linked orgs from settings (Airtable-persisted) + built-in KNOWN_ORGS
            if 'sh_linked_orgs' not in st.session_state:
                _stored_orgs = {}
                if settings and settings.is_available:
                    _stored_orgs = settings.get('speedhive_orgs', {}) or {}
                # Merge with built-in defaults (stored takes priority)
                _merged = dict(KNOWN_ORGS)
                _merged.update(_stored_orgs)
                st.session_state.sh_linked_orgs = _merged
            _linked_orgs = st.session_state.sh_linked_orgs

            # --- UI: Two modes — Browse linked championship OR paste a URL ---
            _sh_mode_options = list(_linked_orgs.keys()) + ["📋 Paste URL / Link New Championship"]
            _sh_mode = st.selectbox(
                "🏁 Championship",
                options=_sh_mode_options,
                key="sh_champ_select",
                help="Select a linked championship to browse events, or paste a Speedhive URL"
            )

            _sh_event_id = None
            _sh_event = None
            _sh_sessions = []

            if _sh_mode == "📋 Paste URL / Link New Championship":
                # --- Fallback: paste URL (also used to link new championships) ---
                _sh_url = st.text_input(
                    "Speedhive Event URL or ID",
                    placeholder="https://speedhive.mylaps.com/events/3347675",
                    help="Paste any Speedhive event URL. We'll auto-detect the championship and link it for next time.",
                    key="speedhive_url_input"
                )
                if _sh_url:
                    _sh_event_id = _sh_client.extract_event_id(_sh_url)
                    if not _sh_event_id:
                        st.error("Could not extract event ID from that URL.")

                    # Discover and offer to link the organization
                    if _sh_event_id:
                        _disc_key = f"sh_discovered_org_{_sh_event_id}"
                        if _disc_key not in st.session_state:
                            with st.spinner("Discovering championship..."):
                                _disc = _sh_client.discover_org_from_event(_sh_event_id)
                                st.session_state[_disc_key] = _disc
                        _disc = st.session_state[_disc_key]
                        if _disc:
                            _org_name = _disc['name']
                            _already_linked = any(v.get('org_id') == _disc['org_id'] for v in _linked_orgs.values())
                            if _already_linked:
                                st.info(f"✅ **{_org_name}** is already linked. Select it from the dropdown above next time!")
                            else:
                                # Offer to link with a short name
                                st.info(f"🔗 Found: **{_org_name}** ({_disc.get('city', '')}, {_disc.get('country', '')})")
                                _link_name = st.text_input(
                                    "Save as (short name for dropdown)",
                                    value=_org_name.split()[0] if _org_name else "",
                                    key="sh_link_name",
                                    help="E.g. 'BTCC', 'British F4', 'GB3' — whatever you'll recognise"
                                )
                                if st.button("🔗 Link Championship", key="sh_link_btn"):
                                    if _link_name:
                                        _linked_orgs[_link_name] = {
                                            "org_id": _disc['org_id'],
                                            "name": _org_name,
                                            "sport": _disc.get('sport', 'Car')
                                        }
                                        st.session_state.sh_linked_orgs = _linked_orgs
                                        # Persist to Airtable
                                        if settings and settings.is_available:
                                            settings.set('speedhive_orgs', _linked_orgs)
                                        st.success(f"✅ **{_link_name}** linked! It will appear in the dropdown from now on.")
                                        st.rerun()
                else:
                    st.caption("⬆️ Paste a Speedhive event URL to link a new championship")

            else:
                # --- Browse mode: show recent events for this championship ---
                _org_info = _linked_orgs.get(_sh_mode, {})
                _org_id = _org_info.get('org_id')

                if _org_id:
                    _events_key = f"sh_org_events_{_org_id}"
                    if _events_key not in st.session_state:
                        with st.spinner(f"Loading {_sh_mode} events..."):
                            _org_events = _sh_client.fetch_organization_events(_org_id)
                            st.session_state[_events_key] = _org_events
                    _org_events = st.session_state[_events_key]

                    if not _org_events:
                        st.warning(f"No events found for {_sh_mode} on Speedhive.")
                    else:
                        # Show recent events as a selectbox (most recent first, already sorted)
                        _recent_events = _org_events[:15]  # Show last 15 events
                        _event_options = {
                            e['id']: f"{e['startDate']}  —  {e['name']}"
                            for e in _recent_events
                        }
                        _selected_event_id = st.selectbox(
                            "📅 Select Event",
                            options=list(_event_options.keys()),
                            format_func=lambda x: _event_options[x],
                            key="sh_event_select",
                            help="Most recent events shown first"
                        )
                        if _selected_event_id:
                            _sh_event_id = _selected_event_id

            # --- Fetch sessions for selected event (shared by both modes) ---
            if _sh_event_id:
                try:
                    _sh_cache_key = f"sh_sessions_{_sh_event_id}"
                    if _sh_cache_key not in st.session_state:
                        with st.spinner("Fetching sessions from Speedhive..."):
                            _sh_event = _sh_client.fetch_event(_sh_event_id)
                            _sh_sessions = _sh_client.fetch_sessions(_sh_event_id)
                            if _sh_event is None:
                                st.error(f"Could not reach Speedhive API for event {_sh_event_id}. The service may be down.")
                            st.session_state[_sh_cache_key] = (_sh_event, _sh_sessions)
                    else:
                        _sh_event, _sh_sessions = st.session_state[_sh_cache_key]
                except Exception as _sh_err:
                    st.error(f"Error fetching from Speedhive: {_sh_err}")
                    _sh_sessions = []
                    _sh_event = None

                if not _sh_sessions:
                    if _sh_event_id:
                        st.warning("No sessions found for this event.")
                else:
                    if _sh_event:
                        st.success(f"**{_sh_event['name']}** — {_sh_event.get('location', {}).get('name', 'Unknown venue')}")

                    # Group sessions by type for easy selection
                    _race_sessions = [s for s in _sh_sessions if s['type'] == 'race']
                    _qual_sessions = [s for s in _sh_sessions if s['type'] == 'qualify']
                    _practice_sessions = [s for s in _sh_sessions if s['type'] == 'practice']

                    st.caption(f"Found {len(_race_sessions)} races, {len(_qual_sessions)} qualifying, {len(_practice_sessions)} practice sessions")

                    # Session selector with checkboxes
                    _sh_selected = []

                    if _race_sessions:
                        _sel_races = st.multiselect(
                            "🏁 Race Sessions",
                            options=[s['id'] for s in _race_sessions],
                            format_func=lambda x: next((s['group'] for s in _race_sessions if s['id'] == x), str(x)),
                            default=[s['id'] for s in _race_sessions],  # All races selected by default
                            key="sh_race_select"
                        )
                        _sh_selected.extend(_sel_races)

                    if _qual_sessions:
                        _sel_quals = st.multiselect(
                            "⏱️ Qualifying Sessions",
                            options=[s['id'] for s in _qual_sessions],
                            format_func=lambda x: next((s['group'] for s in _qual_sessions if s['id'] == x), str(x)),
                            default=[],  # No qualifying by default
                            key="sh_qual_select"
                        )
                        _sh_selected.extend(_sel_quals)

                    if _sh_selected:
                        # Fetch results for selected sessions (cached)
                        _sh_results_key = f"sh_results_{_sh_event_id}_{'_'.join(map(str, sorted(_sh_selected)))}"
                        if _sh_results_key not in st.session_state:
                            with st.spinner(f"Fetching results for {len(_sh_selected)} sessions..."):
                                _all_names = set()
                                _driver_map = {}
                                for sid in _sh_selected:
                                    _cls = _sh_client.fetch_session_results(sid)
                                    if _cls and _cls.get('rows'):
                                        _sinfo = next((s for s in _sh_sessions if s['id'] == sid), {})
                                        for row in _cls['rows']:
                                            _rname = row.get('name', '').strip()
                                            if not _rname or _rname.startswith('-'):
                                                continue
                                            _all_names.add(_rname)
                                            if _rname not in _driver_map:
                                                _driver_map[_rname] = []
                                            _driver_map[_rname].append({
                                                'session_name': _sinfo.get('name', ''),
                                                'session_type': _sinfo.get('type', ''),
                                                'session_group': _sinfo.get('group', ''),
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
                                st.session_state[_sh_results_key] = (sorted(_all_names), _driver_map)

                        _sh_names, _sh_driver_map = st.session_state[_sh_results_key]
                        raw_results_list = _sh_names
                        _speedhive_driver_results = _sh_driver_map
                        _speedhive_event = _sh_event
                        st.session_state['uploaded_timing_names'] = _sh_names
                        st.success(f"✅ {len(_sh_names)} drivers ready to analyze from Speedhive!")

                        # Show breakdown by race category/class
                        _class_counts = {}
                        for _rname, _rdata in _sh_driver_map.items():
                            _classes = set()
                            for _sd in _rdata:
                                _rc = _sd.get('result_class', '').strip()
                                _sg = _sd.get('session_group', '').strip()
                                _cls_label = _rc or _sg or 'Unknown'
                                _classes.add(_cls_label)
                            for _c in _classes:
                                _class_counts[_c] = _class_counts.get(_c, 0) + 1
                        if _class_counts:
                            _class_parts = [f"**{cls}** ({cnt})" for cls, cnt in sorted(_class_counts.items(), key=lambda x: -x[1])]
                            st.caption("📋 Categories: " + " · ".join(_class_parts))

                            # Class filter — work through one class at a time
                            _class_options = ["All Classes"] + sorted(_class_counts.keys(), key=lambda x: _class_counts[x], reverse=True)
                            _selected_class = st.selectbox(
                                "🏷️ Filter by class",
                                options=_class_options,
                                key="sh_class_filter",
                                help="Work through one class at a time, then switch to the next"
                            )

                            if _selected_class and _selected_class != "All Classes":
                                # Filter raw_results_list to only drivers in the selected class
                                _filtered_names = []
                                for _rname in _sh_names:
                                    if _rname in _sh_driver_map:
                                        _driver_classes = set()
                                        for _sd in _sh_driver_map[_rname]:
                                            _rc = _sd.get('result_class', '').strip()
                                            _sg = _sd.get('session_group', '').strip()
                                            _driver_classes.add(_rc or _sg or 'Unknown')
                                        if _selected_class in _driver_classes:
                                            _filtered_names.append(_rname)
                                raw_results_list = _filtered_names
                                st.session_state['uploaded_timing_names'] = _filtered_names
                                st.info(f"🏷️ Showing **{len(_filtered_names)}** drivers in **{_selected_class}**")
    elif input_method == "🕐 Import from Computime":
        try:
            from computime_client import ComputimeClient
            _ct_import_ok = True
        except ImportError as _ct_err:
            st.error(f"Computime client not available: {_ct_err}")
            _ct_import_ok = False

        if _ct_import_ok:
            _ct_url = st.text_input(
                "Computime Results URL or MeetID",
                placeholder="https://www.computime.com.au/.../Resultspage?MeetID=17437",
                help="Paste any Computime results page URL. We'll auto-download and parse the PDF timing sheets.",
                key="computime_url_input"
            )

            _ct_meet_id = ComputimeClient.extract_meet_id(_ct_url) if _ct_url else None

            if _ct_meet_id:
                _ct_client = ComputimeClient()

                # Cache session list
                _ct_sessions_key = f"ct_sessions_{_ct_meet_id}"
                if _ct_sessions_key not in st.session_state:
                    with st.spinner("Fetching session list from Computime..."):
                        try:
                            _ct_sessions = _ct_client.get_sessions(_ct_meet_id)
                            st.session_state[_ct_sessions_key] = _ct_sessions
                        except Exception as _ct_err:
                            st.error(f"Could not reach Computime: {_ct_err}")
                            _ct_sessions = []
                            st.session_state[_ct_sessions_key] = []

                _ct_sessions = st.session_state[_ct_sessions_key]

                if _ct_sessions:
                    _ct_race_sessions = [s for s in _ct_sessions if s['type'] == 'race']
                    _ct_qual_sessions = [s for s in _ct_sessions if s['type'] == 'qualify']
                    _ct_practice_sessions = [s for s in _ct_sessions if s['type'] == 'practice']

                    st.caption(f"Found {len(_ct_race_sessions)} races, {len(_ct_qual_sessions)} qualifying, {len(_ct_practice_sessions)} practice sessions")

                    _ct_selected = []

                    if _ct_race_sessions:
                        _ct_sel_races = st.multiselect(
                            "🏁 Race Sessions",
                            options=[s['id'] for s in _ct_race_sessions],
                            format_func=lambda x: next((s['name'] for s in _ct_race_sessions if s['id'] == x), str(x)),
                            default=[s['id'] for s in _ct_race_sessions],
                            key="ct_race_select"
                        )
                        _ct_selected.extend(_ct_sel_races)

                    if _ct_qual_sessions:
                        _ct_sel_quals = st.multiselect(
                            "⏱️ Qualifying Sessions",
                            options=[s['id'] for s in _ct_qual_sessions],
                            format_func=lambda x: next((s['name'] for s in _ct_qual_sessions if s['id'] == x), str(x)),
                            default=[],
                            key="ct_qual_select"
                        )
                        _ct_selected.extend(_ct_sel_quals)

                    if _ct_selected:
                        _ct_results_key = f"ct_results_{_ct_meet_id}_{'_'.join(sorted(_ct_selected))}"
                        if _ct_results_key not in st.session_state:
                            with st.spinner(f"Downloading & parsing {len(_ct_selected)} timing sheets..."):
                                _ct_event, _ct_names, _ct_driver_map = _ct_client.extract_driver_results(
                                    _ct_meet_id,
                                    selected_sessions=_ct_selected
                                )
                                st.session_state[_ct_results_key] = (_ct_event, _ct_names, _ct_driver_map)

                        _ct_event, _ct_names, _ct_driver_map = st.session_state[_ct_results_key]
                        raw_results_list = _ct_names
                        _speedhive_driver_results = _ct_driver_map  # Same format — reuse pipeline
                        _speedhive_event = _ct_event
                        st.session_state['uploaded_timing_names'] = _ct_names
                        st.success(f"✅ {len(_ct_names)} drivers ready to analyze from Computime!")

                        if _ct_event.get('name'):
                            st.info(f"**{_ct_event['name']}** — {_ct_event.get('date', '')}")

                        # Show breakdown by class
                        _ct_class_counts = {}
                        for _rname, _rdata in _ct_driver_map.items():
                            _classes = set()
                            for _sd in _rdata:
                                _rc = _sd.get('result_class', '').strip()
                                if _rc:
                                    _classes.add(_rc)
                            for _c in _classes:
                                _ct_class_counts[_c] = _ct_class_counts.get(_c, 0) + 1
                        if _ct_class_counts:
                            _ct_class_parts = [f"**{cls}** ({cnt})" for cls, cnt in sorted(_ct_class_counts.items(), key=lambda x: -x[1])]
                            st.caption("📋 Categories: " + " · ".join(_ct_class_parts))

                            _ct_class_options = ["All Classes"] + sorted(_ct_class_counts.keys(), key=lambda x: _ct_class_counts[x], reverse=True)
                            _ct_selected_class = st.selectbox(
                                "🏷️ Filter by class",
                                options=_ct_class_options,
                                key="ct_class_filter",
                                help="Work through one class at a time"
                            )

                            if _ct_selected_class and _ct_selected_class != "All Classes":
                                _ct_filtered = [n for n in _ct_names if any(
                                    _sd.get('result_class', '').strip() == _ct_selected_class
                                    for _sd in _ct_driver_map.get(n, [])
                                )]
                                raw_results_list = _ct_filtered
                                st.session_state['uploaded_timing_names'] = _ct_filtered
                                st.info(f"🏷️ Showing **{len(_ct_filtered)}** drivers in **{_ct_selected_class}**")
                elif _ct_meet_id:
                    st.warning("No sessions found for this meeting.")

    elif input_method == "🇬🇧 Import from TSL Timing":
        try:
            from tsl_timing_client import TSLTimingClient
            _tsl_import_ok = True
        except ImportError as _tsl_err:
            st.error(f"TSL Timing client not available: {_tsl_err}")
            _tsl_import_ok = False

        if _tsl_import_ok:
            st.caption("TSL Timing covers: **BTCC** · **British F4** · **GB3** · **British GT** · **Porsche Carrera Cup GB**")
            # Quick-link helper for finding event pages
            _tsl_champ_pages = {
                "BTCC": "https://www.tsl-timing.com/btcc",
                "British F4": "https://www.tsl-timing.com/f4",
                "GB3": "https://www.tsl-timing.com/gb3",
                "British GT": "https://www.tsl-timing.com/bgt",
                "Porsche Cup GB": "https://www.tsl-timing.com/btcc",
            }
            _tsl_url = st.text_input(
                "TSL Timing Event URL or ID",
                placeholder="https://www.tsl-timing.com/event/261801",
                help="Browse events at tsl-timing.com → click an event → copy the URL. Covers BTCC, F4, GB3, British GT, Porsche Cup GB.",
                key="tsl_url_input"
            )

            _tsl_event_id = TSLTimingClient.extract_event_id(_tsl_url) if _tsl_url else None

            if _tsl_event_id:
                _tsl_client = TSLTimingClient()

                # Cache session list
                _tsl_sessions_key = f"tsl_sessions_{_tsl_event_id}"
                if _tsl_sessions_key not in st.session_state:
                    with st.spinner("Fetching sessions from TSL Timing..."):
                        try:
                            _tsl_title, _tsl_sessions = _tsl_client.get_sessions(_tsl_event_id)
                            st.session_state[_tsl_sessions_key] = (_tsl_title, _tsl_sessions)
                        except Exception as _tsl_err:
                            st.error(f"Could not reach TSL Timing: {_tsl_err}")
                            st.session_state[_tsl_sessions_key] = ('', [])

                _tsl_title, _tsl_sessions = st.session_state[_tsl_sessions_key]

                if _tsl_sessions:
                    if _tsl_title:
                        st.success(f"**{_tsl_title}**")

                    _tsl_race_sessions = [s for s in _tsl_sessions if s['type'] == 'race']
                    _tsl_qual_sessions = [s for s in _tsl_sessions if s['type'] == 'qualify']

                    # Group races by class for cleaner display
                    _tsl_classes = sorted(set(s['class_name'] for s in _tsl_race_sessions))
                    st.caption(f"Found {len(_tsl_race_sessions)} races across {len(_tsl_classes)} classes")

                    _tsl_selected = []

                    if _tsl_race_sessions:
                        _tsl_sel_races = st.multiselect(
                            "🏁 Race Sessions",
                            options=[s['id'] for s in _tsl_race_sessions],
                            format_func=lambda x: next(
                                (f"{s['class_name'][:30]} — {s['name']}" for s in _tsl_race_sessions if s['id'] == x),
                                str(x)
                            ),
                            default=[s['id'] for s in _tsl_race_sessions],
                            key="tsl_race_select"
                        )
                        _tsl_selected.extend(_tsl_sel_races)

                    if _tsl_selected:
                        _tsl_results_key = f"tsl_results_{_tsl_event_id}_{'_'.join(sorted(_tsl_selected))}"
                        if _tsl_results_key not in st.session_state:
                            with st.spinner(f"Downloading & parsing {len(_tsl_selected)} timing sheets..."):
                                _tsl_event, _tsl_names, _tsl_driver_map = _tsl_client.extract_driver_results(
                                    _tsl_event_id,
                                    selected_sessions=_tsl_selected
                                )
                                st.session_state[_tsl_results_key] = (_tsl_event, _tsl_names, _tsl_driver_map)

                        _tsl_event, _tsl_names, _tsl_driver_map = st.session_state[_tsl_results_key]
                        raw_results_list = _tsl_names
                        _speedhive_driver_results = _tsl_driver_map
                        _speedhive_event = _tsl_event
                        st.session_state['uploaded_timing_names'] = _tsl_names
                        st.success(f"✅ {len(_tsl_names)} drivers ready to analyze from TSL Timing!")

                        # Class filter
                        _tsl_class_counts = {}
                        for _rname, _rdata in _tsl_driver_map.items():
                            _classes = set()
                            for _sd in _rdata:
                                _rc = _sd.get('result_class', '').strip()
                                if _rc:
                                    _classes.add(_rc)
                            for _c in _classes:
                                _tsl_class_counts[_c] = _tsl_class_counts.get(_c, 0) + 1
                        if _tsl_class_counts:
                            _tsl_class_parts = [f"**{cls}** ({cnt})" for cls, cnt in sorted(_tsl_class_counts.items(), key=lambda x: -x[1])]
                            st.caption("📋 Categories: " + " · ".join(_tsl_class_parts))

                            _tsl_class_options = ["All Classes"] + sorted(_tsl_class_counts.keys(), key=lambda x: _tsl_class_counts[x], reverse=True)
                            _tsl_selected_class = st.selectbox(
                                "🏷️ Filter by class",
                                options=_tsl_class_options,
                                key="tsl_class_filter",
                                help="Work through one class at a time"
                            )

                            if _tsl_selected_class and _tsl_selected_class != "All Classes":
                                _tsl_filtered = [n for n in _tsl_names if any(
                                    _sd.get('result_class', '').strip() == _tsl_selected_class
                                    for _sd in _tsl_driver_map.get(n, [])
                                )]
                                raw_results_list = _tsl_filtered
                                st.session_state['uploaded_timing_names'] = _tsl_filtered
                                st.info(f"🏷️ Showing **{len(_tsl_filtered)}** drivers in **{_tsl_selected_class}**")
                elif _tsl_event_id:
                    st.warning("No sessions found for this event.")

    elif input_method == "🇺🇸 Import from IMSA":
        try:
            from imsa_client import IMSAClient
            _imsa_import_ok = True
        except ImportError as _imsa_err:
            st.error(f"IMSA client not available: {_imsa_err}")
            _imsa_import_ok = False

        if _imsa_import_ok:
            st.caption("IMSA covers: **Porsche Carrera Cup NA** · **IMSA WeatherTech** · **Mazda MX-5 Cup** · Free JSON results from [results.imsa.com](https://results.imsa.com)")

            _imsa_mode = st.radio(
                "Browse by",
                ["Event URL", "Browse 2026 Events"],
                key="imsa_browse_mode",
                horizontal=True
            )

            _imsa_client = IMSAClient()

            if _imsa_mode == "Event URL":
                _imsa_url = st.text_input(
                    "IMSA Event URL or Path",
                    placeholder="https://imsa.results.alkamelcloud.com/Results/25_2025/06_Sebring International Raceway/",
                    help="Paste any IMSA results event URL from results.imsa.com or imsa.results.alkamelcloud.com",
                    key="imsa_url_input"
                )
                _imsa_event_path = IMSAClient.extract_event_path(_imsa_url) if _imsa_url else None

            else:  # Browse events
                _imsa_year = st.selectbox("Year", [2026, 2025], key="imsa_year_select")
                _imsa_events_key = f"imsa_events_{_imsa_year}"
                if _imsa_events_key not in st.session_state:
                    with st.spinner(f"Loading {_imsa_year} IMSA events..."):
                        st.session_state[_imsa_events_key] = _imsa_client.list_events(_imsa_year)

                _imsa_events = st.session_state[_imsa_events_key]
                if _imsa_events:
                    _imsa_sel_event = st.selectbox(
                        "Event",
                        options=range(len(_imsa_events)),
                        format_func=lambda i: f"{_imsa_events[i]['index']}. {_imsa_events[i]['name']}",
                        key="imsa_event_select"
                    )
                    _imsa_event_path = _imsa_events[_imsa_sel_event]['path'] if _imsa_sel_event is not None else None
                else:
                    st.warning(f"No events found for {_imsa_year}")
                    _imsa_event_path = None

            if _imsa_event_path:
                # List series for the selected event
                _imsa_series_key = f"imsa_series_{_imsa_event_path}"
                if _imsa_series_key not in st.session_state:
                    with st.spinner("Loading series..."):
                        st.session_state[_imsa_series_key] = _imsa_client.list_series(_imsa_event_path)

                _imsa_series = st.session_state[_imsa_series_key]

                if _imsa_series:
                    _imsa_sel_series = st.multiselect(
                        "📊 Championships to import",
                        options=range(len(_imsa_series)),
                        format_func=lambda i: _imsa_series[i]['name'],
                        default=[i for i, s in enumerate(_imsa_series) if 'porsche' in s['name'].lower()],
                        key="imsa_series_select"
                    )

                    if _imsa_sel_series:
                        _imsa_series_filter = [_imsa_series[i]['name'] for i in _imsa_sel_series]

                        # Load sessions for selected series
                        _imsa_all_sessions = []
                        _imsa_sessions_cache_key = f"imsa_sessions_{_imsa_event_path}_{'_'.join(sorted(_imsa_series_filter))}"
                        if _imsa_sessions_cache_key not in st.session_state:
                            with st.spinner("Loading sessions..."):
                                for _sf in _imsa_series_filter:
                                    _sf_matches = [s for s in _imsa_series if s['name'] == _sf]
                                    if _sf_matches:
                                        _sf_sessions = _imsa_client.list_sessions(_sf_matches[0]['path'])
                                        for _ss in _sf_sessions:
                                            _ss['series_name'] = _sf
                                        _imsa_all_sessions.extend(_sf_sessions)
                                st.session_state[_imsa_sessions_cache_key] = _imsa_all_sessions
                        _imsa_all_sessions = st.session_state[_imsa_sessions_cache_key]

                        if _imsa_all_sessions:
                            _imsa_race_sessions = [s for s in _imsa_all_sessions if s['type'] == 'race']
                            _imsa_qual_sessions = [s for s in _imsa_all_sessions if s['type'] == 'qualify']

                            st.caption(f"Found {len(_imsa_race_sessions)} races, {len(_imsa_qual_sessions)} qualifying sessions")

                            _imsa_selected_paths = []

                            if _imsa_race_sessions:
                                _imsa_sel_races = st.multiselect(
                                    "🏁 Race Sessions",
                                    options=[s['path'] for s in _imsa_race_sessions],
                                    format_func=lambda x: next(
                                        (f"{s.get('series_name', '')[:25]} — {s['name']}" for s in _imsa_race_sessions if s['path'] == x),
                                        x
                                    ),
                                    default=[s['path'] for s in _imsa_race_sessions],
                                    key="imsa_race_select"
                                )
                                _imsa_selected_paths.extend(_imsa_sel_races)

                            if _imsa_qual_sessions:
                                _imsa_sel_quals = st.multiselect(
                                    "⏱️ Qualifying Sessions",
                                    options=[s['path'] for s in _imsa_qual_sessions],
                                    format_func=lambda x: next(
                                        (f"{s.get('series_name', '')[:25]} — {s['name']}" for s in _imsa_qual_sessions if s['path'] == x),
                                        x
                                    ),
                                    default=[],
                                    key="imsa_qual_select"
                                )
                                _imsa_selected_paths.extend(_imsa_sel_quals)

                            if _imsa_selected_paths:
                                _imsa_results_key = f"imsa_results_{_imsa_event_path}_{'_'.join(sorted(_imsa_selected_paths)[:3])}"
                                if _imsa_results_key not in st.session_state:
                                    with st.spinner(f"Fetching {len(_imsa_selected_paths)} IMSA result files..."):
                                        _imsa_event, _imsa_names, _imsa_driver_map = _imsa_client.extract_driver_results(
                                            _imsa_event_path,
                                            selected_sessions=_imsa_selected_paths,
                                            series_filter=_imsa_series_filter
                                        )
                                        st.session_state[_imsa_results_key] = (_imsa_event, _imsa_names, _imsa_driver_map)

                                _imsa_event, _imsa_names, _imsa_driver_map = st.session_state[_imsa_results_key]
                                raw_results_list = _imsa_names
                                _speedhive_driver_results = _imsa_driver_map
                                _speedhive_event = _imsa_event
                                st.session_state['uploaded_timing_names'] = _imsa_names
                                st.success(f"✅ {len(_imsa_names)} drivers ready to analyze from IMSA!")

                                if _imsa_event.get('name'):
                                    st.info(f"**{_imsa_event['name']}**")

                                # Class filter
                                _imsa_class_counts = {}
                                for _rname, _rdata in _imsa_driver_map.items():
                                    _classes = set()
                                    for _sd in _rdata:
                                        _rc = _sd.get('result_class', '').strip()
                                        if _rc:
                                            _classes.add(_rc)
                                    for _c in _classes:
                                        _imsa_class_counts[_c] = _imsa_class_counts.get(_c, 0) + 1
                                if _imsa_class_counts:
                                    _imsa_class_parts = [f"**{cls}** ({cnt})" for cls, cnt in sorted(_imsa_class_counts.items(), key=lambda x: -x[1])]
                                    st.caption("📋 Categories: " + " · ".join(_imsa_class_parts))

                                    _imsa_class_options = ["All Classes"] + sorted(_imsa_class_counts.keys(), key=lambda x: _imsa_class_counts[x], reverse=True)
                                    _imsa_selected_class = st.selectbox(
                                        "🏷️ Filter by class",
                                        options=_imsa_class_options,
                                        key="imsa_class_filter",
                                        help="Work through one class at a time (Pro, Pro-Am, Masters)"
                                    )

                                    if _imsa_selected_class and _imsa_selected_class != "All Classes":
                                        _imsa_filtered = [n for n in _imsa_names if any(
                                            _sd.get('result_class', '').strip() == _imsa_selected_class
                                            for _sd in _imsa_driver_map.get(n, [])
                                        )]
                                        raw_results_list = _imsa_filtered
                                        st.session_state['uploaded_timing_names'] = _imsa_filtered
                                        st.info(f"🏷️ Showing **{len(_imsa_filtered)}** drivers in **{_imsa_selected_class}**")

    else: # Paste Text
        # Championship-specific download links
        _paste_links = {
            "BTCC": ("tsl-timing.com", "https://www.tsl-timing.com"),
            "British F4": ("tsl-timing.com", "https://www.tsl-timing.com"),
            "GB3": ("tsl-timing.com", "https://www.tsl-timing.com"),
            "British GT": ("tsl-timing.com", "https://www.tsl-timing.com"),
            "Porsche Cup GB": ("tsl-timing.com", "https://www.tsl-timing.com"),
            "UAE F4": ("formulamideast.com", "https://formulamideast.com"),
            "Porsche Cup NZ": ("motorsport.org.nz", "https://motorsport.org.nz"),
            "Porsche Sprint NA": ("porschesprint.com/results", "https://porschesprint.com/results"),
            "IndyNXT": ("indycar.com — IndyNXT Results", "https://www.indycar.com/results"),
            "DTM": ("dtm.com/results", "https://www.dtm.com/en/results"),
            "CTFROC (Formula Regional Oceania)": ("toyota.co.nz — FR Oceania", "https://www.toyota.co.nz/toyota-racing/castrol-toyota-fr-oceania/"),
        }

        # Auto-load from pre-built CSV in imports/ folder
        # Maps championship name → CSV filename slug
        _CSV_DRIVER_FILES = {
            "CTFROC (Formula Regional Oceania)": "ctfroc_2026_drivers.csv",
            "UAE F4": "uae_f4_2026_drivers.csv",
        }
        _csv_file = _CSV_DRIVER_FILES.get(selected_champ)
        _csv_path = os.path.join(BASE_DIR, "imports", _csv_file) if _csv_file else None
        _csv_names = []
        if _csv_path and os.path.exists(_csv_path):
            try:
                _csv_df = pd.read_csv(_csv_path)
                # Normalize column names to lowercase for matching
                _csv_df.columns = [c.strip().lower().replace(' ', '_') for c in _csv_df.columns]
                for _, _row in _csv_df.iterrows():
                    _fn = str(_row.get('first_name', '')).strip()
                    _ln = str(_row.get('last_name', '')).strip()
                    if _fn and _ln and _fn != 'nan' and _ln != 'nan':
                        _csv_names.append(f"{_fn} {_ln}")
            except Exception:
                pass

        if _csv_names and not raw_results_list:
            # Auto-load drivers from CSV
            raw_results_list = _csv_names
            st.success(f"✅ {len(_csv_names)} drivers auto-loaded from {_csv_file}")
        else:
            _link = _paste_links.get(selected_champ)
            if _link:
                st.caption(f"📋 Download results from [{_link[0]}]({_link[1]}) → copy driver names → paste below.")
            else:
                st.caption("Paste driver names from any timing sheet — one name per line.")
            text_input = st.text_area("Driver List (Name per line)", height=150, key="paste_names_input")
            if text_input:
                raw_results_list = text_input.split('\n')
    
    col_analyze, col_clear = st.columns([1, 4])

    # Check if Update button flagged an auto-analysis
    run_analysis = st.session_state.pop('_run_analysis_on_update', False)

    with col_analyze:
        if st.button("🔍 Analyze & Match Drivers") or run_analysis:
            if not raw_results_list:
                if run_analysis:
                    pass  # Update pressed with no data — just save settings, no error
                else:
                    st.error("Please provide driver data.")
            else:
                clean_names = [n.strip() for n in raw_results_list if len(n.strip()) > 3]

                with st.spinner(f"Analyzing {len(clean_names)} drivers..."):
                    _active_champ = st.session_state.get('global_championship', '')
                    results = dashboard.process_race_results(clean_names, event_name=event_name, championship=_active_champ)
                    st.session_state.matched_results = results

                    # Save Speedhive race results to matched drivers
                    if _speedhive_driver_results:
                        from funnel_manager import save_race_result
                        _sh_date = _speedhive_event.get('startDate', '') if _speedhive_event else ''
                        _saved_count = 0
                        for r in results:
                            _rname = r.get('original_name', '')
                            if _rname in _speedhive_driver_results and r.get('match'):
                                _driver = r['match']
                                _driver.notes = save_race_result(
                                    _driver, event_name, _active_champ,
                                    _speedhive_driver_results[_rname], _sh_date
                                )
                                dashboard.add_new_driver(
                                    _driver.email, _driver.first_name, _driver.last_name,
                                    _driver.facebook_url or "", ig_url=_driver.instagram_url or "",
                                    championship=_driver.championship or "", notes=_driver.notes
                                )
                                _saved_count += 1
                        if _saved_count:
                            st.toast(f"📊 Saved race results for {_saved_count} drivers")
                        # Store in session for contact card display
                        st.session_state['_speedhive_driver_results'] = _speedhive_driver_results

                    # PERSISTENCE: Save driver names as JSON (survives refresh better than pickle)
                    import json as _json_persist
                    try:
                        _persist_data = {
                            'names': clean_names,
                            'event': event_name,
                            'championship': _active_champ,
                            'timestamp': datetime.now().isoformat()
                        }
                        with open(os.path.join(DATA_DIR, "last_race_names.json"), "w") as f:
                            _json_persist.dump(_persist_data, f)
                    except Exception as e:
                        print(f"Failed to cache analysis names: {e}")
    
    with col_clear:
        if st.button("🗑️ Clear Results", help="Delete the current analysis and clear cache."):
            st.session_state.matched_results = None
            # Also clear cached timing sheet data
            st.session_state.pop('uploaded_timing_df', None)
            st.session_state.pop('uploaded_timing_names', None)
            try:
                for _f in ["last_race_analysis.pkl", "last_race_names.json"]:
                    p = os.path.join(DATA_DIR, _f)
                    if os.path.exists(p): os.remove(p)
            except: pass
            st.rerun()

    # 3. Processed Results
    # AUTO-RESTORE: If no results in session, try to restore from saved names
    if 'matched_results' not in st.session_state or not st.session_state.matched_results:
        import json as _json_restore
        _names_path = os.path.join(DATA_DIR, "last_race_names.json")
        if os.path.exists(_names_path):
            try:
                _cache_age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(_names_path))).total_seconds()
                if _cache_age < 24 * 3600:  # 24 hours
                    with open(_names_path, "r") as f:
                        _persist = _json_restore.load(f)
                    _cached_names = _persist.get('names', [])
                    _cached_champ = _persist.get('championship', '')
                    _cached_event = _persist.get('event', event_name)
                    if _cached_names:
                        _restored = dashboard.process_race_results(
                            _cached_names, event_name=_cached_event, championship=_cached_champ
                        )
                        st.session_state.matched_results = _restored
            except Exception as _restore_err:
                print(f"Failed to restore analysis: {_restore_err}")
                
    if 'matched_results' in st.session_state and st.session_state.matched_results:
        st.divider()
        results = st.session_state.matched_results

        # Refresh driver objects from live database — cached copies may have stale stages
        # SKIP on quick-action reruns (Done, Messaged etc.) — driver was just updated in memory
        if not st.session_state.pop('_skip_driver_refresh', False):
            for r in results:
                if r['match_status'] == 'match_found' and r.get('match'):
                    email = r['match'].email
                    fresh = dashboard._find_driver(email)
                    if fresh:
                        r['match'] = fresh

        # NOTE: Auto-reset removed — was silently resetting MESSAGED drivers
        # back to CONTACT on every page load, causing already-contacted drivers
        # to appear as new prospects. Drivers keep their actual stage now.

        # Stages that indicate "messaged" (contacted) — defined early for sorting + icons
        MESSAGED_STAGES = [FunnelStage.MESSAGED, FunnelStage.OUTREACH, FunnelStage.REPLIED,
                          FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT,
                          FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE,
                          FunnelStage.SLEEP_TEST_COMPLETED, FunnelStage.MINDSET_QUIZ_COMPLETED,
                          FunnelStage.FLOW_PROFILE_COMPLETED, FunnelStage.BLUEPRINT_STARTED,
                          FunnelStage.DAY1_COMPLETE, FunnelStage.DAY2_COMPLETE,
                          FunnelStage.STRATEGY_CALL_BOOKED, FunnelStage.CLIENT,
                          FunnelStage.SALE_CLOSED, FunnelStage.FOLLOW_UP]

        # BULK IMPORT ACTION
        new_prospects_count = sum(1 for r in results if r['match_status'] == 'new_prospect')
        if new_prospects_count > 0:
             c_bulk1, c_bulk2 = st.columns([2, 1])
             with c_bulk1:
                 st.metric("Total Drivers", len(results))
             with c_bulk2:
                 if st.button(f"⚡ Bulk Import {new_prospects_count} New Drivers", type="primary", help="Immediately add all new names to the Pipeline as Leads"):
                      added = 0
                      # Use a progress bar for satisfaction
                      progress_bar = st.progress(0)
                      
                      for idx, r in enumerate(results):
                           if r['match_status'] == 'new_prospect':
                               # Generate minimal details
                               name = r['original_name']
                               clean_name = "".join([c for c in name if c.isalnum() or c == ' ']).strip()
                               slug = clean_name.lower().replace(" ", "_")
                               email = f"no_email_{slug}"
                               
                               # Split name
                               parts = clean_name.split(' ')
                               f_name = parts[0].title()
                               l_name = " ".join(parts[1:]).title() if len(parts) > 1 else ""
                               
                               # Add to Dashboard/Memory
                               # USE GLOBAL CHAMPIONSHIP FOR BULK IMPORT AS WELL
                               curr_champ = st.session_state.get('global_championship', '')
                               
                               if dashboard.add_new_driver(email, f_name, l_name, "", "", championship=curr_champ):
                                   dashboard.update_driver_stage(email, FunnelStage.CONTACT)
                                   # Important: Ensure outreach_date is set so they appear in filtered views
                                   added_driver = dashboard._find_driver(email)
                                   if added_driver:
                                        added_driver.outreach_date = datetime.now()
                                        # Update localized match status so UI reflects it immediately
                                        r['match_status'] = 'match_found'
                                        r['match'] = added_driver
                                   added += 1
                                   
                           progress_bar.progress((idx + 1) / len(results))
                           
                      st.success(f"Successfully imported {added} drivers! They are now in the 'Leads / Contact' stage.")
                      st.rerun()
                      
        else:
             st.metric("Total Drivers", len(results))
        
        # All drivers stay visible — messaged ones get a ✅ and sort to the bottom
        filtered = list(results)

        # SORT: New prospects first → In Database → Messaged → Link Sent/Replied → No Socials/Not a Fit last
        def _sort_key(item):
            if item['match_status'] == 'new_prospect':
                return (0, item['original_name'])  # New prospects → top
            if item.get('match'):
                curr = item['match'].current_stage
                if curr == FunnelStage.NO_SOCIALS:
                    return (4, '')  # No Socials → bottom
                if curr == FunnelStage.NOT_A_FIT:
                    return (5, '')  # Not a Fit → very bottom
                if curr in [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT]:
                    return (3, '')  # Link Sent → after messaged
                if curr in MESSAGED_STAGES:
                    return (2, '')  # Messaged → middle
            return (1, '')  # In Database / Ready to message → after new
        filtered.sort(key=_sort_key)

        # Status counts for summary bar
        messaged_count = sum(1 for r in filtered if r.get('match') and r['match'].current_stage in MESSAGED_STAGES)
        no_socials_count = sum(1 for r in filtered if r.get('match') and r['match'].current_stage == FunnelStage.NO_SOCIALS)
        not_a_fit_count = sum(1 for r in filtered if r.get('match') and r['match'].current_stage == FunnelStage.NOT_A_FIT)
        link_sent_count = sum(1 for r in filtered if r.get('match') and r['match'].current_stage in [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT])
        in_db_count = sum(1 for r in filtered if r['match_status'] == 'match_found' and r.get('match') and r['match'].current_stage not in MESSAGED_STAGES and r['match'].current_stage != FunnelStage.NO_SOCIALS and r['match'].current_stage != FunnelStage.NOT_A_FIT)
        new_count = sum(1 for r in filtered if r['match_status'] == 'new_prospect')

        st.markdown(
            f"**{len(filtered)} drivers** &nbsp; · &nbsp; "
            f"🆕 {new_count} New &nbsp; · &nbsp; "
            f"🟢 {in_db_count} In Database &nbsp; · &nbsp; "
            f"✅ {messaged_count} Messaged &nbsp; · &nbsp; "
            f"🔗 {link_sent_count} Link Sent &nbsp; · &nbsp; "
            f"🚫 {no_socials_count} No Socials &nbsp; · &nbsp; "
            f"❌ {not_a_fit_count} Not a Fit"
        )

        # Initialize session state for tracking expanded cards
        if "just_added_names" not in st.session_state:
            st.session_state.just_added_names = set()

        # ── FILTER: Hide already-contacted drivers for fast outreach ──
        _CONTACTED_STAGES = set(MESSAGED_STAGES) | {FunnelStage.NO_SOCIALS, FunnelStage.NOT_A_FIT, FunnelStage.DOES_NOT_REPLY}
        actionable_count = sum(1 for r in filtered if not (r.get('match') and r['match'].current_stage in _CONTACTED_STAGES))
        done_count = len(filtered) - actionable_count

        _fc1, _fc2, _fc3 = st.columns([3, 1, 1])
        with _fc1:
            if _is_end_of_season:
                st.markdown(
                    f'<div style="background:#2d1b4e;border:1px solid #f59e0b;border-radius:8px;padding:10px 16px;'
                    f'margin:4px 0 8px 0;font-size:16px;color:#fbbf24;font-weight:700;">'
                    f'🏆 End of Season &nbsp;·&nbsp; 📋 {actionable_count} to message &nbsp;·&nbsp; ✅ {done_count} contacted'
                    f'</div>', unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f'<div style="background:#064e3b;border:1px solid #10b981;border-radius:8px;padding:10px 16px;'
                    f'margin:4px 0 8px 0;font-size:16px;color:#34d399;font-weight:700;">'
                    f'📋 {actionable_count} to message &nbsp;·&nbsp; ✅ {done_count} already contacted'
                    f'</div>', unsafe_allow_html=True
                )
        with _fc2:
            if _is_end_of_season:
                if st.button("🏁 Switch to Race Weekend", key="_switch_race_weekend", use_container_width=True):
                    st.session_state['_outreach_mode'] = "🏁 Race Weekend"
                    st.rerun()
            else:
                if st.button("🏆 End of Season", key="_switch_eos", type="primary", use_container_width=True):
                    st.session_state['_outreach_mode'] = "🏆 End of Season"
                    st.rerun()
        with _fc3:
            hide_contacted = st.toggle("Hide Contacted", value=st.session_state.get('_hide_contacted', True), key="_hide_contacted")

        if hide_contacted:
            display_list = [r for r in filtered if not (r.get('match') and r['match'].current_stage in _CONTACTED_STAGES)]
        else:
            display_list = filtered

        # PAGINATION — only render a batch of cards per page to keep UI snappy
        PAGE_SIZE = 20
        total_display = len(display_list)
        if total_display > PAGE_SIZE:
            max_page = (total_display - 1) // PAGE_SIZE
            _page_key = '_outreach_page'
            if _page_key not in st.session_state:
                st.session_state[_page_key] = 0
            current_page = st.session_state[_page_key]
            start_idx = current_page * PAGE_SIZE
            end_idx = min(start_idx + PAGE_SIZE, total_display)
            display_list = display_list[start_idx:end_idx]

            # Pagination controls
            _pc1, _pc2, _pc3 = st.columns([1, 2, 1])
            with _pc1:
                if current_page > 0 and st.button("⬅️ Previous", key="_page_prev"):
                    st.session_state[_page_key] = current_page - 1
                    st.rerun()
            with _pc2:
                st.markdown(f"<div style='text-align:center;color:#888;'>Page {current_page + 1} of {max_page + 1} ({total_display} drivers)</div>", unsafe_allow_html=True)
            with _pc3:
                if current_page < max_page and st.button("➡️ Next", key="_page_next"):
                    st.session_state[_page_key] = current_page + 1
                    st.rerun()

        # FULL CARD VIEW
        for i, r in enumerate(display_list):
            # Color Code / Icon logic — clear status for every driver
            if r['match_status'] == 'match_found':
                if r.get('match'):
                    curr = r['match'].current_stage
                    if curr == FunnelStage.NO_SOCIALS:
                        icon = "🚫"
                        label = "NO SOCIALS"
                    elif curr == FunnelStage.NOT_A_FIT:
                        icon = "❌"
                        label = "NOT A FIT"
                    elif curr in [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT]:
                        icon = "🔗"
                        label = "LINK SENT"
                    elif curr in [FunnelStage.REPLIED]:
                        icon = "✅💬"
                        label = "REPLIED"
                    elif curr in MESSAGED_STAGES:
                        icon = "✅✉️"
                        label = "MESSAGED"
                    else:
                        icon = "🟢"
                        label = "IN DATABASE"
                else:
                    icon = "🟢"
                    label = "IN DATABASE"
            else:
                icon = "🆕"
                label = "NEW PROSPECT"

            # Keep expanded only if just added AND not yet processed
            # Auto-close any card that's been actioned (messaged, no socials, not a fit, etc.)
            PROCESSED_STAGES = MESSAGED_STAGES + [FunnelStage.NO_SOCIALS, FunnelStage.NOT_A_FIT, FunnelStage.DOES_NOT_REPLY]
            is_expanded = r['original_name'] in st.session_state.just_added_names
            if r.get('match') and r['match'].current_stage in PROCESSED_STAGES:
                is_expanded = False
                # Clear from just_added so it stays closed
                st.session_state.just_added_names.discard(r['original_name'])

            # Build expander title with optional date + class
            _aka = ""
            if r.get('match') and getattr(r['match'], 'preferred_name', None):
                _pn = r['match'].preferred_name
                _fn = r['match'].first_name or ''
                if _pn.lower().strip() != _fn.lower().strip():
                    _aka = f"  (otherwise known as {_pn})"
            _expander_title = f"{icon} {r['original_name']}{_aka}  [{label}]"
            # Add messaged/outreach date if driver is at a contacted stage
            if r.get('match') and r['match'].current_stage in MESSAGED_STAGES:
                _msg_date = r['match'].outreach_date or r['match'].last_activity
                if _msg_date:
                    _expander_title += f"  📅 {_msg_date.strftime('%d %b')}"
            # Add race class from Speedhive data
            _sh_store_hdr = st.session_state.get('_speedhive_driver_results') or _speedhive_driver_results
            if _sh_store_hdr and r['original_name'] in _sh_store_hdr:
                _driver_classes = set()
                for _sd in _sh_store_hdr[r['original_name']]:
                    _rc = _sd.get('result_class', '').strip()
                    if _rc:
                        _driver_classes.add(_rc)
                if _driver_classes:
                    _expander_title += f"  🏷️ {', '.join(sorted(_driver_classes))}"

            with st.expander(_expander_title, expanded=is_expanded):

                # --- UNIFIED CARD: Same layout for ALL drivers ---
                is_existing = r['match_status'] == 'match_found' and r.get('match')
                driver_match = r.get('match') if is_existing else None

                # Get driver details (from DB if existing, or generate from name if new)
                if is_existing:
                    r_first = driver_match.first_name or r['original_name'].split(' ')[0]
                    r_last = driver_match.last_name or (' '.join(r['original_name'].split(' ')[1:]) if ' ' in r['original_name'] else '')
                    r_email_display = driver_match.email if not driver_match.email.startswith("no_email_") else ""
                    r_fb = driver_match.facebook_url or ""
                    r_ig = driver_match.instagram_url or ""
                    r_champ = driver_match.championship or ""
                    r_notes = driver_match.notes or ""
                    r_full_name = driver_match.full_name
                else:
                    parts = r['original_name'].split(' ')
                    r_first = parts[0].title()
                    r_last = parts[1].title() if len(parts) > 1 else ""
                    r_email_display = ""
                    r_fb = ""
                    r_ig = ""
                    r_champ = st.session_state.get('global_championship', '')
                    r_notes = ""
                    r_full_name = r['original_name']

                # Status banner
                if is_existing:
                    curr = driver_match.current_stage
                    if curr == FunnelStage.NO_SOCIALS:
                        st.warning(f"🚫 {r_full_name} — No Socials Found")
                        if st.button("Re-open Search", key=f"reopen_{i}_{r['original_name']}"):
                            dashboard.update_driver_stage(driver_match.email, FunnelStage.CONTACT)
                            st.rerun()
                    elif curr == FunnelStage.NOT_A_FIT:
                        st.error(f"❌ {r_full_name} — Not a Fit")
                else:
                    st.info(f"🆕 {r['original_name']} — New Prospect (not in database)")

                # ============ SPEEDHIVE RACE DATA (proof they raced) ============
                _sh_driver_data = None
                _sh_store = st.session_state.get('_speedhive_driver_results') or _speedhive_driver_results
                if _sh_store and r['original_name'] in _sh_store:
                    _sh_driver_data = _sh_store[r['original_name']]
                if _sh_driver_data:
                    _sh_rows = []
                    for _sd in _sh_driver_data:
                        _best = _sd.get('best_lap', '')
                        # Format millisecond times if numeric
                        if isinstance(_best, (int, float)) and _best > 0:
                            _m = int(_best // 60000)
                            _s = (_best % 60000) / 1000
                            _best = f"{_m}:{_s:06.3f}" if _m else f"{_s:.3f}s"
                        _sh_rows.append({
                            "Session": _sd.get('session_group', _sd.get('session_name', '')),
                            "Type": (_sd.get('session_type', '') or '').capitalize(),
                            "Pos": _sd.get('position', '—'),
                            "Class Pos": _sd.get('position_in_class', '—'),
                            "Best Lap": _best or '—',
                            "Laps": _sd.get('laps', '—'),
                            "Class": _sd.get('result_class', '—'),
                            "#": _sd.get('start_number', '—'),
                            "Status": _sd.get('status', ''),
                        })
                    # Show as a compact coloured banner + table
                    _best_pos = min((s.get('position') for s in _sh_driver_data if s.get('position')), default=None)
                    _race_count = sum(1 for s in _sh_driver_data if (s.get('session_type') or '') == 'race')
                    _qual_count = sum(1 for s in _sh_driver_data if (s.get('session_type') or '') == 'qualify')
                    _summary_parts = []
                    if _best_pos: _summary_parts.append(f"Best P{_best_pos}")
                    if _race_count: _summary_parts.append(f"{_race_count} race{'s' if _race_count != 1 else ''}")
                    if _qual_count: _summary_parts.append(f"{_qual_count} quali")
                    _summary_parts.append(f"#{_sh_driver_data[0].get('start_number', '?')}")
                    st.success(f"🏁 **Speedhive verified** — {' · '.join(_summary_parts)}")
                    with st.expander("📊 Full session data", expanded=False):
                        st.dataframe(pd.DataFrame(_sh_rows), use_container_width=True, hide_index=True)
                elif _sh_store and r['original_name'] not in _sh_store:
                    # Speedhive was used but this driver wasn't in the results
                    pass  # No banner — they simply weren't in selected sessions

                # ============ PERFORMANCE SUMMARY LINE ============
                from ui_components import _build_perf_line
                from funnel_manager import get_results_summary as _get_rs
                _perf_saved_summary = {}
                if r.get('match') and r['match'].notes:
                    _perf_saved_summary = _get_rs(r['match'].notes)
                _perf_live_data = []
                _sh_store_perf = st.session_state.get('_speedhive_driver_results') or _speedhive_driver_results
                if _sh_store_perf and r['original_name'] in _sh_store_perf:
                    _perf_live_data = _sh_store_perf[r['original_name']]
                _perf_summary_line, _ = _build_perf_line(_perf_saved_summary, _perf_live_data)
                if _perf_summary_line:
                    st.info(f"🏁 {_perf_summary_line}")

                # ============ CLEAN CARD LAYOUT ============
                # Use preferred/social media name for messages (e.g. "Chris" not "Christopher")
                f_name = (getattr(driver_match, 'display_name', None) if driver_match else None) or r_first or r['original_name'].split(' ')[0]

                # --- Build performance + AI message data (once, shared) ---
                from ui_components import generate_ai_message, REPLY_TEMPLATES, _build_perf_line, _perf_opener
                from funnel_manager import get_results_summary
                import re as _re_card

                _perf_data = {}
                _sh_store_msg = st.session_state.get('_speedhive_driver_results') or _speedhive_driver_results
                if _sh_store_msg and r['original_name'] in _sh_store_msg:
                    _perf_data['live'] = _sh_store_msg[r['original_name']]
                if r.get('match') and r['match'].notes:
                    _perf_data['saved'] = get_results_summary(r['match'].notes)

                _thread = ""
                _driver_for_ai = r.get('match')
                if _driver_for_ai and _driver_for_ai.notes:
                    _th_match = _re_card.search(r'\[THREAD\](.*?)\[/THREAD\]', _driver_for_ai.notes, _re_card.DOTALL)
                    if _th_match:
                        _thread = _th_match.group(1).strip()

                if _driver_for_ai:
                    _ai_msg, _ai_type, _ai_explain = generate_ai_message(
                        _driver_for_ai, conversation_thread=_thread,
                        performance_data=_perf_data, event_name=event_name,
                        outreach_mode="end_of_season" if _is_end_of_season else "race_weekend",
                        championship=selected_champ or ""
                    )
                else:
                    _, _pd = _build_perf_line(_perf_data.get('saved', {}), _perf_data.get('live', []))
                    _ai_msg = _perf_opener(
                        _pd, f_name, event_name,
                        outreach_mode="end_of_season" if _is_end_of_season else "race_weekend",
                        championship=selected_champ or ""
                    )
                    _ai_type = "End of season" if _is_end_of_season else "Cold outreach"
                    _ai_explain = "New driver"

                # Store AI message for Chrome extension sync
                # Replace driver's first name with {name} placeholder so the
                # extension can substitute the correct name at click time
                _ext_ai = _ai_msg.replace(f_name, '{name}', 1) if f_name and f_name in _ai_msg else _ai_msg
                st.session_state['_ext_ai_outreach_msg'] = _ext_ai
                # Sync the current driver name to Chrome extension
                st.session_state['_ext_current_driver'] = r['original_name']
                # Also store per-driver dict so extension can look up by name
                if '_ext_ai_messages' not in st.session_state:
                    st.session_state['_ext_ai_messages'] = {}
                st.session_state['_ext_ai_messages'][r['original_name']] = _ext_ai

                # --- TWO COLUMNS: Thread + Message | Social + Actions ---
                rc_left, rc_right = st.columns([3, 2])

                with rc_left:
                    # CONVERSATION THREAD (compact, scrollable)
                    if _thread:
                        with st.container(height=180):
                            for _tl in _thread.split('\n'):
                                _tl = _tl.strip()
                                if not _tl:
                                    continue
                                if _tl.startswith('You:') or _tl.startswith('  You:'):
                                    st.markdown(f"<div style='background:#1877F2;color:white;padding:4px 8px;border-radius:10px;margin:2px 0 2px 30px;font-size:0.82em;'>{_tl.split(':',1)[1].strip()}</div>", unsafe_allow_html=True)
                                else:
                                    _msg_text = _tl.split(':', 1)[1].strip() if ':' in _tl and len(_tl.split(':')[0]) < 25 else _tl
                                    st.markdown(f"<div style='background:#333;color:#eee;padding:4px 8px;border-radius:10px;margin:2px 30px 2px 0;font-size:0.82em;'>{_msg_text}</div>", unsafe_allow_html=True)

                    # AI MESSAGE — ready to copy
                    st.caption(f"🤖 {_ai_type}")
                    evt_key = event_name.replace(" ", "_").lower() if event_name else "no_event"
                    st.code(_ai_msg, language=None)

                    # Template picker (collapsed — only if they want something different)
                    with st.expander("📋 More templates", expanded=False):
                        templates = {f"🤖 {_ai_type}": _ai_msg, "Blank": f"Hey {f_name}, "}
                        for k, v_raw in REPLY_TEMPLATES.items():
                            try: templates[k] = v_raw.replace("{name}", f_name)
                            except: templates[k] = v_raw
                        sel = st.selectbox("Template", list(templates.keys()), key=f"tpl_{i}_{r['original_name']}", label_visibility="collapsed")
                        if sel != f"🤖 {_ai_type}":
                            st.code(templates[sel], language=None)

                with rc_right:
                    # SOCIAL LINKS — one button opens all 4 search tabs for speed
                    from urllib.parse import quote_plus
                    driver_name_q = quote_plus(r['original_name'])
                    _racing_q = quote_plus(r['original_name'] + ' racing')
                    _ag_hash = quote_plus(r['original_name'])

                    # Build URLs for all 4 searches
                    _fb_name_url = f"https://www.facebook.com/search/people/?q={driver_name_q}#ag_driver={_ag_hash}"
                    _fb_race_url = f"https://www.google.com/search?q={_racing_q}"
                    _ig_name_url = f"https://www.google.com/search?q={driver_name_q}+instagram"
                    _ig_race_url = f"https://www.google.com/search?q={_racing_q}+instagram"

                    # If they already have direct profile links, show those too
                    if r_fb:
                        _fb_direct = r_fb if r_fb.startswith("http") else f"https://www.facebook.com/{r_fb}"
                        _fb_direct_with_hash = f"{_fb_direct}#ag_driver={_ag_hash}"
                        st.markdown(f'<a href="{_fb_direct_with_hash}" target="_blank" style="text-decoration:none;display:block;margin-bottom:4px;">'
                            f'<div style="background:#4CAF50;color:white;padding:6px;border-radius:6px;text-align:center;font-weight:bold;font-size:0.82em;">'
                            f'👤 Facebook Profile</div></a>', unsafe_allow_html=True)
                    if r_ig:
                        _ig_direct = r_ig if r_ig.startswith("http") else f"https://www.instagram.com/{r_ig}"
                        _ig_direct_with_hash = f"{_ig_direct}#ag_driver={_ag_hash}"
                        st.markdown(f'<a href="{_ig_direct_with_hash}" target="_blank" style="text-decoration:none;display:block;margin-bottom:4px;">'
                            f'<div style="background:#E1306C;color:white;padding:6px;border-radius:6px;text-align:center;font-weight:bold;font-size:0.82em;">'
                            f'📸 Instagram Profile</div></a>', unsafe_allow_html=True)

                    # --- Search buttons (same approach as Rider app) ---
                    # components.html iframe has allow-popups-to-escape-sandbox
                    # User may need to allow popups for this site on first use
                    import streamlit.components.v1 as components
                    _open4_html = f'''
                    <style>
                        body {{ margin: 0; padding: 0; background: transparent; }}
                        .open4-btn {{
                            display: block; width: 100%; padding: 10px 8px;
                            background: linear-gradient(135deg, #1877F2, #E1306C);
                            color: white; border: none; border-radius: 8px;
                            font-size: 14px; font-weight: bold; cursor: pointer;
                            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
                        }}
                        .open4-btn:hover {{ opacity: 0.85; }}
                        .links {{ display: flex; flex-wrap: wrap; gap: 4px; margin-top: 6px; }}
                        .links a {{
                            flex: 1 1 45%; padding: 5px 6px; border-radius: 5px;
                            color: white; text-decoration: none; font-size: 11px;
                            font-weight: bold; font-family: sans-serif; text-align: center;
                        }}
                        .fb {{ background: #1877F2; }} .ig {{ background: #E1306C; }} .gg {{ background: #34A853; }}
                        .note {{ font-size: 10px; color: #999; text-align: center; margin-top: 4px; }}
                    </style>
                    <button class="open4-btn" onclick="openAll4()">🚀 Open All 4 Searches</button>
                    <div class="links">
                        <a href="{_fb_name_url}" target="_blank" class="fb">👤 FB Name</a>
                        <a href="{_fb_race_url}" target="_blank" class="gg">🔍 Google Racing</a>
                        <a href="{_ig_name_url}" target="_blank" class="ig">📸 IG Name</a>
                        <a href="{_ig_race_url}" target="_blank" class="ig">🏁 IG Race</a>
                    </div>
                    <div class="note" id="note" style="display:none;">⚠️ Allow popups for this site to open all 4 at once</div>
                    <script>
                    function openAll4() {{
                        var urls = ["{_fb_name_url}","{_fb_race_url}","{_ig_name_url}","{_ig_race_url}"];
                        var blocked = false;
                        for (var i = 0; i < urls.length; i++) {{
                            var w = window.open(urls[i], "_blank");
                            if (!w || w.closed) blocked = true;
                        }}
                        if (blocked) document.getElementById("note").style.display = "block";
                    }}
                    </script>
                    '''
                    components.html(_open4_html, height=90)

                    # Messenger link if they have FB
                    if r_fb and not _thread:
                        _fb_msg = r_fb if 'messenger.com' in str(r_fb) else f"https://www.messenger.com/t/{str(r_fb).rstrip('/').split('/')[-1]}"
                        _fb_msg_with_hash = f"{_fb_msg}#ag_driver={_ag_hash}"
                        st.markdown(f"[📱 Open Messenger]({_fb_msg_with_hash})")

                # ============ ACTION BUTTONS — single clean row ============
                if is_existing:
                    _has_social = bool(driver_match.facebook_url or driver_match.instagram_url)
                    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
                    if c1.button("✅ Done", key=f"q_done_{i}_{r['original_name']}", use_container_width=True, type="primary"):
                        # Extension already saved to Airtable — just update local state
                        if driver_match.current_stage == FunnelStage.CONTACT:
                            driver_match.current_stage = FunnelStage.MESSAGED
                        if not driver_match.outreach_date:
                            driver_match.outreach_date = datetime.now()
                        driver_match.last_activity = datetime.now()
                        st.session_state.just_added_names.discard(r['original_name'])
                        st.session_state['_skip_driver_refresh'] = True  # Fast rerun
                        st.toast(f"✅ {r_first} done")
                        st.rerun()
                    if c2.button("🚀 Messaged", key=f"q_msg_{i}_{r['original_name']}", use_container_width=True, disabled=not _has_social):
                        dashboard.update_driver_stage(driver_match.email, FunnelStage.MESSAGED)
                        driver_match.outreach_date = datetime.now()
                        driver_match.last_activity = datetime.now()
                        dashboard.add_new_driver(
                            driver_match.email, driver_match.first_name, driver_match.last_name,
                            driver_match.facebook_url or "", ig_url=driver_match.instagram_url or "",
                            championship=driver_match.championship or "", notes=driver_match.notes
                        )
                        _fu = driver_match.follow_up_date
                        st.toast(f"✅ {r_first} messaged · Follow-up {_fu.strftime('%a %d %b') if _fu else 'auto-set'}")
                        st.session_state['_skip_driver_refresh'] = True
                        st.rerun()
                    if c3.button("↩️ Replied", key=f"q_rep_{i}_{r['original_name']}", use_container_width=True):
                        dashboard.update_driver_stage(driver_match.email, FunnelStage.REPLIED)
                        st.toast(f"↩️ {r_first} replied")
                        st.session_state['_skip_driver_refresh'] = True
                        st.rerun()
                    if c4.button("🔗 Link Sent", key=f"q_lnk_{i}_{r['original_name']}", use_container_width=True):
                        dashboard.update_driver_stage(driver_match.email, FunnelStage.LINK_SENT)
                        st.toast(f"🔗 Link sent to {r_first}")
                        st.session_state['_skip_driver_refresh'] = True
                        st.rerun()
                    if c5.button("🔇 No Reply", key=f"dq_dnr_{i}_{r['original_name']}", use_container_width=True):
                        dashboard.update_driver_stage(driver_match.email, FunnelStage.DOES_NOT_REPLY)
                        st.toast(f"🔇 {r_first} — does not reply")
                        st.session_state['_skip_driver_refresh'] = True
                        st.rerun()
                    if c6.button("🚫 No Socials", key=f"dq_ns_{i}_{r['original_name']}", use_container_width=True):
                        dashboard.update_driver_stage(driver_match.email, FunnelStage.NO_SOCIALS)
                        st.toast(f"🚫 {r_first}")
                        st.session_state['_skip_driver_refresh'] = True
                        st.rerun()
                    if c7.button("❌ Not A Fit", key=f"dq_naf_{i}_{r['original_name']}", use_container_width=True):
                        dashboard.update_driver_stage(driver_match.email, FunnelStage.NOT_A_FIT)
                        st.toast(f"❌ {r_first}")
                        st.session_state['_skip_driver_refresh'] = True
                        st.rerun()
                    if not _has_social:
                        st.caption("⚠️ Add a social URL to enable Messaged")
                else:
                    # Helper to create driver and set stage for new prospects
                    def _quick_save_new(stage, note_text, toast_msg):
                        nf_first = r['original_name'].split(' ')[0]
                        nf_last = r['original_name'].split(' ')[1] if ' ' in r['original_name'] else ""
                        slug = r['original_name'].lower().strip().replace(' ', '_')
                        slug = "".join([c for c in slug if c.isalnum() or c == '_'])
                        final_email = f"no_email_{slug}"
                        # Set stage in memory BEFORE add_new_driver so the Airtable
                        # sync includes the stage in the same API call (1 round trip, not 2)
                        dashboard.add_new_driver(final_email, nf_first, nf_last, "", "", "",
                            notes=note_text)
                        # update_driver_stage sets stage + dates + Airtable sync
                        dashboard.update_driver_stage(final_email, stage)
                        nf_driver = dashboard._find_driver(final_email)
                        if nf_driver:
                            r['match_status'] = 'match_found'
                            r['match'] = nf_driver
                        st.session_state['_skip_driver_refresh'] = True  # Fast rerun
                        st.toast(toast_msg)
                        st.rerun()

                    c1, c2, c3 = st.columns(3)
                    if c1.button("✅ Done", key=f"nf_done_{i}_{r['original_name']}", use_container_width=True, type="primary"):
                        # Fast path: check if driver exists by slug email (no full scan)
                        _slug = r['original_name'].lower().strip().replace(' ', '_')
                        _slug = "".join([c for c in _slug if c.isalnum() or c == '_'])
                        _existing = (dashboard._find_driver(f"no_email_{_slug}")
                                     or dashboard._find_driver(r['original_name'].lower().strip()))
                        if _existing and hasattr(_existing, 'email'):
                            if _existing.current_stage == FunnelStage.CONTACT:
                                _existing.current_stage = FunnelStage.MESSAGED
                            if not _existing.outreach_date:
                                _existing.outreach_date = datetime.now()
                            _existing.last_activity = datetime.now()
                            r['match_status'] = 'match_found'
                            r['match'] = _existing
                            st.session_state.just_added_names.discard(r['original_name'])
                            st.session_state['_skip_driver_refresh'] = True
                            st.toast(f"✅ {r['original_name']} done")
                            st.rerun()
                        else:
                            _quick_save_new(FunnelStage.MESSAGED, "Messaged via extension.", f"✅ {r['original_name']} done")
                    if c2.button("🚫 Not Found", key=f"nf_{i}_{r['original_name']}", use_container_width=True):
                        _quick_save_new(FunnelStage.NO_SOCIALS, "No social media found during Race Outreach.", f"🚫 {r['original_name']}")
                    if c3.button("❌ Not A Fit", key=f"nf_naf_{i}_{r['original_name']}", use_container_width=True):
                        _quick_save_new(FunnelStage.NOT_A_FIT, "Marked Not A Fit during Race Outreach.", f"❌ {r['original_name']}")
                    c4, c5, c6 = st.columns(3)
                    if c4.button("🔇 No Reply", key=f"nf_dnr_{i}_{r['original_name']}", use_container_width=True):
                        _quick_save_new(FunnelStage.DOES_NOT_REPLY, "Does not reply to messages.", f"🔇 {r['original_name']}")
                    if c5.button("🏆 Client", key=f"nf_cl_{i}_{r['original_name']}", use_container_width=True):
                        _quick_save_new(FunnelStage.CLIENT, "Already a client.", f"🏆 {r['original_name']}")
                    if c6.button("📚 Done Blueprint", key=f"nf_bp_{i}_{r['original_name']}", use_container_width=True):
                        _quick_save_new(FunnelStage.BLUEPRINT_STARTED, "Already completed Blueprint.", f"📚 {r['original_name']}")

                # ============ EDIT FORM (collapsed — clean) ============
                with st.expander("✏️ Edit" if is_existing else "💾 Save Contact", expanded=not is_existing):
                    with st.form(key=f"unified_form_{i}_{r['original_name']}"):
                        c_n1, c_n2 = st.columns(2)
                        in_first = c_n1.text_input("First", value=r_first, key=f"uf_first_{i}_{r['original_name']}")
                        in_last = c_n2.text_input("Last", value=r_last, key=f"uf_last_{i}_{r['original_name']}")
                        c_f1, c_f2 = st.columns(2)
                        in_fb = c_f1.text_input("Facebook URL", value=r_fb, key=f"uf_fb_{i}_{r['original_name']}")
                        in_ig = c_f2.text_input("Instagram URL", value=r_ig, key=f"uf_ig_{i}_{r['original_name']}")

                        champ_val = r_champ
                        global_champ = st.session_state.get('global_championship', '')
                        if global_champ and global_champ.lower() not in champ_val.lower():
                            champ_val = f"{champ_val}, {global_champ}" if champ_val else global_champ
                        in_champ = st.text_input("Championship", value=champ_val, key=f"uf_champ_{i}_{r['original_name']}")

                        if st.form_submit_button("💾 Save" if is_existing else "💾 Save & Mark Messaged", type="primary"):
                            final_email = ""
                            if is_existing:
                                final_email = driver_match.email
                            else:
                                slug = f"{in_first} {in_last}".lower().strip().replace(' ', '_')
                                slug = "".join([c for c in slug if c.isalnum() or c == '_'])
                                final_email = f"no_email_{slug}"

                            success = dashboard.add_new_driver(final_email, in_first, in_last, in_fb, ig_url=in_ig, championship=in_champ, notes=r_notes)
                            if success:
                                if in_champ:
                                    session_added = st.session_state.get('session_added_championships', [])
                                    if in_champ not in session_added:
                                        session_added.append(in_champ)
                                        st.session_state.session_added_championships = session_added
                                    try:
                                        _s = load_settings_store()
                                        if _s and _s.is_available:
                                            _s.set('championships', sorted(list(set(session_added))))
                                    except: pass

                                if not is_existing and (in_fb.strip() or in_ig.strip()):
                                    dashboard.update_driver_stage(final_email, FunnelStage.MESSAGED)

                                _saved = dashboard._find_driver(final_email)
                                if _saved:
                                    _saved.outreach_date = datetime.now()
                                    _saved.last_activity = datetime.now()
                                    r['match_status'] = 'match_found'
                                    r['match'] = _saved
                                    pass  # Results will be re-derived from saved names on refresh

                                st.session_state.just_added_names.add(r['original_name'])
                                st.toast(f"✅ {in_first} saved!")
                                st.rerun()
                            else:
                                st.error("Failed to save.")

    # Sync circuit/champ/AI message to Chrome extension via hidden div
    # (called after card loop so session_state has the latest AI message)
    _render_ext_sync_div()




# ==============================================================================
# STRATEGY CALLS VIEW
# ==============================================================================

def render_strategy_calls(dashboard):
    """Render the Strategy Call system — Pre-Call Prep, Post-Call Analysis, Application Form."""
    import json as _json

    st.subheader("📞 Championship Strategy Call System")
    st.caption("58% converting call framework • Pre-Call Prep • Post-Call Coaching • Application Questions")

    mode = st.radio(
        "Mode",
        ["🎯 Pre-Call Preparation", "📊 Post-Call Analysis", "📝 Application Form", "📖 Call Scripts"],
        horizontal=True,
        label_visibility="collapsed",
        key="strategy_mode"
    )

    # ══════════════════════════════════════════════════════════════════
    # MODE 1: LIVE CALL WORKSHEET
    # ══════════════════════════════════════════════════════════════════
    if mode == "🎯 Pre-Call Preparation":
        st.markdown("### 📞 Live Strategy Call Worksheet")

        # ── Driver Selection ──
        call_booked = [d for d in dashboard.drivers.values()
                       if d.current_stage == FunnelStage.STRATEGY_CALL_BOOKED]

        col1, col2 = st.columns([2, 1])
        with col1:
            driver_options = ["— Select a driver —"] + [f"{d.full_name} ({d.championship or 'No championship'})" for d in call_booked]
            selected = st.selectbox("Driver with Strategy Call Booked", driver_options, key="precall_driver")
        with col2:
            call_number = st.radio("Call", ["Call 1 — Discovery", "Call 2 — Close"], key="call_number", horizontal=True)

        if selected == "— Select a driver —":
            st.info("👆 Select a driver to load their call worksheet with auto-populated data.")
            return

        idx = driver_options.index(selected) - 1
        selected_driver = call_booked[idx]
        name = selected_driver.display_name
        full_name = selected_driver.full_name

        # ── AUTO-LOAD ALL AVAILABLE DATA ──
        app_answers = {}

        # 1. Google Sheet application data
        _sheet_key = 'Strategy Call Application.csv'
        _app_df = None
        if hasattr(dashboard, 'data_loader') and hasattr(dashboard.data_loader, 'overrides'):
            _app_df = dashboard.data_loader.overrides.get(_sheet_key)
            if _app_df is None:
                for k in dashboard.data_loader.overrides:
                    if 'strategy' in k.lower() and 'application' in k.lower():
                        _app_df = dashboard.data_loader.overrides[k]
                        break

        if _app_df is not None and hasattr(_app_df, 'to_dict'):
            _name_lower = full_name.lower().strip()
            _email_lower = (selected_driver.email or "").lower().strip()
            for _, _row in _app_df.iterrows():
                _row_email = str(_row.get('Email', _row.get('email', ''))).lower().strip()
                _row_first = str(_row.get('First name', _row.get('first name', ''))).lower().strip()
                _row_last = str(_row.get('Last name', _row.get('last name', ''))).lower().strip()
                if (_email_lower and _row_email == _email_lower) or f"{_row_first} {_row_last}".strip() == _name_lower:
                    _COL_MAP = {
                        'what is your age?': 'age',
                        'what is your current level of performance?': 'performance_level',
                        'what championship do you race in?': 'championship',
                        "what's your no1 racing goal for this season?": 'season_goal',
                        'your improve assessment revealed specific performance gaps. which category surprised you most with its score?': 'assessment_surprise',
                        "what's the #1 mental barrier you're committed to eliminating this season?": 'mental_barrier',
                        'how committed are you to solving this barrier this season?': 'commitment_level',
                        'if you were performing at your full potential consistently, how would racing feel different?': 'full_potential_feeling',
                        'who funds your racing': 'funding_source',
                        'if accepted into flow performance, do you have the financial resources to invest in elite-level mental training right now?': 'financial_ready',
                        'on a scale of 1-10, how serious are you about breakthrough performance?': 'seriousness_scale',
                        'where do you see yourself 3 years from now?': 'three_year_vision',
                        'anything else we should know?': 'anything_else',
                        'what town/city are you based': 'city', 'country': 'country',
                        'what best describes you': 'racer_type',
                    }
                    for col_name, col_val in _row.items():
                        _key = _COL_MAP.get(str(col_name).lower().strip())
                        if _key and col_val and str(col_val).strip().lower() not in ('nan', ''):
                            app_answers[_key] = str(col_val).strip()
                    break

        # 2. Assessment scores
        day1_score = selected_driver.day1_score
        day2_scores = selected_driver.day2_scores or {}

        # Find weakest pillar
        weakest_pillar = ""
        weakest_score = 999
        pillar_labels = {'mindset': 'Mindset', 'preparation': 'Preparation', 'flow': 'Flow', 'feedback': 'Feedback', 'sponsorship': 'Sponsorship'}
        if day2_scores:
            for k, v in day2_scores.items():
                if v < weakest_score:
                    weakest_score = v
                    weakest_pillar = pillar_labels.get(k, k)

        # 3. Race results & trend
        from funnel_manager import get_results_summary
        results_summary = get_results_summary(selected_driver.notes or "")
        trend = results_summary.get("trend", "new")
        best_pos = results_summary.get("best_pos")
        best_circuit = results_summary.get("best_circuit", "")
        best_lap = results_summary.get("best_lap", "")
        latest_result = results_summary.get("latest")

        # ── DATA SUMMARY BAR ──
        st.success(f"✅ Loaded data for **{full_name}**")

        # Row 1: Key metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Day 1 Score", f"{day1_score}/100" if day1_score else "—")
        m2.metric("Weakest Pillar", weakest_pillar or "—")
        m3.metric("Best Result", f"P{best_pos}" if best_pos else "—")
        m4.metric("Trend", {"improving": "📈 Improving", "declining": "📉 Declining", "stable": "➡️ Stable", "new": "🆕 New"}.get(trend, "—"))
        m5.metric("Seriousness", f"{app_answers.get('seriousness_scale', '—')}/10")

        # Row 2: Additional race data
        r1, r2, r3, r4 = st.columns(4)
        if best_lap:
            r1.metric("Best Lap", best_lap)
        else:
            r1.metric("Best Lap", "—")
        if best_circuit:
            r2.metric("Best Circuit", best_circuit[:20])
        else:
            r2.metric("Best Circuit", "—")
        if latest_result:
            _lr_pos = latest_result.get('pos', '?')
            _lr_circuit = latest_result.get('circuit', '')[:15]
            _lr_date = latest_result.get('date', '')
            r3.metric("Latest Result", f"P{_lr_pos}" if _lr_pos else "—", delta=_lr_circuit)
        else:
            r3.metric("Latest Result", "—")
        r4.metric("Commitment", app_answers.get('commitment_level', '—')[:20] if app_answers.get('commitment_level') else "—")

        # Day 2 Pillar Scores
        if day2_scores:
            st.markdown("**Day 2 Pillar Scores:**")
            pcols = st.columns(len(day2_scores))
            for i, (k, v) in enumerate(day2_scores.items()):
                label = pillar_labels.get(k, k)
                is_weakest = (label == weakest_pillar)
                pcols[i].metric(f"{'⚠️ ' if is_weakest else ''}{label}", f"{v:.0f}/10" if v else "—")

        # Season Notes — expandable panel
        _clean_notes = selected_driver.notes or ""
        # Remove internal tags for clean display
        import re as _re
        _display_notes = _re.sub(r'\[STRATEGY_APP\].*?\[/STRATEGY_APP\]', '', _clean_notes, flags=_re.DOTALL).strip()
        _display_notes = _re.sub(r'\[RESULTS\].*?\[/RESULTS\]', '', _display_notes, flags=_re.DOTALL).strip()
        if _display_notes:
            with st.expander(f"📝 Season Notes ({len(_display_notes.split(chr(10)))} lines)", expanded=False):
                st.text(_display_notes)

        st.markdown("---")

        # ══════════════════════════════════════════════════════════════
        # CALL 1 — DISCOVERY WORKSHEET
        # ══════════════════════════════════════════════════════════════
        if "Call 1" in call_number:
            with st.form("call_1_worksheet"):
                st.markdown("## 📞 CALL 1 — Discovery Call Worksheet")
                st.markdown(f"**Driver:** {full_name} | **Championship:** {selected_driver.championship or app_answers.get('championship', '—')}")

                # Pre-call info
                st.markdown("---")
                st.markdown("### 📋 Pre-Call Intel")
                pc1, pc2 = st.columns(2)
                q_response_time = pc1.text_input("⏱️ Response Time (how quickly did they book?)", key="q_response_time")
                q_response_type = pc2.text_input("📧 Response Type (DM/email/phone)", key="q_response_type")

                st.markdown(f"**Full Name:** {full_name}")

                # IMPROVE scores
                st.markdown("---")
                st.markdown("### 📊 IMPROVE Scores")
                if day1_score:
                    st.markdown(f"**Day 1 — 7 Biggest Mistakes Score: {day1_score}/100**")
                if day2_scores:
                    scores_display = " | ".join([f"**{pillar_labels.get(k,k)}:** {v:.0f}/10" for k, v in day2_scores.items()])
                    st.markdown(f"Day 2 Pillar Scores: {scores_display}")
                    if weakest_pillar:
                        st.warning(f"⚠️ **Weakest area: {weakest_pillar} ({weakest_score:.0f}/10)** — This is their #1 priority to fix")
                else:
                    st.info("No pillar scores loaded — ask about their marks out of 10 on the app")

                q_app_marks = st.text_input("Marks out of 10 on app (if different/updated)", value=f"{weakest_pillar}: {weakest_score:.0f}/10" if weakest_pillar else "", key="q_app_marks")

                # Race Results
                if best_pos:
                    st.markdown(f"**Best result:** P{best_pos} at {best_circuit}" if best_circuit else f"**Best result:** P{best_pos}")
                if trend in ("improving", "declining"):
                    st.markdown(f"**Performance trend:** {'📈 On the incline' if trend == 'improving' else '📉 On the decline'}")

                # ── THE DETECTIVE — Pain Amplification ──
                st.markdown("---")
                st.markdown("### 🔍 The Detective — Pain Amplification")

                q_struggles = st.text_area(
                    '❓ "What are the top 2 or 3 struggles you are facing during a race weekend?"',
                    value=app_answers.get('mental_barrier', ''),
                    height=80, key="q_struggles"
                )

                q_how_long = st.text_area(
                    f'❓ "How long has ___ been holding you back?" (Use their words from above)',
                    height=60, key="q_how_long"
                )

                q_emotional_cost = st.text_area(
                    f'❓ "What\'s this costing you emotionally? How does it feel when you\'re ___?"',
                    height=60, key="q_emotional_cost"
                )

                q_tried = st.text_area(
                    '❓ "What have you tried to fix this? What happened?"',
                    height=60, key="q_tried"
                )

                q_investment_worksheet = st.text_area(
                    '❓ "You completed the Racer Investment Worksheet on Day 1 of the Free Training? How much have you spent on equipment/track time trying to go faster?"',
                    height=60, key="q_investment_worksheet"
                )

                q_season_goal = st.text_area(
                    '❓ "What\'s your goal for this season?"',
                    value=app_answers.get('season_goal', ''),
                    height=60, key="q_season_goal"
                )

                q_managed = st.text_area(
                    '❓ "___ struggling with this? How have you managed that long?"',
                    height=60, key="q_managed"
                )

                q_no_change = st.text_area(
                    '❓ "If nothing changes, where will you be next season?"',
                    height=60, key="q_no_change"
                )

                # ── THE FRAMEWORK — Present Solution ──
                st.markdown("---")
                st.markdown("### 🎯 The Framework — Present Your Solution")
                st.markdown(f"""
> *"Based on what you've shared, here's exactly what needs to happen. Grab a pen...*
>
> *The lowest score is their No. 1 Priority — **{weakest_pillar or '___'}***
>
> *You can either do that alone or you can get my help to do it if we both think it's a good fit"*
""")

                # ── THE PROGRAMME REVEAL ──
                st.markdown("---")
                st.markdown("### 💎 Programme Reveal")
                st.markdown("""
> *"Where do you want to go from here?"*
> *(They'll ask about working together)*
>
> *"Happy to explain. Just remember — I won't be able to offer you anything today. I need to speak with the other riders first and really think about this. But I can walk you through what working together looks like."*
> **[PAUSE — Let that land]**
>
> *"Remember the 5 pillars from the free training? Here's what you actually get in the programme:*
> - **Pillar 1 — MINDSET:** Daily training videos + 1-on-1 coaching calls after each module
> - **Pillar 2 — PREPARATION:** Complete race weekend structure — fast from the out-lap
> - **Pillar 3 — FLOW:** 6 modules on getting into flow state consistently
> - **Pillar 4 — FEEDBACK:** The In The Zone app — works without signal, tells you what to focus on next session
> - **Pillar 5 — FUNDING:** The complete sponsorship blueprint"*
""")

                q_scale_fix = st.text_area(
                    f'❓ "On a scale of 1-10, how close do you think that would come to fixing ___?"',
                    height=40, key="q_scale_fix"
                )

                # ── THE TIEDOWNS ──
                st.markdown("---")
                st.markdown("### 🔗 Tiedowns & Summary")

                q_struggling_with = st.text_area("You're struggling with...", value=q_struggles if q_struggles else app_answers.get('mental_barrier', ''), height=40, key="q_struggling_with")
                q_youve_tried = st.text_area("You've tried...", value=q_tried if q_tried else "", height=40, key="q_youve_tried")
                st.markdown(f'> *"You need... A proven process that\'s going to give you the way to perform consistently at your best to achieve **{q_season_goal if q_season_goal else "___"}**"*')

                st.markdown("""
> *"Can you commit to 20 minutes a day for the training?"*
> *"Does this sound like something you'd want to do?"*
> **Excellent, do you have any other questions?**
> ⏸️ *WAIT FOR THEM TO SAY "HOW MUCH IS IT"*
""")

                # ── THE INVESTMENT ──
                st.markdown("---")
                st.markdown("### 💰 Investment")
                st.markdown("""
> *"Good question. Now, I am not saying we have space right now; I am happy to discuss the payment options, but I need to be clear I have to take the other calls before we know whether there is space on the programme or not. Is that okay?"*
>
> **Plan A:** £4,000 — one-time payment, immediate full access
> **Plan B:** 8 months × £550/month
> **Plan C:** 16 months × £275/month
>
> *"On all plans you have lifetime access. Most riders choose Plan B. Which one makes the most sense for your budget?"*
""")

                q_plan_chosen = st.text_input("💳 Which plan did they lean towards?", key="q_plan_chosen")

                # ── TWO PICTURES ──
                st.markdown("---")
                st.markdown("### 🖼️ Paint Two Pictures")
                _struggles_text = q_struggling_with or app_answers.get('mental_barrier', '___')
                _goal_text = q_season_goal or app_answers.get('season_goal', '___')
                st.markdown(f"""
> *"So {name}, you've got a decision to make in the next 24 hours. Let me paint two pictures:*
> *3 months from now you're going to be somewhere and you're going to be someone — the question is, who will you be?"*
>
> **Version 1 — No Action:** *"Things stay the same... 3 months from now you're still **{_struggles_text}**, still frustrated, still finishing not where you want to be. Nowhere near your goal of **{_goal_text}**. You saved 4k but what has it actually cost you?"*
>
> **Version 2 — You Invest:** *"3 months from now: You've applied the mental frameworks, you understand why you struggled before, you've had breakthrough weekends. People are taking notice. Instead of frustration, you're driving home knowing no one on your bike with your budget on that track could have ridden better. AND you've secured your first £12k sponsor."*
>
> *"Which one do you want to be?"*
""")

                # ── BOOK CALL 2 ──
                st.markdown("---")
                st.markdown("### 📅 Book Call 2")
                st.markdown("> **Book Second Call:** [calendly.com/caminocoaching/rider-fit-call](https://calendly.com/caminocoaching/rider-fit-call)")

                q_missed = st.text_area('❓ "Is there anything you feel you\'ve missed that I can add to my notes before we head off?"', height=60, key="q_missed")

                st.markdown("---")
                save_call1 = st.form_submit_button("💾 Save Call 1 Notes", use_container_width=True, type="primary")

            if save_call1:
                # Save all answers to driver notes
                call_notes = f"""[{datetime.now().strftime('%d %b %Y %H:%M')} 📞 CALL 1 WORKSHEET]
Struggles: {q_struggles}
How long: {q_how_long}
Emotional cost: {q_emotional_cost}
Tried before: {q_tried}
Investment so far: {q_investment_worksheet}
Season goal: {q_season_goal}
If no change: {q_no_change}
Scale (programme fix): {q_scale_fix}
Plan chosen: {q_plan_chosen}
Missed anything: {q_missed}
[/CALL 1 WORKSHEET]"""
                existing = selected_driver.notes or ""
                selected_driver.notes = f"{call_notes}\n\n{existing}" if existing else call_notes
                dashboard.add_new_driver(
                    selected_driver.email, selected_driver.first_name, selected_driver.last_name,
                    selected_driver.facebook_url or "", ig_url=selected_driver.instagram_url or "",
                    championship=selected_driver.championship or "", notes=selected_driver.notes
                )
                st.toast(f"✅ Call 1 notes saved for {full_name}")
                st.balloons()

                # Downloadable text file
                _download_text = f"""STRATEGY CALL 1 — DISCOVERY WORKSHEET
{'='*50}
Driver: {full_name}
Date: {datetime.now().strftime('%d %b %Y %H:%M')}
Championship: {selected_driver.championship or app_answers.get('championship', '—')}

PRE-CALL DATA
{'─'*30}
Day 1 IMPROVE Score: {day1_score or '—'}/100
Weakest Pillar: {weakest_pillar or '—'}{f' ({weakest_score:.0f}/10)' if weakest_pillar else ''}
Best Result: {f'P{best_pos}' if best_pos else '—'}{f' at {best_circuit}' if best_circuit else ''}
Best Lap: {best_lap or '—'}
Trend: {trend.title()}
Seriousness: {app_answers.get('seriousness_scale', '—')}/10

CALL 1 Q&A
{'─'*30}
Struggles: {q_struggles}
How long: {q_how_long}
Emotional cost: {q_emotional_cost}
Tried before: {q_tried}
Investment so far: {q_investment_worksheet}
Season goal: {q_season_goal}
If nothing changes: {q_no_change}
Scale (programme fix): {q_scale_fix}
Plan chosen: {q_plan_chosen}
Missed anything: {q_missed}
"""
                st.download_button(
                    "📥 Download Call 1 Notes",
                    _download_text,
                    file_name=f"Call1_{full_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.txt",
                    mime="text/plain",
                    use_container_width=True
                )

        # ══════════════════════════════════════════════════════════════
        # CALL 2 — CLOSE WORKSHEET
        # ══════════════════════════════════════════════════════════════
        else:
            # Try to load Call 1 notes for recap
            _c1_struggles = ""
            _c1_tried = ""
            _c1_goal = app_answers.get('season_goal', '')
            _c1_plan = ""
            import re as _re
            if selected_driver.notes:
                _c1m = _re.search(r'\[.*CALL 1 WORKSHEET\](.*?)\[/CALL 1 WORKSHEET\]', selected_driver.notes, _re.DOTALL)
                if _c1m:
                    _c1_text = _c1m.group(1)
                    for line in _c1_text.split('\n'):
                        if line.startswith('Struggles:'):
                            _c1_struggles = line.replace('Struggles:', '').strip()
                        elif line.startswith('Tried before:'):
                            _c1_tried = line.replace('Tried before:', '').strip()
                        elif line.startswith('Season goal:'):
                            _c1_goal = line.replace('Season goal:', '').strip()
                        elif line.startswith('Plan chosen:'):
                            _c1_plan = line.replace('Plan chosen:', '').strip()

            with st.form("call_2_worksheet"):
                st.markdown("## 🤝 CALL 2 — Close Worksheet")
                st.markdown(f"**Driver:** {full_name}")

                # ── RECAP ──
                st.markdown("---")
                st.markdown("### 🔄 Recap from Call 1")
                st.markdown('> *"Ok so just let me recap what we covered last time..."*')

                q2_want = st.text_area('You want to ___', value=_c1_goal, height=40, key="q2_want")
                q2_struggling = st.text_area("You're ___ with ___", value=_c1_struggles, height=40, key="q2_struggling")
                q2_tried = st.text_area("You have tried ___", value=_c1_tried, height=40, key="q2_tried")
                st.markdown(f'> *"The £4,000 investment felt manageable"*')
                st.markdown('> *"You\'re ready to work on the mental side rather than more equipment. Still accurate?"*')

                # ── COACHABILITY ──
                st.markdown("---")
                st.markdown("### 🧠 Coachability Check")
                st.markdown("""
> *"One question: How coachable are you?"*
> *"We've found our process works best when riders follow the steps exactly."*
> *"I only like to work with people who are coachable, open to feedback, and ready to take action quickly."*
> *"Is that you?"*
> *"What about your commitment level — are you genuinely ready for daily practice?"*
> *"Your racing, home, and work schedule allows for proper implementation?"*
> *"Perfect, just wanted to be absolutely sure."*
""")
                q2_coachable = st.text_area("Their response to coachability check:", height=60, key="q2_coachable")

                # ── GOOD NEWS / BAD NEWS ──
                st.markdown("---")
                st.markdown("### 🎉 Good News / Bad News")
                st.markdown("""
> *"I've got some good news and some bad news for you. Which would you like first?"*
>
> **GOOD NEWS:** *"I'm very keen to have you in the program and would like to offer you a spot. Congratulations!"*
>
> **BAD NEWS:** *"I'm afraid you're going to be stuck with the Camino family for the next 6 months!"*
>
> *"How are you feeling — excited, nervous, or a bit of both?"*
""")
                q2_feeling = st.text_area("How are they feeling?", height=40, key="q2_feeling")

                # ── CLOSE ──
                st.markdown("---")
                st.markdown("### ✅ Close & Payment")
                st.markdown(f"""
> *"Here's what happens next: Once we take payment you'll get instant access to the training platform, and we'll book your first kickoff call."*
>
> *"From my notes, you preferred **{_c1_plan or '[payment option]'}**, so I have that ready. When you're ready, I'll take your card details."*
>
> *"You've made an excellent choice. You will get an email in the next 30 minutes."*
""")

                # ── 6-MONTH PICTURE (if needed) ──
                st.markdown("---")
                st.markdown("### 🖼️ Six Month Pictures (if they need a nudge)")
                st.markdown("""
> **Picture 1:** *"Six months from now, you've mastered the mental game. You're the rider setting lap records, enjoying every session, achieving goals you didn't think possible."*
>
> **Picture 2:** *"Six months of the same struggles, same frustrations, still wondering if you'll ever breakthrough."*
>
> *"Which future do you prefer? Then let's make Picture 1 your reality."*
""")

                # ── OBJECTION HANDLING ──
                st.markdown("---")
                st.markdown("### 🛡️ Objection Handling")
                st.markdown("""
> - *"What has changed between our first call and now?"*
> - *"What's the real reason you are hesitating?"*
> - *"What would you need to feel comfortable moving forward?"*
> - *"Would it help to get started with the Starter plan and upgrade later?"*
> - *"Should we set a check-in date to finalise your spot?"*
>
> **"I still need to think about it":**
> - Ask what's changed since the first call?
> - If it's logistics of moving money: take the £500 deposit. **Company policy!**
""")
                q2_objections = st.text_area("Objections raised & how handled:", height=80, key="q2_objections")
                q2_outcome = st.selectbox("Call Outcome", ["✅ CLOSED — Payment taken", "📅 Call 3 booked", "❌ Not a fit", "⏳ Thinking about it"], key="q2_outcome")

                st.markdown("---")
                save_call2 = st.form_submit_button("💾 Save Call 2 Notes", use_container_width=True, type="primary")

            if save_call2:
                call_notes = f"""[{datetime.now().strftime('%d %b %Y %H:%M')} 🤝 CALL 2 WORKSHEET]
Outcome: {q2_outcome}
Coachability response: {q2_coachable}
Feeling: {q2_feeling}
Objections: {q2_objections}
[/CALL 2 WORKSHEET]"""
                existing = selected_driver.notes or ""
                selected_driver.notes = f"{call_notes}\n\n{existing}" if existing else call_notes
                # Update stage based on outcome
                if "CLOSED" in q2_outcome:
                    selected_driver.current_stage = FunnelStage.CLIENT
                    selected_driver.sale_closed_date = datetime.now()
                elif "Not a fit" in q2_outcome:
                    selected_driver.current_stage = FunnelStage.NOT_A_FIT
                dashboard.add_new_driver(
                    selected_driver.email, selected_driver.first_name, selected_driver.last_name,
                    selected_driver.facebook_url or "", ig_url=selected_driver.instagram_url or "",
                    championship=selected_driver.championship or "", notes=selected_driver.notes
                )
                st.toast(f"✅ Call 2 notes saved for {full_name}")
                if "CLOSED" in q2_outcome:
                    st.balloons()

                # Downloadable text file
                _download_text2 = f"""STRATEGY CALL 2 — CLOSE WORKSHEET
{'='*50}
Driver: {full_name}
Date: {datetime.now().strftime('%d %b %Y %H:%M')}
Outcome: {q2_outcome}

RECAP FROM CALL 1
{'─'*30}
Wants to: {q2_want}
Struggling with: {q2_struggling}
Has tried: {q2_tried}

CALL 2 Q&A
{'─'*30}
Coachability response: {q2_coachable}
Feeling after offer: {q2_feeling}
Objections: {q2_objections}
Outcome: {q2_outcome}
"""
                st.download_button(
                    "📥 Download Call 2 Notes",
                    _download_text2,
                    file_name=f"Call2_{full_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.txt",
                    mime="text/plain",
                    use_container_width=True
                )

    # ══════════════════════════════════════════════════════════════════
    # MODE 2: POST-CALL ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    elif mode == "📊 Post-Call Analysis":
        st.markdown("### 📊 Post-Call Analysis & Coaching")
        st.info("Paste a call transcript and get scored against the Championship Strategy Call Framework.")

        transcript = st.text_area(
            "📄 Paste Call Transcript",
            height=300, key="postcall_transcript",
            placeholder="Paste the full call transcript here...\n\ne.g.\n@0:00 - Craig: Hey Sam, how are you doing?\n@0:05 - Sam: Good thanks, yeah not too bad..."
        )

        pc1, pc2 = st.columns(2)
        call_type = pc1.radio("Call Type", ["Call 1 (Discovery)", "Call 2 (Close)"], key="postcall_type", horizontal=True)
        driver_name = pc2.text_input("Driver/Rider Name (optional)", key="postcall_name")

        if st.button("🔍 Analyze Call", key="postcall_analyze", type="primary", use_container_width=True) and transcript:
            with st.spinner("Analyzing transcript against framework..."):
                analysis = analyze_call_transcript(transcript)
                report = format_analysis_report(analysis)
                st.session_state['call_analysis'] = report
                st.session_state['call_analysis_raw'] = analysis

        if 'call_analysis' in st.session_state:
            st.markdown("---")

            # Score cards
            raw = st.session_state.get('call_analysis_raw', {})
            sc1, sc2, sc3, sc4, sc5 = st.columns(5)
            overall = raw.get('overall_score', 0)
            color = "normal" if overall >= 70 else ("off" if overall >= 50 else "off")
            sc1.metric("Overall", f"{overall}/100")
            sc2.metric("Adherence", f"{raw.get('adherence_score', 0)}%")
            sc3.metric("Pain Depth", f"{raw.get('pain_amplification_score', 0)}%")
            sc4.metric("Push/Pull", f"{raw.get('push_pull_score', 0)}%")
            sc5.metric("Objections", f"{raw.get('objection_handling_score', 0)}%")

            st.markdown(st.session_state['call_analysis'])

            # Save analysis to driver if identified
            if driver_name:
                matching = [d for d in dashboard.drivers.values()
                           if driver_name.lower() in d.full_name.lower()]
                if matching and st.button(f"💾 Save Analysis to {matching[0].full_name}'s Notes", key="save_analysis"):
                    driver = matching[0]
                    ts = datetime.now().strftime("%d %b %Y %H:%M")
                    note = f"[{ts} 📞] Call Analysis — Score: {overall}/100\n{st.session_state['call_analysis']}"
                    existing = driver.notes or ""
                    driver.notes = f"{note}\n\n{existing}" if existing else note
                    dashboard.add_new_driver(
                        driver.email, driver.first_name, driver.last_name,
                        driver.facebook_url or "", ig_url=driver.instagram_url or "",
                        championship=driver.championship or "", notes=driver.notes
                    )
                    st.toast(f"✅ Analysis saved to {driver.full_name}")
                    st.rerun()

    # ══════════════════════════════════════════════════════════════════
    # MODE 3: APPLICATION FORM (replaces Typeform)
    # ══════════════════════════════════════════════════════════════════
    elif mode == "📝 Application Form":
        st.markdown("### 📝 Strategy Call Application Form")
        st.info("Fill in or paste the candidate's application answers. This replaces the Typeform process.")

        with st.form("application_form"):
            form_answers = {}
            for q in APPLICATION_QUESTIONS:
                if q["type"] == "text":
                    form_answers[q["id"]] = st.text_input(q["label"], key=f"app_{q['id']}")
                elif q["type"] == "textarea":
                    form_answers[q["id"]] = st.text_area(q["label"], key=f"app_{q['id']}", height=80)
                elif q["type"] == "select":
                    form_answers[q["id"]] = st.selectbox(q["label"], q["options"], key=f"app_{q['id']}")
                elif q["type"] == "slider":
                    form_answers[q["id"]] = str(st.slider(q["label"], q.get("min", 1), q.get("max", 10), 7, key=f"app_{q['id']}"))

            app_submitted = st.form_submit_button("💾 Save Application & Generate Pre-Call Brief", use_container_width=True, type="primary")

        if app_submitted and form_answers.get("first_name"):
            analysis = analyze_candidate_data(form_answers)
            st.success(f"✅ Application saved for {analysis['name']}")

            # Show quick analysis
            st.markdown(f"""
### 🎯 Quick Assessment: {analysis['name']}
| Indicator | Status |
|-----------|--------|
| **Coachability** | {analysis['coachability']} |
| **Financial** | {analysis['financial_flag']} |
| **Pain Gap** | {analysis['pain_gap']} |
| **Seriousness** | {analysis['seriousness']} |
""")

            # Try to match to existing driver and save
            email = form_answers.get("email", "")
            if email:
                matching = [d for d in dashboard.drivers.values() if d.email == email]
                if matching:
                    driver = matching[0]
                    app_json = _json.dumps(form_answers)
                    tag = f"[STRATEGY_APP]{app_json}[/STRATEGY_APP]"
                    import re as _re
                    existing = (driver.notes or "")
                    existing = _re.sub(r'\[STRATEGY_APP\].*?\[/STRATEGY_APP\]', '', existing, flags=_re.DOTALL).strip()
                    driver.notes = f"{tag}\n{existing}" if existing else tag
                    dashboard.add_new_driver(
                        driver.email, driver.first_name, driver.last_name,
                        driver.facebook_url or "", ig_url=driver.instagram_url or "",
                        championship=driver.championship or "", notes=driver.notes
                    )
                    st.toast(f"✅ Saved to {driver.full_name}'s record")

            st.session_state['precall_answers'] = form_answers
            st.info("💡 Switch to **Pre-Call Preparation** mode to generate the personalized call script.")

    # ══════════════════════════════════════════════════════════════════
    # MODE 4: CALL SCRIPTS REFERENCE
    # ══════════════════════════════════════════════════════════════════
    elif mode == "📖 Call Scripts":
        st.markdown("### 📖 Call Script Reference Library")

        is_driver_view = st.checkbox("🏎️ Show Driver (car) terminology", value=True, key="scripts_driver_mode")

        tab1, tab2, tab3 = st.tabs(["📞 Call 1 — Discovery", "🤝 Call 2 — Close", "📋 AdClients Push/Pull"])

        with tab1:
            script = CALL_1_FRAMEWORK.format(
                name="[NAME]", detective_notes="[Use your pre-call prep notes here]",
                dream_notes="[Mirror their language from the application]",
                their_struggles="[From application]", what_theyve_tried="[Ask on call]",
                their_goal="[From application]", their_current_struggle="[From application]",
                riders_or_drivers="drivers" if is_driver_view else "riders",
                bike_or_car="car" if is_driver_view else "bike",
                ridden_or_driven="driven" if is_driver_view else "ridden",
                specific_thing_1="[SPECIFIC THING 1]", specific_thing_2="[SPECIFIC THING 2]",
            )
            if is_driver_view:
                script = swap_terminology(script, to_driver=True)
            st.markdown(script)

        with tab2:
            call2 = CALL_2_FRAMEWORK
            if is_driver_view:
                call2 = swap_terminology(call2, to_driver=True)
            st.markdown(call2)

        with tab3:
            # Load from knowledge base if available
            kb_path = os.path.join("data", "strategy_call_knowledge.json")
            if os.path.exists(kb_path):
                with open(kb_path, 'r') as f:
                    kb = _json.load(f)
                adclients = kb.get("adclients_close", "AdClients framework not loaded.")
                if is_driver_view:
                    adclients = swap_terminology(adclients, to_driver=True)
                st.markdown(adclients)
            else:
                st.warning("AdClients Push/Pull framework not loaded. Place the PDF in the project root.")

        # Gold Standard Examples
        st.markdown("---")
        st.markdown("### 🏆 Gold Standard Calls — Sam Hirst & Angela Brunson")
        st.caption("Both CLOSED via the 2-call process. Study these patterns to aim for 58%+ conversion.")

        gs_tabs = st.tabs(["📋 Sam Hirst — Key Moments", "📋 Angela Brunson — Key Moments", "📄 Full Transcripts"])

        with gs_tabs[0]:
            gs = GOLD_STANDARD["sam_hirst"]
            st.markdown(f"**Outcome: {gs['outcome']}**")
            st.markdown("#### Call 1 — Discovery")
            for stage, detail in gs["call_1_highlights"].items():
                st.markdown(f"- **{stage.replace('_', ' ').title()}:** {detail}")
            st.markdown("#### Call 2 — Close")
            for stage, detail in gs["call_2_highlights"].items():
                st.markdown(f"- **{stage.replace('_', ' ').title()}:** {detail}")
            st.markdown("#### Key Techniques to Replicate")
            for t in gs["key_techniques"]:
                st.markdown(f"- ✅ {t}")

        with gs_tabs[1]:
            gs = GOLD_STANDARD["angela_brunson"]
            st.markdown(f"**Outcome: {gs['outcome']}**")
            st.markdown("#### Call 1 — Discovery")
            for stage, detail in gs["call_1_highlights"].items():
                st.markdown(f"- **{stage.replace('_', ' ').title()}:** {detail}")
            st.markdown("#### Call 2 — Close")
            for stage, detail in gs["call_2_highlights"].items():
                st.markdown(f"- **{stage.replace('_', ' ').title()}:** {detail}")
            st.markdown("#### Key Techniques to Replicate")
            for t in gs["key_techniques"]:
                st.markdown(f"- ✅ {t}")

        with gs_tabs[2]:
            kb_path = os.path.join("data", "strategy_call_knowledge.json")
            if os.path.exists(kb_path):
                with open(kb_path, 'r') as f:
                    kb = _json.load(f)

                example_tabs = st.tabs(["Sam Hirst Call 1", "Sam Hirst Call 2", "Angela Brunson Call 1", "Angela Brunson Call 2"])
                examples = [
                    ("sam_hirst_call1", "Sam Hirst Call 1"),
                    ("sam_hirst_call2", "Sam Hirst Call 2"),
                    ("angel_brunson_call1", "Angela Brunson Call 1"),
                    ("angel_brunson_call2", "Angela Brunson Call 2"),
                ]
                for t, (key, title) in zip(example_tabs, examples):
                    with t:
                        content = kb.get(key, f"{title} not available.")
                        with st.expander(f"📄 Full Transcript — {title}", expanded=False):
                            st.text(content[:10000] if len(content) > 10000 else content)
                            if len(content) > 10000:
                                st.caption(f"Showing first 10,000 of {len(content)} characters")


# ==============================================================================
# ADMIN VIEW
# ==============================================================================
def render_admin(dashboard, drivers):
    st.subheader("⚙️ Admin")

    # MANUAL ADD DRIVER
    with st.expander("👤 Manually Add New Driver", expanded=False):
        st.write("Add a single driver who didn't come from an automated source.")
        
        with st.form("manual_add_driver_form"):
            c1, c2 = st.columns(2)
            new_first = c1.text_input("First Name")
            new_last = c2.text_input("Last Name")
            new_email = st.text_input("Email")
            new_champ = st.text_input("Championship (Optional)")
            new_fb = st.text_input("Facebook URL (Optional)")
            new_ig = st.text_input("Instagram URL (Optional)")
            new_notes = st.text_area("Initial Notes (Optional)")
            
            if st.form_submit_button("💾 Save to Database"):
                if new_first:
                    # Handle missing email
                    final_email = new_email.strip()
                    if not final_email:
                        slug = f"{new_first} {new_last}".lower().strip().replace(' ', '_')
                        slug = "".join([c for c in slug if c.isalnum() or c == '_'])
                        final_email = f"no_email_{slug}"
                    
                    success = dashboard.add_new_driver(
                        final_email, 
                        new_first.strip(), 
                        new_last.strip(), 
                        fb_url=new_fb.strip(), 
                        ig_url=new_ig.strip(),
                        championship=new_champ.strip(),
                        notes=new_notes.strip()
                    )
                    
                    if success:
                        st.toast(f"✅ Added {new_first} {new_last} to database!", icon="🎉")
                        dashboard.update_driver_stage(final_email, FunnelStage.CONTACT)
                        st.rerun()
                    else:
                        st.error("Failed to add driver. Name/Email might already exist.")
                else:
                    st.warning("First Name is required.")

    # BULK IMPORT TO DATABASE
    with st.expander("📥 Import to Database", expanded=False):
        st.write("Bulk import drivers into the Airtable database from CSV or a pasted list.")

        import_method = st.radio(
            "Import method",
            ["📋 Paste Names", "📄 Upload CSV"],
            horizontal=True,
            key="admin_import_method"
        )

        _import_rows = []  # list of dicts: {first_name, last_name, email, championship, fb_url, ig_url, phone, notes}

        if import_method == "📄 Upload CSV":
            uploaded_file = st.file_uploader(
                "Upload CSV file",
                type=["csv"],
                help="CSV with columns like: First Name, Last Name, Email, Championship, FB URL, IG URL, Phone",
                key="admin_csv_upload"
            )
            if uploaded_file:
                try:
                    import io, csv
                    content = uploaded_file.read().decode('utf-8', errors='replace')
                    reader = csv.DictReader(io.StringIO(content))
                    raw_rows = list(reader)

                    if not raw_rows:
                        st.warning("CSV file is empty.")
                    else:
                        # Show detected columns
                        st.caption(f"Detected {len(raw_rows)} rows with columns: {', '.join(raw_rows[0].keys())}")

                        # Map columns flexibly (case-insensitive)
                        for row in raw_rows:
                            lrow = {str(k).lower().strip(): str(v).strip() for k, v in row.items() if v}

                            first = (lrow.get('first name', '') or lrow.get('first_name', '') or
                                     lrow.get('firstname', '')).strip()
                            last = (lrow.get('last name', '') or lrow.get('last_name', '') or
                                    lrow.get('lastname', '') or lrow.get('surname', '')).strip()

                            # Try full name column if no first/last
                            full_name = (lrow.get('full name', '') or lrow.get('full_name', '') or
                                         lrow.get('name', '') or lrow.get('driver', '') or
                                         lrow.get('driver name', '')).strip()
                            if full_name and not first and not last:
                                parts = full_name.split()
                                first = parts[0] if parts else ''
                                last = ' '.join(parts[1:]) if len(parts) > 1 else ''

                            email = (lrow.get('email', '') or lrow.get('email address', '') or
                                     lrow.get('email_address', '')).strip()
                            champ = (lrow.get('championship', '') or lrow.get('champ', '') or
                                     lrow.get('series', '') or lrow.get('class', '')).strip()
                            fb = (lrow.get('fb url', '') or lrow.get('fb_url', '') or
                                  lrow.get('facebook', '') or lrow.get('facebook url', '')).strip()
                            ig = (lrow.get('ig url', '') or lrow.get('ig_url', '') or
                                  lrow.get('instagram', '') or lrow.get('instagram url', '')).strip()
                            phone = (lrow.get('phone', '') or lrow.get('phone number', '') or
                                     lrow.get('phone_number', '') or lrow.get('tel', '')).strip()
                            notes = (lrow.get('notes', '') or lrow.get('note', '')).strip()

                            if first or last or email:
                                _import_rows.append({
                                    'first_name': first.title() if first else '',
                                    'last_name': last.title() if last else '',
                                    'email': email,
                                    'championship': champ,
                                    'fb_url': fb,
                                    'ig_url': ig,
                                    'phone': phone,
                                    'notes': notes,
                                })
                        st.success(f"✅ Parsed {len(_import_rows)} valid drivers from CSV")
                except Exception as csv_err:
                    st.error(f"Error reading CSV: {csv_err}")

        else:  # Paste Names
            pasted_text = st.text_area(
                "Paste driver names (one per line)",
                height=200,
                placeholder="John Smith\nJane Doe\nMike Johnson",
                key="admin_paste_names"
            )
            paste_champ = st.text_input(
                "Championship for all (optional)",
                key="admin_paste_champ",
                help="Applied to all pasted drivers"
            )

            if pasted_text:
                lines = [l.strip() for l in pasted_text.strip().split('\n') if l.strip() and len(l.strip()) > 2]
                for line in lines:
                    # Clean name: remove numbers, special chars at start (position numbers)
                    import re as _re_import
                    clean = _re_import.sub(r'^[\d\.\)\-\s]+', '', line).strip()
                    clean = "".join([c for c in clean if c.isalpha() or c == ' ' or c == '-' or c == "'"]).strip()
                    if not clean or len(clean) < 2:
                        continue
                    parts = clean.split()
                    first = parts[0].title() if parts else ''
                    last = ' '.join(p.title() for p in parts[1:]) if len(parts) > 1 else ''
                    _import_rows.append({
                        'first_name': first,
                        'last_name': last,
                        'email': '',
                        'championship': paste_champ.strip() if paste_champ else '',
                        'fb_url': '',
                        'ig_url': '',
                        'phone': '',
                        'notes': '',
                    })
                if _import_rows:
                    st.success(f"✅ {len(_import_rows)} drivers ready to import")

        # PREVIEW & IMPORT
        if _import_rows:
            # Check for duplicates against existing database
            _new_rows = []
            _dup_rows = []
            for row in _import_rows:
                full = f"{row['first_name']} {row['last_name']}".strip().lower()
                is_dup = False
                for existing in drivers.values():
                    existing_full = existing.full_name.lower() if hasattr(existing, 'full_name') else ''
                    if existing_full and full and existing_full == full:
                        is_dup = True
                        break
                if is_dup:
                    _dup_rows.append(row)
                else:
                    _new_rows.append(row)

            if _dup_rows:
                st.warning(f"⚠️ {len(_dup_rows)} drivers already in database (will be skipped): "
                           + ", ".join(f"{r['first_name']} {r['last_name']}" for r in _dup_rows[:10])
                           + ("..." if len(_dup_rows) > 10 else ""))

            if _new_rows:
                # Preview table
                preview_df = pd.DataFrame([{
                    'Name': f"{r['first_name']} {r['last_name']}".strip(),
                    'Email': r['email'] or '—',
                    'Championship': r['championship'] or '—',
                    'FB': '✅' if r['fb_url'] else '—',
                    'IG': '✅' if r['ig_url'] else '—',
                } for r in _new_rows[:50]])
                st.dataframe(preview_df, use_container_width=True, hide_index=True)
                if len(_new_rows) > 50:
                    st.caption(f"Showing first 50 of {len(_new_rows)} drivers")

                # Import button
                if st.button(f"⚡ Import {len(_new_rows)} New Drivers to Database", type="primary",
                             key="admin_bulk_import_btn", use_container_width=True):
                    added = 0
                    failed = 0
                    progress = st.progress(0)

                    for idx, row in enumerate(_new_rows):
                        first = row['first_name']
                        last = row['last_name']
                        email = row['email'].strip()
                        if not email:
                            slug = f"{first} {last}".lower().strip().replace(' ', '_')
                            slug = "".join([c for c in slug if c.isalnum() or c == '_'])
                            email = f"no_email_{slug}"

                        try:
                            success = dashboard.add_new_driver(
                                email, first, last,
                                fb_url=row['fb_url'],
                                ig_url=row['ig_url'],
                                championship=row['championship'],
                                notes=row['notes']
                            )
                            if success:
                                dashboard.update_driver_stage(email, FunnelStage.CONTACT)
                                added_driver = dashboard._find_driver(email)
                                if added_driver:
                                    added_driver.outreach_date = datetime.now()
                                    if row['phone']:
                                        added_driver.phone = row['phone']
                                added += 1
                            else:
                                failed += 1
                        except Exception as import_err:
                            print(f"Import error for {first} {last}: {import_err}")
                            failed += 1

                        progress.progress((idx + 1) / len(_new_rows))

                    if added > 0:
                        st.success(f"✅ Successfully imported {added} drivers into the database!")
                    if failed > 0:
                        st.warning(f"⚠️ {failed} drivers failed to import.")
                    st.cache_resource.clear()
                    st.rerun()
            else:
                st.info("All drivers are already in the database. Nothing to import.")

    # PLATFORM INTEGRATIONS (Airtable)
    with st.expander("🔌 Airtable Sync", expanded=False):
        st.caption("Push all local data updates to Airtable Master Record.")
        if st.button("🔄 Sync Database to Airtable", use_container_width=True):
             with st.spinner("Syncing Database to Airtable..."):
                 count = dashboard.data_loader.sync_database_to_airtable()
                 if count > 0:
                     st.success(f"✅ Successfully synced {count} records to Airtable!")
                     st.cache_resource.clear()
                 else:
                     st.warning("No records synced (Check Airtable connection).")

    # DATA SOURCE INFO
    with st.expander("☁️ Data Sources", expanded=False):
        st.success("Airtable is the master record. Google Sheets feed assessment data automatically.")
        st.write(f"Total Drivers in DB: {len(drivers)}")
# ==============================================================================
# MAIN LAYOUT (NAVIGATION)
# ==============================================================================

st.title("🏎️ Driver Pipeline Dashboard")

# --- DEBUG: HEALTH CHECK ---
if 'dashboard' in locals() or 'dashboard' in globals():
    # Defensive check if dashboard loaded
    if hasattr(dashboard.data_loader, 'load_report'):
        rep = dashboard.data_loader.load_report
        skipped = rep.get('skipped', 0)
        if skipped > 10:
            st.error(f"⚠️ **DATA LOADING ISSUE DETECTED** ⚠️\n\n{skipped} rows were SKIPPED. Only {rep.get('loaded', 0)} loaded.\nPlease check the 'Skipped Rows Breakdown' below.")
            with st.expander("🔍 VIEW DEBUG INFO (What went wrong?)", expanded=True):
                 st.write("**Skip Reasons:**")
                 st.json(rep.get('reasons', {}))

                 st.write("**Troubleshooting:**")
                 st.markdown("- **Missing Identity**: App couldn't find 'Email' OR 'Full Name' in the data.")
            st.divider()

# Force Reload Button (Temporary for debugging/updates)
if 'dashboard' in locals() or 'dashboard' in globals():
    # Cloud Status Indicator
    if hasattr(dashboard, 'airtable') and dashboard.airtable:
         st.sidebar.success("☁️ Cloud Storage: Connected")
    else:
         st.sidebar.warning("⚠️ Airtable not connected")

    # Sync Health Indicator — shows failed saves and retry option
    from sync_manager import render_sync_status
    _at = dashboard.airtable if hasattr(dashboard, 'airtable') else None
    render_sync_status(_at)

    st.sidebar.caption("v2.10.0 (reliable sync)")
    
    if st.sidebar.button("🔄 Force Reload / Clear Cache"):
        st.cache_resource.clear()
        st.rerun()

# 1. AUTO-SYNC GOOGLE SHEETS
# --- CACHED DATA LOADER (Module Level) ---
@st.cache_data(ttl=300) # 5 min — matches dashboard cache. Refresh App forces reload.
def load_all_sheets_data_cached():
     import concurrent.futures
     
     SHEET_CONFIG = {
         # internal_file -> accepted secret keys (aliases supported)
         "Strategy Call Application.csv": ["strategy_apps", "strategy_apps_sheet", "strategy_call_application"],
         "Podium Contenders Blueprint Registered.csv": ["blueprint_regs", "blueprint_registered", "blueprint_registrations"],
         "7 Biggest Mistakes Assessment.csv": ["seven_mistakes", "day1", "day_1", "day1_assessment", "seven_biggest_mistakes"],
         "Day 2 Self Assessment.csv": ["day2_assessment", "day2", "day_2", "day2_self_assessment"],
         "Flow Profile.csv": ["flow_profile", "flow_profile_assessment"],
         "Sleep Test.csv": ["sleep_test", "sleep"],
         "Mindset Quiz.csv": ["mindset_quiz", "mindset", "https://docs.google.com/spreadsheets/d/1JyPe2PHFdSfSZUr63YW31AOL_OMGH3Lg4_Vg_VZG6a8/edit?gid=1029796485#gid=1029796485"],
         "export (15).csv": ["race_weekend", "race_review", "race_weekend_review", "race_reviews"],
         "Xperiencify.csv": ["xperiencify", "xperiencify_export"]
     }
     
     sheet_secrets = st.secrets.get("sheets", {})
     loaded_data = {}
     missing_keys = []
     load_errors = []
     
     # 1. Identify valid tasks
     tasks = {}
     for internal_file, aliases in SHEET_CONFIG.items():
         matched_alias = None
         url = ""
         for alias in aliases:
             if alias.startswith("http"):
                 url = alias
                 matched_alias = alias
                 break
             if sheet_secrets.get(alias):
                 matched_alias = alias
                 url = sheet_secrets.get(alias, "")
                 break

         if url:
             tasks[matched_alias] = (url, internal_file)
         else:
             missing_keys.append("/".join(aliases))
     
     # 2. Execute in Parallel (Increased workers for speed)
     with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
         future_to_key = {
             executor.submit(load_google_sheet, url): (key, internal_file)
             for key, (url, internal_file) in tasks.items()
         }
         
         for future in concurrent.futures.as_completed(future_to_key):
             key, internal_file = future_to_key[future]
             try:
                 # Timeout to prevent hanging forever
                 df = future.result(timeout=15)
                 if df is not None and not df.empty:
                     loaded_data[internal_file] = df
             except Exception as exc:
                 load_errors.append(f"{key}: {exc}")
                 print(f"Error loading {key}: {exc}")
                 
     return loaded_data, missing_keys, load_errors

if HAS_GSHEETS:
    try:
        # Check for secrets
        if "connections" in st.secrets and "gsheets" in st.secrets["connections"]:
            
            # CACHED CALL
            try:
                 overrides, missing_config, sheet_errors = load_all_sheets_data_cached()
                 

            except Exception as e:
                st.error(f"GSheets Cache Error: {e}")
                overrides = {}
                sheet_errors = [str(e)]
        else:
            overrides = {}
            sheet_errors = ["No 'gsheets' connection in secrets.toml"]
    except Exception as e:
        overrides = {}
        sheet_errors = [str(e)]
else:
    overrides = {}
    sheet_errors = ["Missing streamlit_gsheets module"]

# Load Logic
try:
    # Show loading spinner on cold start (when cache is empty)
    with st.spinner("🏎️ Loading driver database..."):
        dashboard = load_dashboard_data(overrides=overrides)

    # ===========================================================================
    # FAST PATH — must run BEFORE sidebar/settings/navigation (700+ lines)
    # ===========================================================================
    # 1. Stage just updated → close dialog instantly, skip everything
    if st.session_state.pop('_stage_just_updated', False):
        if '_stage_toast' in st.session_state:
            st.toast(st.session_state.pop('_stage_toast'), icon="✅")
        else:
            st.toast("✅ Updated")
        # NOTE: Do NOT call st.stop() here — it prevents the page from rendering
        # after the dialog closes, leaving the user on a blank screen.

    # 2. Contact card dialog open → render dialog, skip heavy page render
    #    (handled later at render section, but we flag it early)

    # ===========================================================================
    # CHROME EXTENSION SAVE HANDLERS — run FIRST, before any UI rendering.
    # The extension injects query params (?save_outreach=, ?set_stage=, etc.)
    # into the Streamlit URL. These must be processed immediately so saves don't
    # time out waiting for the full UI to render (which takes 10-30 seconds).
    # After processing, st.stop() prevents unnecessary UI rendering for save-only
    # reruns — the tab is typically a background tab opened by the extension.
    # ===========================================================================
    _is_ext_save = any(p in st.query_params for p in ['save_outreach', 'set_stage', 'save_messages', 'save_url'])

    if _is_ext_save:
        # --- STAGE ORDER for advance-only logic ---
        STAGE_ORDER = [
            FunnelStage.CONTACT, FunnelStage.MESSAGED, FunnelStage.REPLIED,
            FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT,
            FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE,
            FunnelStage.SLEEP_TEST_COMPLETED, FunnelStage.MINDSET_QUIZ_COMPLETED,
            FunnelStage.FLOW_PROFILE_COMPLETED, FunnelStage.REGISTERED,
            FunnelStage.DAY1_COMPLETE, FunnelStage.DAY2_COMPLETE,
            FunnelStage.STRATEGY_CALL_BOOKED, FunnelStage.CLIENT,
        ]
        def _stage_index(stage):
            try:
                return STAGE_ORDER.index(stage)
            except ValueError:
                return -1

        # --- ?set_stage= AUTO STAGE UPDATE ---
        if "set_stage" in st.query_params and "driver" in st.query_params:
            _stage_key = st.query_params["set_stage"]
            _driver_q = st.query_params["driver"]
            _create_driver = st.query_params.get("create_driver", "") == "true"
            _social_url = st.query_params.get("social_url", "")

            _new_stage = None
            for s in FunnelStage:
                if s.name == _stage_key or s.value == _stage_key:
                    _new_stage = s
                    break

            if _new_stage:
                result, match_type = find_driver_by_identifier(dashboard, _driver_q)
                _driver_obj = result[0][0] if match_type == 'multiple' else result

                if (not _driver_obj or not hasattr(_driver_obj, 'email')) and _create_driver:
                    _clean_name = unquote_plus(_driver_q).strip()
                    _name_parts = _clean_name.split(' ', 1)
                    _first = _name_parts[0]
                    _last = _name_parts[1] if len(_name_parts) > 1 else ''
                    _slug = _clean_name.lower().replace(' ', '_')
                    _slug = "".join([c for c in _slug if c.isalnum() or c == '_'])
                    _synth_email = f"no_email_{_slug}"
                    _driver_obj = dashboard.data_loader._get_or_create_driver(_synth_email, _first, _last)
                    _driver_obj.current_stage = FunnelStage.CONTACT
                    _driver_obj.outreach_date = datetime.now()
                    _driver_obj.last_activity = datetime.now()
                    if _social_url:
                        if 'instagram.com' in _social_url:
                            _driver_obj.instagram_url = _social_url
                        elif 'facebook.com' in _social_url or 'messenger.com' in _social_url:
                            _driver_obj.facebook_url = _social_url
                    print(f"[AG SAVE] Created driver: {_driver_obj.full_name}")

                if _driver_obj and hasattr(_driver_obj, 'email'):
                    if _social_url:
                        _url_changed = False
                        if 'instagram.com' in _social_url:
                            _driver_obj.instagram_url = _social_url
                            _url_changed = True
                        elif 'facebook.com' in _social_url or 'messenger.com' in _social_url:
                            _driver_obj.facebook_url = _social_url
                            _url_changed = True
                        if _url_changed:
                            print(f"[AG SAVE] 📎 URL updated for {_driver_obj.full_name}: {_social_url[:60]}")
                            dashboard.add_new_driver(
                                _driver_obj.email, _driver_obj.first_name, _driver_obj.last_name,
                                _driver_obj.facebook_url or "", ig_url=_driver_obj.instagram_url or "",
                                championship=_driver_obj.championship or "", notes=_driver_obj.notes or ""
                            )
                    _circuit = st.query_params.get("circuit", "")
                    if _circuit and not _driver_obj.championship:
                        _driver_obj.championship = _circuit
                        dashboard.add_new_driver(
                            _driver_obj.email, _driver_obj.first_name, _driver_obj.last_name,
                            _driver_obj.facebook_url or "", ig_url=_driver_obj.instagram_url or "",
                            championship=_circuit, notes=_driver_obj.notes or ""
                        )
                    current_idx = _stage_index(_driver_obj.current_stage)
                    new_idx = _stage_index(_new_stage)
                    if new_idx > current_idx:
                        dashboard.update_driver_stage(_driver_obj.email, _new_stage)
                        print(f"[AG SAVE] ✅ {_driver_obj.full_name} → {_new_stage.value}")

            for _p in ['set_stage', 'driver', 'create_driver', 'social_url', 'circuit']:
                if _p in st.query_params:
                    del st.query_params[_p]

            # Rebuild name index so prospect list picks up this driver immediately
            if hasattr(dashboard, 'race_manager'):
                dashboard.race_manager.refresh_data()

        # --- ?save_outreach= OUTREACH + AUTO-ADVANCE TO MESSAGED ---
        if "save_outreach" in st.query_params and "driver" in st.query_params:
            _driver_q = st.query_params["driver"]
            _template_name = st.query_params.get("save_outreach", "outreach")
            _social_url = st.query_params.get("social_url", "")
            _platform = st.query_params.get("platform", "FB")
            _create_driver = st.query_params.get("create_driver", "") == "true"
            _circuit = st.query_params.get("circuit", "")
            _championship = st.query_params.get("championship", "")
            _raw_msgs = st.query_params.get("save_messages", "")
            print(f"[AG SAVE] save_outreach fired — driver={_driver_q}, template={_template_name}, platform={_platform}")

            result, match_type = find_driver_by_identifier(dashboard, _driver_q)
            _driver_obj = result[0][0] if match_type == 'multiple' else result
            print(f"[AG SAVE] Driver lookup: match_type={match_type}, found={'YES' if _driver_obj and hasattr(_driver_obj, 'email') else 'NO'}")

            if (not _driver_obj or not hasattr(_driver_obj, 'email')) and _create_driver:
                _clean_name = unquote_plus(_driver_q).strip()
                _name_parts = _clean_name.split(' ', 1)
                _first = _name_parts[0]
                _last = _name_parts[1] if len(_name_parts) > 1 else ''
                _slug = _clean_name.lower().replace(' ', '_')
                _slug = "".join([c for c in _slug if c.isalnum() or c == '_'])
                _synth_email = f"no_email_{_slug}"
                _driver_obj = dashboard.data_loader._get_or_create_driver(_synth_email, _first, _last)
                _driver_obj.current_stage = FunnelStage.CONTACT
                _driver_obj.outreach_date = datetime.now()
                _driver_obj.last_activity = datetime.now()
                print(f"[AG SAVE] Created driver: {_driver_obj.full_name}")

            if _driver_obj and hasattr(_driver_obj, 'email'):
                if _social_url:
                    if 'instagram.com' in _social_url:
                        _driver_obj.instagram_url = _social_url
                    elif 'facebook.com' in _social_url or 'messenger.com' in _social_url:
                        _driver_obj.facebook_url = _social_url
                    print(f"[AG SAVE] 📎 URL saved for {_driver_obj.full_name}: {_social_url[:60]}")
                if _championship and not _driver_obj.championship:
                    _driver_obj.championship = _championship
                if _raw_msgs:
                    import re as _re
                    _lines = []
                    for part in _raw_msgs.split('||'):
                        part = part.strip()
                        if part.startswith('Y>'):
                            _lines.append(f"  You: {part[2:]}")
                        elif part.startswith('T>'):
                            _lines.append(f"  {_driver_obj.first_name or 'Them'}: {part[2:]}")
                    if _lines:
                        _ts = datetime.now().strftime("%d %b %H:%M")
                        _thread_text = "\n".join(_lines)
                        _thread_block = f"[{_ts} 📱 {_platform}] [THREAD]\n{_thread_text}\n[/THREAD]"
                        existing = _driver_obj.notes or ""
                        existing = _re.sub(
                            r'\[\d{2} \w{3} \d{2}:\d{2} 📱[^\]]*\] \[THREAD\].*?\[/THREAD\]\n?',
                            '', existing, flags=_re.DOTALL
                        ).strip()
                        _driver_obj.notes = f"{_thread_block}\n{existing}" if existing else _thread_block

                _ts = datetime.now().strftime("%d %b %H:%M")
                _outreach_log = f"[{_ts}] 📤 {_platform} outreach sent ({_template_name})"
                _driver_obj.notes = f"{_driver_obj.notes}\n{_outreach_log}" if _driver_obj.notes else _outreach_log

                dashboard.add_new_driver(
                    _driver_obj.email, _driver_obj.first_name, _driver_obj.last_name,
                    _driver_obj.facebook_url or "", ig_url=_driver_obj.instagram_url or "",
                    championship=_driver_obj.championship or "", notes=_driver_obj.notes
                )

                print(f"[AG SAVE] {_driver_obj.full_name} current_stage={_driver_obj.current_stage}")
                if _driver_obj.current_stage == FunnelStage.CONTACT:
                    dashboard.update_driver_stage(_driver_obj.email, FunnelStage.MESSAGED)
                    print(f"[AG SAVE] ✅ {_driver_obj.full_name} advanced CONTACT → MESSAGED")
                else:
                    print(f"[AG SAVE] ℹ️ {_driver_obj.full_name} already at {_driver_obj.current_stage}")
            else:
                print(f"[AG SAVE] ⚠️ Driver not found: {_driver_q}")

            _params_to_clear = [p for p in ['save_outreach', 'driver', 'social_url', 'platform', 'create_driver', 'circuit', 'championship', 'save_messages', 'msg_platform'] if p in st.query_params]
            for _p in _params_to_clear:
                del st.query_params[_p]

            # Rebuild the name index so the prospect list picks up this driver
            # immediately (otherwise it stays as "NEW PROSPECT" until cache refresh)
            if hasattr(dashboard, 'race_manager'):
                dashboard.race_manager.refresh_data()

        # --- ?save_messages= CONVERSATION CAPTURE (standalone, no outreach) ---
        if "save_messages" in st.query_params and "driver" in st.query_params:
            _driver_q = st.query_params["driver"]
            _raw_msgs = st.query_params.get("save_messages", "")
            _platform = st.query_params.get("msg_platform", "FB")
            if _raw_msgs and _driver_q:
                result, match_type = find_driver_by_identifier(dashboard, _driver_q)
                _driver_obj = result[0][0] if match_type == 'multiple' else result
                if _driver_obj and hasattr(_driver_obj, 'email'):
                    _lines = []
                    for part in _raw_msgs.split('||'):
                        part = part.strip()
                        if part.startswith('Y>'):
                            _lines.append(f"  You: {part[2:]}")
                        elif part.startswith('T>'):
                            _lines.append(f"  {_driver_obj.first_name or 'Them'}: {part[2:]}")
                    if _lines:
                        _ts = datetime.now().strftime("%d %b %H:%M")
                        _thread_text = "\n".join(_lines)
                        _thread_block = f"[{_ts} 📱] [THREAD]\n{_thread_text}\n[/THREAD]"
                        existing = _driver_obj.notes or ""
                        import re as _re
                        existing = _re.sub(
                            r'\[\d{2} \w{3} \d{2}:\d{2} 📱\] \[THREAD\].*?\[/THREAD\]\n?',
                            '', existing, flags=_re.DOTALL
                        ).strip()
                        existing = _re.sub(
                            r'\[\d{2} \w{3} \d{2}:\d{2} 📱 (?:FB|IG)\] Captured thread:.*?(?=\[|\Z)',
                            '', existing, flags=_re.DOTALL
                        ).strip()
                        _driver_obj.notes = f"{_thread_block}\n{existing}" if existing else _thread_block
                        dashboard.add_new_driver(
                            _driver_obj.email, _driver_obj.first_name, _driver_obj.last_name,
                            _driver_obj.facebook_url or "", ig_url=_driver_obj.instagram_url or "",
                            championship=_driver_obj.championship or "", notes=_driver_obj.notes
                        )
                        print(f"[AG SAVE] Saved {_platform} thread for {_driver_obj.full_name}")
            for _p in ['save_messages', 'msg_platform']:
                if _p in st.query_params:
                    del st.query_params[_p]

        # --- ?save_url= SILENT URL SAVE (no card opened) ---
        if "save_url" in st.query_params and "driver" in st.query_params:
            _driver_q = st.query_params["driver"]
            _fb_url_q = st.query_params.get("fb_url", "")
            _ig_url_q = st.query_params.get("ig_url", "")
            print(f"[AG SAVE] save_url fired — driver={_driver_q}, fb={_fb_url_q[:60]}, ig={_ig_url_q[:60]}")

            result, match_type = find_driver_by_identifier(dashboard, _driver_q)
            _driver_obj = result[0][0] if match_type == 'multiple' else result

            if _driver_obj and hasattr(_driver_obj, 'email'):
                _url_changed = False
                if _fb_url_q:
                    _driver_obj.facebook_url = _fb_url_q
                    _url_changed = True
                if _ig_url_q:
                    _driver_obj.instagram_url = _ig_url_q
                    _url_changed = True
                if _url_changed:
                    dashboard.add_new_driver(
                        _driver_obj.email, _driver_obj.first_name, _driver_obj.last_name,
                        _driver_obj.facebook_url or "", ig_url=_driver_obj.instagram_url or "",
                        championship=_driver_obj.championship or "", notes=_driver_obj.notes or ""
                    )
                    print(f"[AG SAVE] ✅ URL saved for {_driver_obj.full_name}")
            else:
                print(f"[AG SAVE] ⚠️ Driver not found for URL save: {_driver_q}")

            for _p in ['save_url', 'driver', 'fb_url', 'ig_url']:
                if _p in st.query_params:
                    del st.query_params[_p]

        # Save processed — decide whether to stop or rerun.
        # Background tabs (opened by extension) have minimal session state.
        # The user's main tab has navigation, dashboard state, etc.
        # If it's the main tab, we must rerun to restore the UI.
        # If it's a background tab, st.stop() prevents expensive UI rendering.
        _has_active_session = any(k in st.session_state for k in [
            'nav_selection', '_open_driver_card', 'calendar_selected_driver',
            '_driver_link_handled', 'global_championship', '_stage_toast'
        ])
        if _has_active_session:
            # Main tab — params already cleared, rerun to restore normal UI
            st.rerun()
        else:
            # Background tab — give Airtable 2 seconds, then stop
            import time as _time_mod
            _time_mod.sleep(2)
            st.stop()
    # ===========================================================================
    # END OF EARLY SAVE HANDLERS
    # ===========================================================================

    # --- DEBUG INSPECTOR (Hidden for diagnostics) ---
    if 'dashboard' in locals() and dashboard:
        with st.sidebar.expander("🛠️ Debug Inspector"):
            st.write(f"In-Memory Drivers: {len(dashboard.drivers)}")

            # Show Airtable loading debug info
            if hasattr(dashboard.data_loader, 'airtable_debug'):
                dbg = dashboard.data_loader.airtable_debug
                st.write(f"Airtable Records: {dbg.get('total_records', 'N/A')}")
                st.write(f"Skipped: {dbg.get('skipped_count', 0)}")
                if dbg.get('column_names'):
                    st.caption(f"Columns: {', '.join(dbg['column_names'][:10])}")
                if dbg.get('skipped'):
                    with st.expander("Skipped Records"):
                        for s in dbg['skipped'][:5]:
                            st.code(s, language=None)

            dbg_q = st.text_input("Search Memory (Name/Email)")
            if dbg_q:
                hits = [r for r in dashboard.drivers.values() if dbg_q.lower() in r.full_name.lower() or dbg_q.lower() in str(r.email).lower()]
                if hits:
                    st.success(f"Found {len(hits)}:")
                    for h in hits:
                            st.write(f"**{h.full_name}**")
                            st.caption(f"ID: `{h.email}`")
                            st.json({
                                "Stage": str(h.current_stage),
                                "FB": h.facebook_url,
                                "IG": h.instagram_url,
                                "Source": "GSheet" if h.in_gsheet_input else "Airtable (Verified)"
                            })
                else:
                    st.error("No matches in memory.")
    drivers = dashboard.drivers
    daily_metrics = dashboard.get_daily_metrics()  
except Exception as e:
    st.error(f"Error loading dashboard: {e}")
    st.stop()

# ==============================================================================
# EARLY DIALOG CHECK — open contact card BEFORE heavy rendering
# This prevents the 30+ second wait when opening cards from calendar/pipeline.
# Dialogs float above the page, so they don't need the pipeline to render first.
# Catches: calendar clicks, pipeline button clicks, search results, lead magnets
# ==============================================================================
# Only ONE dialog at a time — calendar takes priority if both are set
if 'calendar_selected_driver' in st.session_state:
    _early_driver = dashboard._find_driver(st.session_state['calendar_selected_driver'])
    if _early_driver:
        view_calendar_dialog(_early_driver, dashboard)
    else:
        del st.session_state['calendar_selected_driver']
elif '_open_driver_card' in st.session_state:
    _card_driver_id = st.session_state['_open_driver_card']
    _card_driver = dashboard._find_driver(_card_driver_id)
    if _card_driver:
        # Reset stale counter — dialog is actively open, not stale
        st.session_state['_dash_stale_count'] = 0
        view_unified_dialog(_card_driver, dashboard)
    # Clean up AFTER dialog renders (dialog stays open across reruns)
    # Only clear when the key changes (new driver) or user navigates away
    if '_open_driver_card' in st.session_state and not _card_driver:
        del st.session_state['_open_driver_card']

# Stage toast now handled by fast path at line ~2073

# ==============================================================================
# CALENDAR VIEW
# ==============================================================================

# Stall thresholds (hours) — shared with pipeline, used to auto-generate follow-up dates
_CAL_STALL_THRESHOLDS = {
    FunnelStage.MESSAGED: 48,
    FunnelStage.REPLIED: 48,
    FunnelStage.LINK_SENT: 24,
    FunnelStage.BLUEPRINT_LINK_SENT: 24,
    FunnelStage.RACE_WEEKEND: 24,
    FunnelStage.RACE_REVIEW_COMPLETE: 24,
    FunnelStage.BLUEPRINT_STARTED: 24,
    FunnelStage.REGISTERED: 24,
    FunnelStage.DAY1_COMPLETE: 24,
    FunnelStage.DAY2_COMPLETE: 24,
    FunnelStage.STRATEGY_CALL_BOOKED: 48,
}

# Stage → date attribute mapping for calendar
_CAL_STAGE_DATE_MAP = {
    FunnelStage.MESSAGED: 'outreach_date',
    FunnelStage.REPLIED: 'replied_date',
    FunnelStage.LINK_SENT: 'link_sent_date',
    FunnelStage.BLUEPRINT_LINK_SENT: 'link_sent_date',
    FunnelStage.RACE_WEEKEND: 'race_weekend_review_date',
    FunnelStage.RACE_REVIEW_COMPLETE: 'race_weekend_review_date',
    FunnelStage.BLUEPRINT_STARTED: 'registered_date',
    FunnelStage.REGISTERED: 'registered_date',
    FunnelStage.DAY1_COMPLETE: 'day1_complete_date',
    FunnelStage.DAY2_COMPLETE: 'day2_complete_date',
    FunnelStage.STRATEGY_CALL_BOOKED: 'strategy_call_booked_date',
}

# Stages to skip on calendar (won/lost — no follow-up needed)
_CAL_SKIP_STAGES = {
    FunnelStage.CLIENT, FunnelStage.SALE_CLOSED,
    FunnelStage.NOT_A_FIT, FunnelStage.DOES_NOT_REPLY, FunnelStage.NO_SOCIALS,
}

def render_calendar_view(dashboard):
    from streamlit_calendar import calendar

    st.subheader("📅 Follow-Up Calendar")

    events = []
    seen_drivers = set()  # Avoid duplicates

    def _platform_badge(driver):
        """Return emoji badge showing which platform(s) the conversation is on."""
        has_fb = bool(driver.facebook_url and str(driver.facebook_url).strip())
        has_ig = bool(driver.instagram_url and str(driver.instagram_url).strip())
        if has_fb and has_ig:
            return "📘📷"
        if has_fb:
            return "📘"
        if has_ig:
            return "📷"
        return ""

    # Colors
    COLOR_OVERDUE = "#FF4B4B"   # Red — past due
    COLOR_TODAY   = "#FF9800"   # Orange — due today
    COLOR_FUTURE  = "#4CAF50"   # Green — scheduled ahead
    COLOR_STALLED = "#E91E63"   # Pink — auto-generated from stall detection
    COLOR_PAST_DONE = "#9E9E9E" # Grey — past follow-ups already handled

    now = datetime.now()
    now_date = now.date()

    # ── 1. Drivers with explicit follow_up_date ──
    for driver_id, driver in dashboard.drivers.items():
        if driver.follow_up_date:
            fu_date = driver.follow_up_date
            if isinstance(fu_date, str):
                try:
                    from dateutil import parser as dp
                    fu_date = dp.parse(fu_date)
                except:
                    continue
            fu_day = fu_date.date() if hasattr(fu_date, 'date') else fu_date

            if fu_day < now_date:
                bg_color = COLOR_OVERDUE
            elif fu_day == now_date:
                bg_color = COLOR_TODAY
            else:
                bg_color = COLOR_FUTURE

            stage_label = driver.current_stage.value if hasattr(driver.current_stage, 'value') else str(driver.current_stage)
            _badge = _platform_badge(driver)
            events.append({
                "title": f"{_badge} 📋 {driver.full_name} [{stage_label}]".strip(),
                "start": fu_date.isoformat() if hasattr(fu_date, 'isoformat') else str(fu_date),
                "allDay": True,
                "resourceId": driver_id,
                "extendedProps": {"driver_id": driver_id},
                "backgroundColor": bg_color,
                "borderColor": bg_color,
            })
            seen_drivers.add(driver_id)

    # ── 2. Auto-generate follow-ups for stalled drivers without follow_up_date ──
    for driver_id, driver in dashboard.drivers.items():
        if driver_id in seen_drivers:
            continue
        if driver.current_stage in _CAL_SKIP_STAGES:
            continue
        if driver.is_disqualified:
            continue

        threshold_hours = _CAL_STALL_THRESHOLDS.get(driver.current_stage)
        if not threshold_hours:
            continue

        # Find the stage-entry date
        date_attr = _CAL_STAGE_DATE_MAP.get(driver.current_stage, 'outreach_date')
        stage_date = getattr(driver, date_attr, None)
        if not stage_date:
            stage_date = getattr(driver, 'last_activity', None)
        if not stage_date:
            stage_date = getattr(driver, 'outreach_date', None)
        if not stage_date:
            continue

        if isinstance(stage_date, str):
            try:
                from dateutil import parser as dp
                stage_date = dp.parse(stage_date)
            except:
                continue

        # Calculate when follow-up is due
        due_date = stage_date + timedelta(hours=threshold_hours)
        hours_elapsed = (now - stage_date).total_seconds() / 3600

        # Only show if past the threshold (i.e. stalled)
        if hours_elapsed < threshold_hours:
            continue

        due_day = due_date.date()
        days_overdue = (now_date - due_day).days

        if days_overdue > 0:
            bg_color = COLOR_STALLED
        elif days_overdue == 0:
            bg_color = COLOR_TODAY
        else:
            bg_color = COLOR_FUTURE

        stage_label = driver.current_stage.value if hasattr(driver.current_stage, 'value') else str(driver.current_stage)

        # Title shows urgency
        if days_overdue >= 7:
            urgency = "🔴"
        elif days_overdue >= 3:
            urgency = "🟠"
        elif days_overdue >= 1:
            urgency = "🟡"
        else:
            urgency = "⏰"

        events.append({
            "title": f"{urgency} {_platform_badge(driver)} {driver.full_name} [{stage_label}]".strip(),
            "start": due_date.isoformat(),
            "allDay": True,
            "resourceId": driver_id,
            "extendedProps": {"driver_id": driver_id},
            "backgroundColor": bg_color,
            "borderColor": bg_color,
        })
        seen_drivers.add(driver_id)

    # ── 3. 2026 Race Calendars (shared module-level constant) ──
    for _series_name, _series_data in RACE_CALENDARS.items():
        _color = _series_data["color"]
        for _rd in _series_data["rounds"]:
            events.append({
                "title": f"🏁 {_series_name} {_rd['round']} — {_rd['name']}",
                "start": _rd["start"],
                "end": _rd["end"],       # end is exclusive in FullCalendar
                "allDay": True,
                "display": "background",  # renders as subtle background highlight
                "backgroundColor": _color,
                "borderColor": _color,
                "textColor": "#FFFFFF",
            })
            # Also add a visible label event on the first day
            events.append({
                "title": f"🏁 {_series_name} {_rd['round']} — {_rd['name']}",
                "start": _rd["start"],
                "allDay": True,
                "backgroundColor": _color,
                "borderColor": _color,
            })

    # ── Summary above calendar ──
    overdue_count = sum(1 for e in events if e['backgroundColor'] in [COLOR_OVERDUE, COLOR_STALLED])
    today_count = sum(1 for e in events if e['backgroundColor'] == COLOR_TODAY)
    upcoming_count = sum(1 for e in events if e['backgroundColor'] == COLOR_FUTURE)

    sum_cols = st.columns(4)
    sum_cols[0].metric("Total Follow-Ups", len(events))
    sum_cols[1].metric("🔴 Overdue", overdue_count)
    sum_cols[2].metric("🟠 Due Today", today_count)
    sum_cols[3].metric("🟢 Upcoming", upcoming_count)

    if overdue_count > 0:
        st.warning(f"⚠️ **{overdue_count} overdue follow-ups** — click each to open the contact card and send a message")

    # ══════════════════════════════════════════════════════════════════
    # PIPELINE ACTION LISTS — Quick-access dropdown per funnel stage
    # ══════════════════════════════════════════════════════════════════
    _skip = {FunnelStage.CLIENT, FunnelStage.SALE_CLOSED, FunnelStage.NOT_A_FIT,
             FunnelStage.DOES_NOT_REPLY, FunnelStage.NO_SOCIALS, FunnelStage.CONTACT}

    # Build driver lists per stage category
    _stage_buckets = {
        "↩️ Replied (awaiting next step)": {
            'stages': [FunnelStage.REPLIED],
            'drivers': [],
        },
        "🔗 Review Link Sent (not started)": {
            'stages': [FunnelStage.LINK_SENT],
            'drivers': [],
        },
        "📊 Review Started (not completed)": {
            'stages': [FunnelStage.RACE_WEEKEND],
            'drivers': [],
        },
        "✅ Review Done (not started Blueprint)": {
            'stages': [FunnelStage.RACE_REVIEW_COMPLETE],
            'drivers': [],
        },
        "📚 Blueprint Link Sent (not started)": {
            'stages': [FunnelStage.BLUEPRINT_LINK_SENT],
            'drivers': [],
        },
        "📚 In Blueprint (stalled)": {
            'stages': [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED],
            'drivers': [],
        },
        "📘 Day 1 Done (not started Day 2)": {
            'stages': [FunnelStage.DAY1_COMPLETE],
            'drivers': [],
        },
        "📗 Day 2 Done (not started Day 3)": {
            'stages': [FunnelStage.DAY2_COMPLETE],
            'drivers': [],
        },
        "📞 Strategy Call Booked (not completed)": {
            'stages': [FunnelStage.STRATEGY_CALL_BOOKED],
            'drivers': [],
        },
    }

    for _rid, _r in dashboard.drivers.items():
        if _r.current_stage in _skip or _r.is_disqualified:
            continue
        for _bname, _bdata in _stage_buckets.items():
            if _r.current_stage in _bdata['stages']:
                _bdata['drivers'].append(_r)
                break

    # Sort each bucket by days in stage (longest first)
    for _bdata in _stage_buckets.values():
        _bdata['drivers'].sort(key=lambda x: x.days_in_current_stage, reverse=True)

    st.divider()
    st.markdown("#### 📋 Outreach Lists by Stage")

    # Show each bucket as an expander with driver buttons
    for _bname, _bdata in _stage_buckets.items():
        _count = len(_bdata['drivers'])
        if _count == 0:
            continue

        with st.expander(f"{_bname} ({_count})", expanded=False):
            for _idx, _r in enumerate(_bdata['drivers']):
                _days = _r.days_in_current_stage
                _urgency = "🔴" if _days >= 7 else "🟠" if _days >= 3 else "🟡" if _days >= 1 else "⚪"
                _plat = _platform_badge(_r)
                _label = f"{_urgency} {_plat} {_r.full_name} — {_days}d".strip()
                if _r.championship:
                    _label += f" · {_r.championship}"

                if st.button(_label, key=f"cal_list_{_r.email}_{_idx}", use_container_width=True):
                    st.session_state['calendar_selected_driver'] = _r.email.lower().strip()
                    st.rerun()

    st.divider()

    calendar_options = {
        "headerToolbar": {
            "left": "today prev,next",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek,listMonth"
        },
        "initialView": "dayGridMonth",
        "editable": True,
        "firstDay": 1,
        "eventDisplay": "block",
        "dayMaxEvents": 5,
        "moreLinkText": "more follow-ups",
    }

    # Render Calendar
    state = calendar(events=events, options=calendar_options, key="calendar_widget")

    # HANDLER: Drag & Drop (Event Change)
    if state.get("eventChange"):
        change = state["eventChange"]
        event = change.get("event", {})
        driver_id = event.get("extendedProps", {}).get("driver_id") or event.get("resourceId")
        new_start_str = event.get("start")

        if driver_id and new_start_str:
            try:
                from dateutil import parser
                new_date = parser.parse(new_start_str)
                dashboard.data_loader.save_driver_details(driver_id, follow_up_date=new_date)
                st.toast(f"Moved follow-up to {new_date.strftime('%d %b')}!")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to update date: {e}")

    # HANDLER: Click - Open Contact Card Dialog
    #
    # streamlit_calendar caches its last eventClick across reruns.
    # Three guards prevent infinite-rerun loops & ghost re-opens:
    #   1. Dialog already open  → skip (calendar_selected_driver set)
    #   2. Dialog just closed   → skip (_cal_dismissed flag set)
    #   3. After setting calendar_selected_driver, call st.rerun() so the
    #      EARLY DIALOG CHECK (line ~2745) opens it on the next pass.
    #
    # The _cal_dismissed flag is cleared once eventClick is no longer
    # in the component state (user navigated/clicked elsewhere).
    #
    has_click = bool(state.get("eventClick"))

    if has_click and 'calendar_selected_driver' not in st.session_state and not st.session_state.get('_cal_dismissed'):
        event = state["eventClick"]["event"]
        driver_id = (
            event.get("extendedProps", {}).get("driver_id") or
            event.get("extendedProps", {}).get("resourceId") or
            event.get("resourceId")
        )

        # Fallback: parse driver name from event title  e.g. "📋 Daryl Hutt [Messaged]"
        if not driver_id:
            title = event.get("title", "")
            import re
            m = re.match(r'^[^\w]*(.+?)\s*\[', title)
            if m:
                driver_id = m.group(1).strip()

        if driver_id:
            driver_key = driver_id.lower().strip()
            if dashboard._find_driver(driver_key):
                st.session_state['calendar_selected_driver'] = driver_key
                # Must rerun so the EARLY DIALOG CHECK at top of script opens
                # the dialog. Without this, the page renders blank (calendar
                # skipped because key is set, but dialog check already passed).
                st.rerun()

    # Clear dismissed flag once eventClick is gone (user navigated away)
    if not has_click and st.session_state.get('_cal_dismissed'):
        del st.session_state['_cal_dismissed']

    # Dialog is now opened early (before heavy rendering) — see EARLY DIALOG CHECK above.
    # No need to call view_calendar_dialog here; it's already open.


# --- PERSISTENCE: RESTORE SESSION ---
# Driver list persistence is now handled by JSON names file in the Outreach section.
# It saves driver names and auto-re-analyzes on restore (lines ~1402-1425).
if 'matched_results' not in st.session_state:
    st.session_state.matched_results = []

# --- URL QUERY PARAMS FOR TAB PERSISTENCE ---
# Use query params to keep user on same tab after refresh
def on_nav_change():
    val = st.session_state.main_nav
    if "Dashboard" in val: st.query_params["tab"] = "dashboard"
    elif "Calendar" in val: st.query_params["tab"] = "calendar"
    elif "Race" in val: st.query_params["tab"] = "race"
    elif "Strategy" in val: st.query_params["tab"] = "strategy"
    elif "All" in val: st.query_params["tab"] = "database"
    elif "Admin" in val: st.query_params["tab"] = "admin"

# Initialize if not set
if "main_nav" not in st.session_state:
    st.session_state.main_nav = "📊 Funnel Dashboard" # Default
    
    # Check URL
    if "tab" in st.query_params:
        tab_val = st.query_params["tab"]
        if tab_val == "race": st.session_state.main_nav = "🏁 Race Outreach"
        elif tab_val == "strategy": st.session_state.main_nav = "📞 Strategy Calls"
        elif tab_val == "calendar": st.session_state.main_nav = "📅 Calendar"
        elif tab_val == "admin": st.session_state.main_nav = "⚙️ Admin"

# NAVIGATION BAR
nav = st.radio(
    "Navigation",
    ["📊 Funnel Dashboard", "🏁 Race Outreach", "📞 Strategy Calls", "📅 Calendar", "⚙️ Admin"],
    horizontal=True,
    label_visibility="collapsed",
    key="main_nav", # This binding ensures persistence
    on_change=on_nav_change
)

# FAST PATH moved to right after load_dashboard_data (line ~2068)
# to avoid 700+ lines of sidebar/settings code running first.

# --- DEEP LINK: ?driver= QUERY PARAM ---
# Chrome extension sends ?driver=Name&fb_url=...&ig_url=... to auto-open contact card
if "driver" in st.query_params and not st.session_state.get('_driver_link_handled'):
    _driver_q = st.query_params["driver"]
    _fb_url_q = st.query_params.get("fb_url", "")
    _ig_url_q = st.query_params.get("ig_url", "")

    result, match_type = find_driver_by_identifier(dashboard, _driver_q)

    # Auto-save captured social URL to the driver record
    def _apply_social_url(driver_obj):
        updated = False
        if _fb_url_q:
            driver_obj.facebook_url = _fb_url_q
            updated = True
        if _ig_url_q:
            driver_obj.instagram_url = _ig_url_q
            updated = True
        if updated:
            dashboard.add_new_driver(
                driver_obj.email, driver_obj.first_name, driver_obj.last_name,
                driver_obj.facebook_url or "", ig_url=driver_obj.instagram_url or "",
                championship=driver_obj.championship or ""
            )
            st.toast(f"📎 Saved {'FB' if _fb_url_q else 'IG'} URL for {driver_obj.full_name}")

    if match_type == 'multiple':
        st.info(f"Multiple drivers match **{unquote_plus(_driver_q)}**. Select one:")
        _names = [f"{r.full_name} ({int(s*100)}%)" for r, s in result[:5]]
        _choice = st.selectbox("Select driver", _names, key="_driver_deep_select")
        _idx = _names.index(_choice)
        if st.button("Open Contact Card", key="_driver_deep_btn"):
            _apply_social_url(result[_idx][0])
            st.session_state['_driver_link_handled'] = True
            st.session_state['_open_driver_card'] = result[_idx][0].email
            st.rerun()
    elif result is not None:
        _apply_social_url(result)
        st.session_state['_driver_link_handled'] = True
        st.session_state['_open_driver_card'] = result.email
        st.rerun()
    else:
        st.warning(f"No driver found matching: {unquote_plus(_driver_q)}")

    # Clean up URL params
    for _p in ['fb_url', 'ig_url']:
        if _p in st.query_params:
            del st.query_params[_p]

# Render View
# Dialogs are overlays that render on top of the page content — we ALWAYS
# render the page underneath so closing a dialog never leaves a blank screen.
if nav == "📊 Funnel Dashboard":
    # Detect stale _open_driver_card (user dismissed dialog natively via X or outside click)
    if '_open_driver_card' in st.session_state:
        _dash_stale = st.session_state.get('_dash_stale_count', 0) + 1
        st.session_state['_dash_stale_count'] = _dash_stale
        if _dash_stale >= 3:
            # Dialog was closed natively — clean up
            del st.session_state['_open_driver_card']
            st.session_state.pop('_dash_stale_count', None)
            st.rerun()
    else:
        st.session_state.pop('_dash_stale_count', None)
    render_dashboard(dashboard, daily_metrics, drivers)
elif nav == "🏁 Race Outreach":
    st.session_state.pop('_open_driver_card', None)
    st.session_state.pop('calendar_selected_driver', None)
    render_race_outreach(dashboard)
elif nav == "📞 Strategy Calls":
    st.session_state.pop('_open_driver_card', None)
    st.session_state.pop('calendar_selected_driver', None)
    render_strategy_calls(dashboard)
elif nav == "⚙️ Admin":
    st.session_state.pop('_open_driver_card', None)
    st.session_state.pop('calendar_selected_driver', None)
    render_admin(dashboard, drivers)
elif nav == "📅 Calendar":
    st.session_state.pop('_open_driver_card', None)
    if 'calendar_selected_driver' in st.session_state:
        # Dialog SHOULD be open (handled by early dialog check above).
        # But if the user dismissed it natively (clicked outside / X button),
        # the key lingers. Detect via rerun counter and clean up.
        _stale = st.session_state.get('_cal_stale_count', 0) + 1
        st.session_state['_cal_stale_count'] = _stale
        if _stale >= 2:
            del st.session_state['calendar_selected_driver']
            st.session_state.pop('_cal_stale_count', None)
            st.session_state['_cal_dismissed'] = True
            st.rerun(scope="app")
    else:
        st.session_state.pop('_cal_stale_count', None)
    # Always render calendar (dialog floats above it)
    render_calendar_view(dashboard)
