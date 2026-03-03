import streamlit as st
import urllib.parse
import re as _re
from datetime import datetime, timedelta
from funnel_manager import FunnelStage


def _clean_first_name(name):
    """Extract a clean first name from a value that might be a social media handle.

    Examples:
        Camerondunker3  -> Cameron   (handle with trailing digits)
        jacob.pierce54  -> Jacob     (IG handle with dots/digits)
        john_smith      -> John      (underscored handle)
        Cameron         -> Cameron   (already clean)
        Cameron Dunker  -> Cameron   (normal name - take first part)
    """
    if not name or not name.strip():
        return 'mate'
    name = name.strip()
    # If it contains a space, it is likely a real name -- just take the first word
    if ' ' in name:
        return name.split()[0].title()
    # Detect CamelCase handles like CameronDunker first
    camel_parts = _re.sub(r'([a-z])([A-Z])', r'\1 \2', name).split()
    if len(camel_parts) > 1:
        return camel_parts[0].title()
    # Detect handle-like patterns: underscores, dots, or digits anywhere
    if '_' in name or '.' in name or _re.search(r'\d', name):
        # Replace separators, strip all digits
        cleaned = _re.sub(r'[_.]', ' ', name)
        cleaned = _re.sub(r'\d+', ' ', cleaned).strip()
        if cleaned:
            parts = cleaned.split()
            if len(parts) > 1:
                first = parts[0]
                if len(first) >= 2 and first.isalpha():
                    return first.title()
            else:
                word = parts[0]
                if len(word) > 7 and word.isalpha():
                    # Likely two names jammed together (e.g. camerondunker)
                    # Take first 7 chars as a reasonable first-name length
                    return word[:7].title()
                if len(word) >= 2 and word.isalpha():
                    return word.title()
    # As-is (single name with no handle pattern)
    return name.title()

# ═══════════════════════════════════════════════════════════════════════
# AI MESSAGE GENERATOR — Full funnel knowledge from Blueprint
# ═══════════════════════════════════════════════════════════════════════

# Stage → date attribute mapping for follow-up detection
_STAGE_DATE_ATTRS = {
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
    FunnelStage.STRATEGY_CALL_NO_SHOW: 'strategy_call_booked_date',
}

# Follow-up timing matrix (from Blueprint Section 4)
_FOLLOW_UP_TIMERS = {
    FunnelStage.MESSAGED: {'hours': 72, 'max_follow_ups': 1, 'stall_days': 7},
    FunnelStage.REPLIED: {'hours': 48, 'max_follow_ups': 2, 'stall_days': 7},
    FunnelStage.LINK_SENT: {'hours': 24, 'max_follow_ups': 2, 'stall_days': 7},
    FunnelStage.BLUEPRINT_LINK_SENT: {'hours': 24, 'max_follow_ups': 2, 'stall_days': 7},
    FunnelStage.RACE_WEEKEND: {'hours': 48, 'max_follow_ups': 2, 'stall_days': 7},
    FunnelStage.RACE_REVIEW_COMPLETE: {'hours': 24, 'max_follow_ups': 2, 'stall_days': 7},
    FunnelStage.REGISTERED: {'hours': 24, 'max_follow_ups': 1, 'stall_days': 7},
    FunnelStage.BLUEPRINT_STARTED: {'hours': 24, 'max_follow_ups': 1, 'stall_days': 7},
    FunnelStage.DAY1_COMPLETE: {'hours': 24, 'max_follow_ups': 1, 'stall_days': 7},
    FunnelStage.DAY2_COMPLETE: {'hours': 24, 'max_follow_ups': 1, 'stall_days': 7},
    FunnelStage.STRATEGY_CALL_BOOKED: {'hours': 0, 'max_follow_ups': 0, 'stall_days': 0},
}


def _classify_sentiment(message_text):
    """Classify the sentiment of a driver's last message (from Blueprint 3.3)."""
    if not message_text:
        return "productive"  # Default when no context
    msg = message_text.lower()
    great_words = ['win', 'won', 'podium', 'pb', 'best', 'breakthrough', 'amazing',
                   'fantastic', 'brilliant', 'pole', 'first', 'top', 'smashed']
    tough_words = ['rough', 'struggled', 'crashed', 'frustrated', 'difficult',
                   'terrible', 'awful', 'bad', 'worst', 'poor', 'nightmare',
                   'mechanical', 'dnf', 'retired', 'injury', 'hurt']
    if any(w in msg for w in great_words):
        return "great"
    if any(w in msg for w in tough_words):
        return "tough"
    return "productive"


def _extract_last_their_message(notes_or_thread):
    """Extract the last message from the driver (not 'You') in a captured thread.

    Handles multiple formats:
    1. Structured: "Them: message" or "Name: message"
    2. Facebook Messenger paste: Contact name on its own line, message below,
       separated by "You sent" blocks for your messages.
    3. Chrome extension [THREAD] format
    """
    if not notes_or_thread:
        return ""

    text = notes_or_thread.strip()
    lines = text.split('\n')

    # Check if this is a Facebook Messenger paste (contains "You sent")
    _has_you_sent = 'you sent' in text.lower()

    if not _has_you_sent:
        # --- FORMAT 1: Structured colon format (Them: / Name:) ---
        # Only for Chrome extension captured threads, NOT FB pastes
        for line in reversed(lines):
            line = line.strip()
            if line.startswith('Them:') or line.startswith('  Them:'):
                return line.split(':', 1)[1].strip()
            if ':' in line and not line.startswith('You:') and not line.startswith('  You:'):
                parts = line.split(':', 1)
                if len(parts) == 2 and len(parts[0].strip()) < 30 and len(parts[1].strip()) > 5:
                    # Skip URLs and timestamps
                    prefix = parts[0].strip().lower()
                    if prefix in ('http', 'https', 'ftp') or _re.match(r'^\d', prefix):
                        continue
                    return parts[1].strip()

    # --- FORMAT 2: Facebook Messenger paste ---
    # FB format alternates between "You sent" (your messages) and contact responses.
    # We need to find the LAST contiguous block that's NOT from "You".
    import re as _re_extract

    if _has_you_sent:
        # Build a list of (sender, content) blocks
        # "You sent" marks YOUR messages. Everything else is THEIR message.
        _blocks = []
        _current_sender = None
        _current_lines = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # "You sent" = start of YOUR message block
            if stripped.lower() == 'you sent':
                # Save previous block if exists
                if _current_sender and _current_lines:
                    _blocks.append((_current_sender, '\n'.join(_current_lines)))
                _current_sender = 'you'
                _current_lines = []
                continue

            # Date/time headers — these precede a new message from either side
            if _re_extract.match(r'^(\d{1,2}/\d{1,2}/\d{4},?\s*\d{1,2}:\d{2}|\d{1,2}\s+\w{3}\s+\d{4},?\s*\d{1,2}:\d{2}|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+\d{1,2}:\d{2}|\d{1,2}:\d{2})$', stripped):
                # Date line — next line will tell us who's speaking
                # Save current block
                if _current_sender and _current_lines:
                    _blocks.append((_current_sender, '\n'.join(_current_lines)))
                _current_sender = None
                _current_lines = []
                continue

            # "Write to ..." footer — skip
            if stripped.lower().startswith('write to '):
                continue

            # First non-date, non-"You sent" line after a date or start
            if _current_sender is None:
                # This is either a contact name header or content
                # If the next block has "You sent", this was a header before your msg
                # If not, this starts their message
                _current_sender = 'them'
                _current_lines = [stripped]
                continue

            # Continuation of current block
            _current_lines.append(stripped)

        # Save last block
        if _current_sender and _current_lines:
            _blocks.append((_current_sender, '\n'.join(_current_lines)))

        # Find the last 'them' block
        for sender, content in reversed(_blocks):
            if sender == 'them':
                # Clean up: remove repeated contact name headers
                msg_lines = content.split('\n')
                # FB duplicates the name: "Contact Name\nContact Name\nActual message"
                if len(msg_lines) >= 2 and msg_lines[0] == msg_lines[1]:
                    msg_lines = msg_lines[2:]
                elif len(msg_lines) >= 2:
                    # Check if first line is just a short name (no punctuation)
                    first = msg_lines[0]
                    if len(first) < 50 and not any(c in first for c in '.!?,'):
                        msg_lines = msg_lines[1:]
                return '\n'.join(msg_lines).strip()

    return ""


def _has_recent_follow_up_sent(driver, within_hours=48):
    """Check if a follow-up or outreach was sent recently (via notes marker)."""
    notes = driver.notes or ""
    # Look for ✅ FU_SENT markers AND 📤 outreach markers
    matches = _re.findall(r'\[(\d{2} \w{3} \d{2}:\d{2}) ✅\]', notes)
    # Also check for outreach sent via Chrome extension
    outreach_matches = _re.findall(r'\[(\d{2} \w{3} \d{2}:\d{2})\] 📤', notes)
    all_matches = matches + outreach_matches
    if not all_matches:
        return False, None
    # Parse each and find the most recent one
    now = datetime.now()
    best_time = None
    for ts_str in all_matches:
        try:
            parsed = datetime.strptime(ts_str, "%d %b %H:%M").replace(year=now.year)
            if best_time is None or parsed > best_time:
                best_time = parsed
        except ValueError:
            continue
    if best_time:
        hours_ago = (now - best_time).total_seconds() / 3600
        return hours_ago <= within_hours, best_time
    return False, None


def _build_perf_line(perf, live_data=None):
    """Build a one-line performance summary from saved results or live Speedhive data.

    Args:
        perf: dict from get_results_summary() (saved [RESULTS] data)
        live_data: list of session result dicts from Speedhive import (optional, takes priority)

    Returns: (summary_line, detail_dict) or (None, {})
    """
    detail = {}

    # Prefer live Speedhive data (fresher, from current import)
    if live_data:
        races = [d for d in live_data if d.get('session_type') == 'race']
        quals = [d for d in live_data if d.get('session_type') == 'qualify']
        practice = [d for d in live_data if d.get('session_type') == 'practice']

        parts = []
        if races:
            best_race = min(races, key=lambda r: r.get('position') or 999)
            detail['race_pos'] = best_race.get('position')
            detail['race_class'] = best_race.get('result_class', '')
            parts.append(f"P{best_race['position']} race")
        if quals:
            best_qual = min(quals, key=lambda r: r.get('position') or 999)
            detail['qual_pos'] = best_qual.get('position')
            best_qlap = best_qual.get('best_lap', '')
            if best_qlap:
                detail['qual_lap'] = best_qlap
                parts.append(f"P{best_qual['position']} qualifying ({best_qlap})")
            else:
                parts.append(f"P{best_qual['position']} qualifying")
        if practice:
            best_prac = min(practice, key=lambda r: r.get('position') or 999)
            detail['practice_pos'] = best_prac.get('position')
            best_plap = best_prac.get('best_lap', '')
            if best_plap:
                detail['practice_lap'] = best_plap
                parts.append(f"P{best_prac['position']} practice ({best_plap})")
            else:
                parts.append(f"P{best_prac['position']} practice")

        # Collect all best laps for overall best
        all_laps = [d.get('best_lap', '') for d in live_data if d.get('best_lap') and d['best_lap'] != '00.000']
        if all_laps:
            detail['best_lap'] = min(all_laps)

        if parts:
            return " · ".join(parts), detail
        return None, detail

    # Fallback to saved [RESULTS] summary
    if perf and perf.get('count', 0) > 0:
        latest = perf.get('latest')
        parts = []
        if latest:
            detail['circuit'] = latest.get('circuit', '')
            if latest.get('pos'):
                detail['race_pos'] = latest['pos']
                parts.append(f"P{latest['pos']} at {latest.get('circuit', 'last round')}")
            if latest.get('best_lap') and latest['best_lap'] != '00.000':
                detail['best_lap'] = latest['best_lap']
                parts.append(f"{latest['best_lap']} best lap")
        if perf.get('best_pos'):
            detail['season_best'] = perf['best_pos']
        trend = perf.get('trend', 'new')
        detail['trend'] = trend
        # Extended season data for richer AI messages
        if perf.get('extended_trend'):
            detail['extended_trend'] = perf['extended_trend']
        if perf.get('season_narrative'):
            detail['season_narrative'] = perf['season_narrative']
        if perf.get('rounds_completed'):
            detail['rounds_completed'] = perf['rounds_completed']
        if perf.get('at_season_best'):
            detail['at_season_best'] = True
        if perf.get('best_circuit'):
            detail['best_circuit'] = perf['best_circuit']
        if perf.get('round_by_round'):
            detail['round_by_round'] = perf['round_by_round']
        trend_map = {'improving': '📈 improving', 'declining': '📉', 'stable': '➡️ consistent', 'new': ''}
        if trend_map.get(trend):
            parts.append(trend_map[trend])
        if parts:
            return " · ".join(parts), detail
    return None, detail


def _perf_opener(detail, first_name, event_name="", outreach_mode="race_weekend", championship=""):
    """Generate a performance-driven opening line for outreach messages.

    Args:
        detail: dict from _build_perf_line() — now includes extended_trend,
                season_narrative, rounds_completed, at_season_best, round_by_round
        first_name: driver's first name
        event_name: circuit/event name (optional)
        outreach_mode: 'race_weekend' or 'end_of_season'
        championship: championship name (used for end_of_season mode)

    Returns: personalised opening string, or generic opener if no data
    """
    # END OF SEASON MODE — reference the championship and season, not a circuit
    if outreach_mode == 'end_of_season' and championship:
        if not detail:
            return f"Hey {first_name}, I see you were competing in the {championship} this season. How did it go for you?"

        race_pos = detail.get('race_pos')
        season_best = detail.get('season_best')
        rounds_completed = detail.get('rounds_completed', 0)
        extended_trend = detail.get('extended_trend', '')

        if race_pos:
            if race_pos <= 3:
                return f"Hey {first_name}, I see you were competing in the {championship} this season - P{race_pos} in the championship is a mega result! How are you feeling about it?"
            elif extended_trend in ('consistently_improving', 'generally_improving'):
                return f"Hey {first_name}, I see you were racing in the {championship} this season and the form got stronger as it went on. How are you feeling about the season overall?"
            elif season_best and season_best <= 5:
                return f"Hey {first_name}, saw you competed in the {championship} this season, with a best of P{season_best}. How did you find the season overall?"
            else:
                return f"Hey {first_name}, I see you competed in the {championship} this season. How did it go for you?"
        return f"Hey {first_name}, I see you were competing in the {championship} this season. How did it go for you?"

    # RACE WEEKEND MODE (original logic)
    if not detail:
        if event_name:
            return f"Hey {first_name}, I see you were out at {event_name} at the weekend - how was it for you?"
        return f"Hey {first_name},"

    circuit = detail.get('circuit') or event_name or 'the weekend'

    # Race position opener
    race_pos = detail.get('race_pos')
    qual_pos = detail.get('qual_pos')
    best_lap = detail.get('best_lap')
    trend = detail.get('trend', '')
    extended_trend = detail.get('extended_trend', '')
    season_best = detail.get('season_best')
    at_season_best = detail.get('at_season_best', False)
    rounds_completed = detail.get('rounds_completed', 0)

    # Best result determines tone
    if race_pos:
        if race_pos <= 3:
            return f"Hey {first_name}, P{race_pos} at {circuit} - that's a mega result! How did it feel up there?"
        elif at_season_best and rounds_completed and rounds_completed >= 3:
            return f"Hey {first_name}, P{race_pos} at {circuit} - that's your best of the season after {rounds_completed} rounds! Clearly something's clicking. What's changed?"
        elif season_best and race_pos <= season_best:
            return f"Hey {first_name}, P{race_pos} at {circuit} - best finish of the season so far! What changed?"
        elif extended_trend == 'consistently_improving':
            return f"Hey {first_name}, P{race_pos} at {circuit} and the results have been climbing every round - that's a proper upward trend. What are you doing differently?"
        elif extended_trend == 'generally_improving':
            return f"Hey {first_name}, I see you grabbed P{race_pos} at {circuit} - the form's been getting stronger through the season. How's it feeling out there?"
        elif trend == 'improving':
            return f"Hey {first_name}, I see you grabbed P{race_pos} at {circuit} and the trend's heading the right way - how's it feeling out there?"
        elif extended_trend in ('consistently_declining', 'generally_declining'):
            return f"Hey {first_name}, I see you were out at {circuit} at the weekend. How's the season going for you so far?"
        elif race_pos <= 10:
            return f"Hey {first_name}, P{race_pos} at {circuit} at the weekend - solid run. How'd you feel it went?"
        else:
            return f"Hey {first_name}, I see you were out at {circuit} at the weekend. How was it for you?"

    if qual_pos:
        qual_lap = detail.get('qual_lap', '')
        if qual_lap:
            return f"Hey {first_name}, P{qual_pos} in qualifying at {circuit} with a {qual_lap} - not bad at all. How'd the rest of the weekend go?"
        return f"Hey {first_name}, P{qual_pos} in qualifying at {circuit} - how'd the rest of the weekend go?"

    if best_lap:
        return f"Hey {first_name}, I see you were out at {circuit} putting down a {best_lap} - how was the weekend for you?"

    if event_name:
        return f"Hey {first_name}, I see you were out at {event_name} at the weekend - how was it for you?"
    return f"Hey {first_name},"


def generate_ai_message(driver, conversation_thread="", performance_data=None, event_name="", outreach_mode="race_weekend", championship=""):
    """Generate a contextually appropriate follow-up message based on the
    driver's funnel stage, conversation thread, performance data, and Blueprint knowledge.

    Args:
        driver: Driver object
        conversation_thread: captured thread text (optional)
        performance_data: dict with 'saved' (get_results_summary result) and/or
                         'live' (list of Speedhive session dicts for this driver)
        event_name: circuit/event name for context
        outreach_mode: 'race_weekend' or 'end_of_season'
        championship: championship name (used for end_of_season mode)

    Returns (message_text, message_type, explanation)
    """
    stage = driver.current_stage
    # Use display_name (preferred social media name) for messages, not formal first_name
    _raw_name = driver.first_name or getattr(driver, 'display_name', None) or (driver.full_name.split()[0] if driver.full_name else "mate")
    first_name = _clean_first_name(_raw_name)
    # FALLBACK: If still a long single word (likely IG handle like "Johnnylytras")
    # but full_name has a proper name with space, prefer that
    if len(first_name) > 8 and ' ' not in first_name and driver.full_name and ' ' in driver.full_name:
        first_name = driver.full_name.split()[0].title()
    track = driver.championship or championship or ""
    last_msg = _extract_last_their_message(conversation_thread)
    sentiment = _classify_sentiment(last_msg)

    # Extract performance context
    perf_saved = (performance_data or {}).get('saved', {})
    perf_live = (performance_data or {}).get('live', [])
    perf_line, perf_detail = _build_perf_line(perf_saved, perf_live)

    # ── CONTACT: First outreach — performance-driven opener ──
    if stage in [FunnelStage.CONTACT, None]:
        opener = _perf_opener(perf_detail, first_name, event_name, outreach_mode=outreach_mode, championship=track)
        _mode_label = "End of season" if outreach_mode == 'end_of_season' else "Cold outreach (performance)"
        return opener, _mode_label, f"First contact. Perf: {perf_line or 'none'}"

    # ── Stage 1b: DM Sent, no reply ──
    if stage == FunnelStage.MESSAGED:
        # Use performance data to make the check-in relevant
        if perf_detail.get('race_pos'):
            msg = f"Hey {first_name}, just following up - saw you grabbed P{perf_detail['race_pos']}" \
                  f"{' at ' + perf_detail.get('circuit', '') if perf_detail.get('circuit') else ''}. " \
                  f"Did you see my message?"
        elif track:
            msg = f"Hey {first_name}, just checking in - did you see my message about {track}?"
        else:
            msg = f"Hey {first_name}, just checking in - did you see my message?"
        return msg, "Soft check-in", "No reply to Message 1. One follow-up only."

    # ── Stage 2: They replied → Introduce + Offer Assessment ──
    if stage == FunnelStage.REPLIED:
        if sentiment == "great":
            opener = "That's great work, well done!"
        elif sentiment == "tough":
            opener = "Sounds like you had a tough weekend!"
        else:
            opener = "Sounds like you had a productive weekend!"

        # Add performance insight if available
        perf_insight = ""
        if perf_detail.get('trend') == 'improving':
            perf_insight = "\n\nLooking at your recent results, the trend is clearly heading the right way - "
        elif perf_detail.get('season_best') and perf_detail.get('race_pos') and perf_detail['race_pos'] <= perf_detail['season_best']:
            perf_insight = "\n\nThat's your best finish of the season too - "

        msg = f"""Thanks for the reply, {first_name}.

{opener}{perf_insight}

Not sure if you know - I'm a Flow Performance Coach. A bit different from the usual driver-coach.

I work with drivers in many championships on the mental side of racing - helping them access the Flow State, where performance becomes automatic, consistent, and confident under pressure.

I've built a free post-race assessment tool that shows exactly where your gains are hiding - and how to unlock them in time for the next round.

Want me to send it over?"""
        return msg, f"Message 2 ({sentiment} weekend)", f"Sentiment detected: {sentiment}. Introduce as coach + offer Race Weekend Review."

    # ── Stage 3b: Assessment offered, no reply (FOMO nudge) ──
    if stage in [FunnelStage.RACE_WEEKEND]:
        msg = f"""{first_name} - wanted to circle back on the race weekend assessment

Most drivers who complete it say the same thing: 'I didn't realise THAT was what was holding me back'

If you're still interested, I can send the link over. If not, no worries - good luck with the rest of the season"""
        return msg, "FOMO nudge", "Assessment offered but no response. Nudge with social proof."

    # ── Stage 4a: Review link sent — not started or not completed ──
    if stage == FunnelStage.LINK_SENT:
        import random
        variants = [
            f"Hey, {first_name} just checking in. Is the link I sent working for you as I see you haven't started yet?",
            f"Hey {first_name}, just checking everything is ok with the link I sent you?",
            f"Hi {first_name}, just wanted to make sure the link came through ok - let me know if you need me to resend it 👍",
        ]
        msg = random.choice(variants)
        return msg, "Link sent check-in", "Review link sent but not started. Simple check-in (randomised)."

    # ── Stage 4b: Blueprint link sent — not started training ──
    if stage == FunnelStage.BLUEPRINT_LINK_SENT:
        msg = f"""Hey {first_name}, just checking in - did you get a chance to look at the Podium Contenders Blueprint I sent over?

Most drivers smash through Day 1 in about 20 minutes and say it completely changed how they think about their race weekends.

The link's still active if you want to dive in - let me know if you had any issues with it"""
        return msg, "Blueprint link check", "Blueprint link sent but not started. Nudge to begin Day 1."

    # ── Stage 5: Assessment complete → Offer free training ──
    if stage == FunnelStage.RACE_REVIEW_COMPLETE:
        msg = f"""Hey {first_name}, Great to see you completed the race weekend review!

Based on your results, you've qualified for our pre-season free training that many drivers are using to ensure they are on point from the first round.

Want me to send it over?"""
        return msg, "Offer free training", "Race Review completed. Offer Podium Contenders Blueprint."

    # ── Stage 6a: Registered but not started Day 1 ──
    if stage in [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED]:
        msg = f"Hi {first_name} I see you signed into the free training but didn't go much further, was everything ok with the link and the platform for you?"
        return msg, "Platform check (D0)", "Signed in but didn't start Day 1. Check platform issues."

    # ── Stage 6b: Day 1 complete, stalled before Day 2 ──
    if stage == FunnelStage.DAY1_COMPLETE:
        perf_hook = ""
        if perf_detail.get('trend') == 'improving':
            perf_hook = " Your recent results show the trend is heading the right way - Day 2 digs into why and how to lock that in."
        msg = f"Hey {first_name}, great work on completing the first day of the free training, how was it for you?{perf_hook}"
        return msg, "Day 1 engagement (D1)", "Completed Day 1, not started Day 2. Ask about experience."

    # ── Stage 6c: Day 2 complete, stalled before Day 3 ──
    if stage == FunnelStage.DAY2_COMPLETE:
        msg = f"Hey {first_name}, I see you completed the first 2 days of the Free Training but missed the third, is everything ok with the link and platform for you?"
        return msg, "Day 2 check (D2)", "Completed Day 2, not started Day 3. Check platform."

    # ── Stage 7: All days complete → Book strategy call ──
    if stage == FunnelStage.STRATEGY_CALL_BOOKED:
        perf_hook = ""
        if perf_detail.get('race_pos'):
            perf_hook = f"\n- Where your current P{perf_detail['race_pos']} finishes are leaving time on the table"
        msg = f"""Hey {first_name}

Saw you smashed through all 3 days of the training - solid work

You've unlocked your free championship strategy session. This is where we map out your specific performance roadmap for 2026.

On the call we'll cover:

- Your biggest mental performance gap (and how to fix it){perf_hook}
- The exact system you need for consistent results
- Whether the Flow Performance programme is right for you

Got a few slots this week - want to grab one?"""
        return msg, "Call booking prompt", "All 3 days complete. Offer strategy call with urgency."

    # ── Fallback ──
    msg = f"Hey {first_name}, just checking in - how are things going? Let me know if you need anything!"
    return msg, "General check-in", "No specific stage template. Generic follow-up."


def _driver_needs_follow_up(driver):
    """Check if driver has been at their current stage for 24h+ and needs a nudge."""
    if driver.current_stage in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED]:
        return False, 0
    date_attr = _STAGE_DATE_ATTRS.get(driver.current_stage)
    if not date_attr:
        return False, 0
    d = getattr(driver, date_attr, None)
    if not d or not isinstance(d, datetime):
        d = getattr(driver, 'last_activity', None)
    if not d or not isinstance(d, datetime):
        d = getattr(driver, 'outreach_date', None)
    if d and isinstance(d, datetime):
        hours = (datetime.now() - d).total_seconds() / 3600
        days = max(0, int(hours / 24))
        return hours >= 24, days
    return False, 0


# ═══════════════════════════════════════════════════════════════════════════
# HANDLE REPLY — Context-aware reply engine
# Checks what the driver has already done and produces the right next-step
# reply to move them toward Review → Free Training → Strategy Call.
# ═══════════════════════════════════════════════════════════════════════════

def _build_driver_progress(driver, dashboard=None):
    """Build a structured progress dict showing what the driver has completed.
    Cross-references by name across all drivers to catch ScoreApp completions."""
    progress = {
        'messaged': driver.current_stage not in [FunnelStage.CONTACT] and driver.outreach_date is not None,
        'replied': driver.replied_date is not None,
        'race_review_started': driver.race_weekend_review_date is not None,
        'race_review_done': driver.current_stage in [FunnelStage.RACE_REVIEW_COMPLETE] or (
            driver.race_weekend_review_status == 'completed'),
        'link_sent': driver.link_sent_date is not None,
        'registered': driver.registered_date is not None,
        'day1_done': driver.day1_complete_date is not None,
        'day2_done': driver.day2_complete_date is not None,
        'call_booked': driver.strategy_call_booked_date is not None,
        'client': driver.current_stage in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED],
        # Lead magnets
        'has_flow_profile': driver.flow_profile_result is not None,
        'has_sleep_test': driver.sleep_score is not None,
        'has_mindset_quiz': driver.mindset_result is not None,
        # Scores
        'day1_score': driver.day1_score,
        'day2_scores': driver.day2_scores,
        'flow_profile_result': driver.flow_profile_result,
        'sleep_score': driver.sleep_score,
        'mindset_result': driver.mindset_result,
    }

    # NAME-BASED CROSS-REFERENCE: enrich progress from matching drivers
    if dashboard and not progress['race_review_done']:
        _driver_name_lower = (driver.full_name or '').lower().strip()
        _driver_first_lower = (driver.first_name or '').lower().strip()
        _driver_last_lower = (driver.last_name or '').lower().strip()
        if _driver_name_lower and len(_driver_name_lower) > 2:
            all_drivers = getattr(dashboard, 'drivers', {})
            if not all_drivers and hasattr(dashboard, 'data_loader'):
                all_drivers = getattr(dashboard.data_loader, 'drivers', {})
            for _xk, _xr in all_drivers.items():
                if _xr is driver:
                    continue
                _xr_name = (getattr(_xr, 'full_name', '') or '').lower().strip()
                _xr_first = (getattr(_xr, 'first_name', '') or '').lower().strip()
                _xr_last = (getattr(_xr, 'last_name', '') or '').lower().strip()
                _matched = False
                if _driver_name_lower and _xr_name and _driver_name_lower == _xr_name:
                    _matched = True
                elif (_driver_first_lower and _driver_last_lower and
                      _xr_first == _driver_first_lower and _xr_last == _driver_last_lower):
                    _matched = True
                if _matched:
                    if getattr(_xr, 'race_weekend_review_status', '') == 'completed' or getattr(_xr, 'race_weekend_review_date', None):
                        progress['race_review_done'] = True
                        progress['race_review_started'] = True
                    if getattr(_xr, 'registered_date', None):
                        progress['registered'] = True
                    if getattr(_xr, 'day1_complete_date', None):
                        progress['day1_done'] = True
                        if not progress['day1_score'] and getattr(_xr, 'day1_score', None):
                            progress['day1_score'] = _xr.day1_score
                    if getattr(_xr, 'day2_complete_date', None):
                        progress['day2_done'] = True
                    if getattr(_xr, 'strategy_call_booked_date', None):
                        progress['call_booked'] = True
                    if not progress['has_flow_profile'] and getattr(_xr, 'flow_profile_result', None):
                        progress['has_flow_profile'] = True
                        progress['flow_profile_result'] = _xr.flow_profile_result
                    if not progress['has_sleep_test'] and getattr(_xr, 'sleep_score', None):
                        progress['has_sleep_test'] = True
                        progress['sleep_score'] = _xr.sleep_score
                    break

    return progress


def _determine_reply_goal(driver, progress):
    """Determine the ideal next step for this driver and return (goal_key, goal_label, goal_description)."""
    stage = driver.current_stage

    # Already a client — nothing to push
    if progress['client']:
        return 'client', '🏆 Client', 'Already a client — nurture the relationship.'

    # Call booked — confirm and prep
    if progress['call_booked'] or stage == FunnelStage.STRATEGY_CALL_BOOKED:
        return 'confirm_call', '📞 Confirm Strategy Call', 'Call is booked — confirm, build excitement, prep them.'

    # Day 2 done → push to strategy call
    if progress['day2_done'] or stage == FunnelStage.DAY2_COMPLETE:
        return 'book_call', '📞 Book Strategy Call', 'Completed Day 2 — next step is booking the strategy call.'

    # Day 1 done → push to Day 2
    if progress['day1_done'] or stage == FunnelStage.DAY1_COMPLETE:
        return 'push_day2', '📝 Complete Day 2', 'Completed Day 1 — encourage them to complete Day 2 assessment.'

    # Registered / Blueprint Started → push to Day 1
    if progress['registered'] or stage in [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED]:
        return 'push_day1', '📚 Start Day 1', 'Registered but hasn\'t started Day 1 yet.'

    # Blueprint link sent → check they opened it
    if stage == FunnelStage.BLUEPRINT_LINK_SENT:
        return 'check_blueprint', '📚 Start Free Training', 'Blueprint link sent — check they opened it and nudge to start.'

    # Race review done → offer free training
    if progress['race_review_done'] or stage == FunnelStage.RACE_REVIEW_COMPLETE:
        return 'offer_training', '📚 Offer Free Training', 'Review completed — offer the Podium Contenders Blueprint.'

    # Link sent (review link) → check they started
    if progress['link_sent'] or stage in [FunnelStage.LINK_SENT]:
        return 'check_review', '📊 Complete Race Review', 'Review link sent — check if they started and nudge to finish.'

    # Race weekend started but not completed
    if progress['race_review_started'] or stage == FunnelStage.RACE_WEEKEND:
        return 'finish_review', '📊 Finish Race Review', 'Started the review but hasn\'t finished — nudge to complete.'

    # Replied → introduce yourself + offer review
    if progress['replied'] or stage == FunnelStage.REPLIED:
        return 'intro_offer', '📊 Offer Race Weekend Review', 'They replied — introduce yourself and offer the review tool.'

    # Messaged but no reply → soft follow-up
    if progress['messaged'] or stage in [FunnelStage.MESSAGED, FunnelStage.OUTREACH]:
        return 'follow_up_msg', '💬 Get a Reply', 'Messaged but no reply — soft follow-up.'

    # Lead magnet completed but not in pipeline — specific follow-up per type
    if progress['has_flow_profile']:
        return 'lm_flow_profile', '🧠 Flow Profile Follow-up', 'Completed Flow Profile, follow up and introduce Blueprint training.'
    if progress['has_mindset_quiz']:
        return 'lm_mindset_quiz', '💡 Mindset Quiz Follow-up', 'Completed Mindset Quiz, follow up and introduce Blueprint training.'
    if progress['has_sleep_test']:
        return 'lm_sleep_test', '😴 Sleep Test Follow-up', 'Completed Sleep Test, follow up and introduce Blueprint training.'

    # Contact — first outreach
    return 'first_outreach', '📨 Send First Message', 'No outreach yet — send the initial message.'


def _generate_thread_reply(driver, conversation_thread, goal_key, dashboard=None):
    """Analyze the full conversation thread + driver data to generate a single
    direct reply that accounts for everything that's happened so far.

    Thread audit checks:
    - Links already sent (scoreapp review, blueprint, booking)
    - "Yes" responses already actioned
    - How many outreach attempts
    - What was offered and whether it was taken up

    Driver data checks:
    - Current stage, review completion, training progress
    - Lead magnet scores, Day 1/Day 2 completion
    - Championship context

    Returns (label, message) or None if no thread.
    """
    if not conversation_thread or len(conversation_thread.strip()) < 5:
        return None

    import re as _re_thread

    _raw_name = driver.first_name or getattr(driver, 'display_name', None) or (driver.full_name.split()[0] if driver.full_name else "mate")
    name = _clean_first_name(_raw_name)
    if len(name) > 8 and ' ' not in name and driver.full_name and ' ' in driver.full_name:
        name = driver.full_name.split()[0].title()

    thread_lower = conversation_thread.lower()

    # ══════════════════════════════════════════════════════════════
    # STEP 1: FULL THREAD AUDIT — what's already happened?
    # ══════════════════════════════════════════════════════════════
    _review_link_sent = 'improve-driver.scoreapp.com' in thread_lower or 'scoreapp' in thread_lower
    _blueprint_link_sent = 'academy.caminocoaching' in thread_lower or 'podium-contenders-blueprint' in thread_lower
    _booking_link_sent = 'booking link' in thread_lower or 'calendly' in thread_lower
    _offered_review = any(w in thread_lower for w in [
        'race weekend review', 'post-race assessment', 'assessment tool',
        'where your gains are hiding', 'want me to send it over'])
    _offered_training = any(w in thread_lower for w in [
        'free training', 'podium contenders blueprint', 'blueprint'])
    _offered_call = any(w in thread_lower for w in [
        'strategy call', 'strategy session', 'book a call', 'grab a slot'])

    # ══════════════════════════════════════════════════════════════
    # STEP 2: RIDER DATA AUDIT — what have they actually done?
    # Cross-references by NAME across all drivers to catch ScoreApp
    # completions even when email doesn't match the pipeline entry.
    # ══════════════════════════════════════════════════════════════
    _review_completed = (
        driver.race_weekend_review_status == 'completed' or
        driver.current_stage in [FunnelStage.RACE_REVIEW_COMPLETE] or
        driver.race_weekend_review_date is not None
    )
    _training_started = driver.registered_date is not None
    _day1_done = driver.day1_complete_date is not None
    _day2_done = driver.day2_complete_date is not None
    _call_booked = driver.strategy_call_booked_date is not None
    _is_client = driver.current_stage in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED]

    # NAME-BASED CROSS-REFERENCE: Search all drivers by name to find
    # matching ScoreApp entries when email doesn't match
    _xref_driver = None
    if dashboard and not _review_completed:
        _driver_name_lower = (driver.full_name or '').lower().strip()
        _driver_first_lower = (driver.first_name or '').lower().strip()
        _driver_last_lower = (driver.last_name or '').lower().strip()
        if _driver_name_lower and len(_driver_name_lower) > 2:
            all_drivers = getattr(dashboard, 'drivers', {})
            if not all_drivers and hasattr(dashboard, 'data_loader'):
                all_drivers = getattr(dashboard.data_loader, 'drivers', {})
            for _xk, _xr in all_drivers.items():
                if _xr is driver:
                    continue  # Skip self
                _xr_name = (getattr(_xr, 'full_name', '') or '').lower().strip()
                _xr_first = (getattr(_xr, 'first_name', '') or '').lower().strip()
                _xr_last = (getattr(_xr, 'last_name', '') or '').lower().strip()
                # Match by full name or first+last name
                _name_match_found = False
                if _driver_name_lower and _xr_name and _driver_name_lower == _xr_name:
                    _name_match_found = True
                elif (_driver_first_lower and _driver_last_lower and
                      _xr_first == _driver_first_lower and _xr_last == _driver_last_lower):
                    _name_match_found = True
                if _name_match_found:
                    # Found a name match — check their data
                    if (getattr(_xr, 'race_weekend_review_status', '') == 'completed' or
                        getattr(_xr, 'race_weekend_review_date', None) is not None):
                        _review_completed = True
                        _xref_driver = _xr
                    if getattr(_xr, 'registered_date', None) is not None:
                        _training_started = True
                        _xref_driver = _xr
                    if getattr(_xr, 'day1_complete_date', None) is not None:
                        _day1_done = True
                        _xref_driver = _xr
                    if getattr(_xr, 'day2_complete_date', None) is not None:
                        _day2_done = True
                        _xref_driver = _xr
                    if getattr(_xr, 'strategy_call_booked_date', None) is not None:
                        _call_booked = True
                        _xref_driver = _xr
                    break  # Found match, stop searching

    # ══════════════════════════════════════════════════════════════
    # STEP 3: LAST MESSAGE ANALYSIS — what did they just say?
    # ══════════════════════════════════════════════════════════════
    last_msg = _extract_last_their_message(conversation_thread)
    if not last_msg:
        last_msg = conversation_thread.strip()
    msg_lower = last_msg.lower()

    # Detect parent/family
    _parent_words = ['father', 'dad', 'mother', 'mum', 'mom', 'parent', 'wife', 'husband',
                     'partner', 'his account', 'her account', 'on behalf',
                     "son's", "daughter's", "my son", "my daughter", "my lad", "the boy"]
    _is_parent = any(w in msg_lower for w in _parent_words)

    _parent_name = None
    _name_match = _re_thread.search(r"(?:[Tt]his is|[Ii]'m|[Ii] am|[Mm]y name is|[Ii]t's)\s+([A-Z][a-z]+)", last_msg)
    if _name_match and _is_parent:
        _parent_name = _name_match.group(1)

    _mentions_meetup = any(w in msg_lower for w in [
        'next round', 'next race', 'at the track', 'catch up',
        'at the event', 'come and find', 'see you at', 'at the next', 'in the paddock'])

    # ══════════════════════════════════════════════════════════════
    # STEP 4: BUILD THE REPLY — using full context
    # ══════════════════════════════════════════════════════════════

    # --- PARENT REPLYING ---
    if _is_parent:
        greeting = f"Hi{' ' + _parent_name if _parent_name else ''}! Thanks for letting me know"

        # What should we say about next steps? Depends on what's already happened
        if _review_link_sent and not _review_completed:
            next_step = (
                f"I sent {name} the race weekend review link a while back but I don't think he's had a chance to complete it yet. "
                f"No rush at all, it only takes about 5 minutes and shows exactly where the gains are hiding.\n\n"
                f"If you could pass that on to {name} when you get a chance, that would be brilliant 👍")
        elif _review_completed and not _training_started:
            next_step = (
                f"{name} actually completed the race weekend review, really interesting results. "
                f"Based on that, he's qualified for some free training that a lot of drivers are using.\n\n"
                f"Happy to send {name} the link whenever suits 👍")
        elif _training_started and not _day1_done:
            next_step = (
                f"{name} signed up for the free training but hasn't had a chance to start Day 1 yet. "
                f"Only takes about 20 minutes, would be great for {name} to have a look when he gets a chance 👍")
        elif _day1_done and not _day2_done:
            next_step = (
                f"{name} smashed through Day 1 of the training, some great insights in there. "
                f"Day 2 shows exactly which areas will give the biggest gains. "
                f"Would be great for {name} to finish that off when he gets a chance 👍")
        elif _day2_done and not _call_booked:
            next_step = (
                f"{name} has completed the training, solid work. "
                f"The next step is a free strategy call where we look at everything together. "
                f"If {name} fancies booking one, I'll send the link over 👍")
        elif _offered_review and not _review_link_sent:
            next_step = (
                f"I mentioned a race weekend review tool to {name}, it shows drivers exactly where the gains are. "
                f"Only takes about 5 minutes. Happy to send the link whenever suits 👍")
        else:
            next_step = (
                f"I work with drivers on the mental performance side of racing, helping them find consistency and flow state under pressure. "
                f"Happy to send some info over whenever {name}'s ready 🏁")

        if _mentions_meetup:
            meetup_line = (
                f", really appreciate you passing the message on 👍\n\n"
                f"That's great that {name} is keen to have a chat, "
                f"all the work I do with drivers is online so we can sort something out easily.\n\n")
        else:
            meetup_line = (
                f", no worries about the account situation, happens a lot with racing accounts! 👍\n\n")

        return ("Direct Reply",
            f"{greeting}{meetup_line}{next_step}")

    # --- "YES" / POSITIVE INTEREST ---
    _yes_words = ['yes', 'yeah', 'yep', 'sure', 'go on then', 'send it', 'send it over',
                  'sounds good', 'interested', 'definitely', 'go for it', 'why not',
                  'please', 'ok send', 'that would be great', 'love to']
    if any(w in msg_lower for w in _yes_words) and len(msg_lower) < 100:
        if _review_link_sent and not _review_completed:
            return ("Direct Reply",
                f"The link's still active {name}, here it is again:\nhttps://improve-driver.scoreapp.com\n\n"
                f"Takes about 5 minutes. There's some free training at the bottom of the results page too 👍🏻")
        elif _review_completed and not _blueprint_link_sent:
            return ("Direct Reply",
                f"OK {name}, here you go - instant access to the Podium Contenders Blueprint:\n"
                f"https://academy.caminocoaching.co.uk/podium-contenders-blueprint/order/\n\n"
                f"📚 What you'll learn:\n✓ Day 1: The 7 biggest mistakes costing you lap times\n"
                f"✓ Day 2: The 5-pillar system for accessing flow state on command\n"
                f"✓ Day 3: Your race weekend mental preparation protocol\n\n"
                f"Complete all 3 days and you'll unlock a free strategy call.\nSee you inside! 🏁\nCraig")
        elif not _review_link_sent:
            return ("Direct Reply",
                f"Superb {name}! Here's the link to The Post-Race Weekend Performance Score:\n"
                f"https://improve-driver.scoreapp.com\n\n"
                f"This short review zeroes in on where you're losing lap time, where any gaps are showing up and how to fill them 🚀\n\n"
                f"At the bottom of the results page is some free training on how to fill those gaps 👍🏻")
        elif _day2_done and not _call_booked:
            return ("Direct Reply",
                f"Brilliant {name}! Here's the link to grab a slot:\n[BOOKING LINK]\n\n"
                f"Pick a time that suits you best. Looking forward to it! 🏁")

    # --- QUESTIONS ---
    _question_words = ['what do you', 'how does', 'what is', 'tell me more', 'how do you',
                       'what exactly', 'what kind', 'explain', 'more info', 'what does it involve']
    if any(w in msg_lower for w in _question_words):
        return ("Direct Reply",
            f"Great question {name}!\n\n"
            f"I'm a Flow Performance Coach, I work with drivers on the mental side of racing. "
            f"Think of it as the missing piece between physical fitness, bike setup, and actual on-track performance.\n\n"
            f"I help drivers access the Flow State, that zone where everything clicks, your reactions are automatic, "
            f"and you're riding at your best without overthinking.\n\n"
            f"The quickest way to see where your gains are is a short post-race assessment I built. "
            f"Takes about 5 mins and shows you exactly what's holding you back.\n\n"
            f"Want me to send it over?")

    # --- BUSY / LATER ---
    _busy_words = ['busy', 'later', 'when i get time', 'get back to you', 'speak soon',
                   'catch up later', 'not now', 'remind me', 'will do', "i'll have a look",
                   'will check', 'tomorrow', 'next week', 'after the weekend']
    if any(w in msg_lower for w in _busy_words):
        return ("Direct Reply",
            f"No rush at all {name} 👍\n\n"
            f"Whenever you get a minute, the link's there ready for you. "
            f"Takes about 5 minutes and most drivers say it's an eye-opener.\n\n"
            f"Good luck with the prep! 🏁")

    # --- MENTIONS CATCHING UP ---
    if _mentions_meetup:
        if _review_link_sent and not _review_completed:
            extra = (f"I sent the race weekend review link over previously, might be worth doing before the next round "
                     f"so we've got something specific to chat about 👍")
        else:
            extra = (f"I've got a quick post-race assessment that shows where the gains are hiding. "
                     f"Might be worth doing before the next round so we've got something specific to go through 🏁")
        return ("Direct Reply",
            f"That would be great {name}! All the work I do with drivers is online so we can easily sort a time to chat.\n\n{extra}")

    # --- NOT INTERESTED ---
    _no_words = ['no thanks', 'not interested', 'not for me', 'no ta', 'nah', 'not right now',
                 'all sorted', 'got a coach', 'already have', 'happy as i am']
    if any(w in msg_lower for w in _no_words):
        return ("Direct Reply",
            f"No worries at all {name}, totally understand 👍\n\n"
            f"If anything changes or you fancy a chat about the mental side of racing in future, "
            f"the door's always open.\n\n"
            f"Good luck with the rest of the season! 🏁")

    # --- WEEKEND CHAT (sentiment-based) ---
    sentiment = _classify_sentiment(last_msg)
    if sentiment == "great":
        if _review_link_sent and not _review_completed:
            offer = (f"I sent you the race weekend review link a while back, now would be a perfect time to do it "
                     f"while that good form is fresh. Shows you exactly WHY the good weekends are good so you can lock it in 👍")
        else:
            offer = (f"I've built a quick post-race review that helps drivers understand exactly WHY "
                     f"the good weekends are good, so you can repeat it.\n\nWant me to send it over?")
        return ("Direct Reply",
            f"That's brilliant {name}, well done! 💪\n\n"
            f"Sounds like things are clicking.\n\n{offer}")
    elif sentiment == "tough":
        if _review_link_sent and not _review_completed:
            offer = (f"I sent you the race weekend review link previously, now might be a good time to do it. "
                     f"It pinpoints exactly what happened and how to fix it before the next round.")
        else:
            offer = (f"I've got a quick post-race review tool that helps drivers pinpoint exactly what happened "
                     f"and how to fix it before the next round. Most drivers say 'I had no idea THAT was the thing'.\n\n"
                     f"Want me to send the link?")
        return ("Direct Reply",
            f"Ah mate, that's frustrating {name}. Those weekends are the hardest.\n\n"
            f"The thing is, the mental side of a tough weekend affects the NEXT weekend too, "
            f"if you don't process it properly.\n\n{offer}")

    # --- FALLBACK: Acknowledge + context-aware nudge ---
    if _review_link_sent and not _review_completed:
        nudge = (f"Just checking, did you get a chance to do the race weekend review I sent over? "
                 f"The link's still active if you want to give it a go 👍")
    elif _review_completed and not _training_started:
        nudge = (f"Your race weekend review flagged some really interesting patterns. "
                 f"I've got some free training that covers exactly how to fix those gaps, want me to send the link?")
    elif _training_started and not _day1_done:
        nudge = (f"I see you signed into the free training, was everything ok with the platform? "
                 f"Day 1 only takes about 20 minutes and drivers are saying it's been a game-changer 👍")
    elif _day1_done and not _day2_done:
        nudge = (f"Great stuff on completing Day 1! Day 2 is the 5-Pillar Assessment, "
                 f"shows you exactly which areas will give you the biggest gains. Ready to dive in?")
    elif _day2_done and not _call_booked:
        nudge = (f"You've completed both days of the training, solid work! "
                 f"The next step is a free strategy call where we map out your performance roadmap. "
                 f"Want me to send the booking link?")
    else:
        nudge = (f"I work with drivers on the mental performance side of racing, helping them find consistency "
                 f"and access the Flow State where everything clicks.\n\n"
                 f"I've got a quick assessment that shows exactly where the gains are hiding. "
                 f"Takes about 5 minutes, would you be up for giving it a go?")

    return ("Direct Reply",
        f"Thanks for getting back to me {name}, really appreciate it 👍\n\n{nudge}")


def _generate_handle_reply_messages(driver, progress, goal_key, conversation_thread=""):
    """Generate 2-3 reply options based on driver's context and goal."""
    _raw_name = driver.first_name or getattr(driver, 'display_name', None) or (driver.full_name.split()[0] if driver.full_name else "mate")
    name = _clean_first_name(_raw_name)
    if len(name) > 8 and ' ' not in name and driver.full_name and ' ' in driver.full_name:
        name = driver.full_name.split()[0].title()

    last_msg = _extract_last_their_message(conversation_thread)
    sentiment = _classify_sentiment(last_msg)

    replies = []

    # ── THREAD-AWARE REPLY (always first when thread exists) ──
    thread_reply = _generate_thread_reply(driver, conversation_thread, goal_key)
    if thread_reply:
        replies.append(thread_reply)

    if goal_key == 'follow_up_msg':
        replies.append(("Soft Check-in", f"Hey {name}, just checking in - did you see my message? 👋"))
        if driver.championship:
            replies.append(("Championship Hook", f"Hey {name}, just following up - how's the {driver.championship} prep going? Did you see my message?"))
        replies.append(("Value Lead", f"Hey {name}, I know you're busy with race prep so I'll keep this short - saw something in your recent results that caught my eye. Did you get my earlier message?"))

    elif goal_key == 'intro_offer':
        # Adapt opener to their sentiment
        if sentiment == "great":
            opener = "That's great work, well done!"
        elif sentiment == "tough":
            opener = "Sounds like you had a tough weekend."
        else:
            opener = "Sounds like you had a productive weekend!"

        replies.append(("Full Intro + Review Offer",
            f"Thanks for the reply {name}.\n\n{opener}\n\nNot sure if you know - I'm a Flow Performance Coach. A bit different from the usual driver-coach.\n\nI work with drivers in many championships on the mental side of racing - helping them access the Flow State, where performance becomes automatic, consistent, and confident under pressure.\n\nI've built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.\n\nWant me to send it over?"))

        replies.append(("Short + Direct",
            f"Thanks for the reply {name}! {opener}\n\nI actually built a quick race weekend performance tool that shows drivers exactly where the gains are hiding. Takes about 5 mins.\n\nWant me to send the link?"))

        if progress.get('has_flow_profile') or progress.get('has_sleep_test'):
            lead_magnet_ref = ""
            if progress.get('flow_profile_result'):
                lead_magnet_ref = f" I noticed you already completed the Flow Profile ({progress['flow_profile_result']}) -"
            elif progress.get('sleep_score'):
                lead_magnet_ref = f" I see you scored {progress['sleep_score']} on the sleep test -"
            replies.append(("Reference Lead Magnet",
                f"Thanks {name}! {opener}{lead_magnet_ref} this assessment goes a step further and maps your specific race weekend performance.\n\nWant me to send it over?"))

    elif goal_key == 'finish_review':
        replies.append(("Encourage Completion",
            f"Hey {name}, I see you started the Race Weekend Review but didn't get to the results page - that's where the good stuff is.\n\nYour results break down exactly where you're losing time and why - most drivers tell me they had no idea THAT was what held them back.\n\nTakes about 3 minutes to finish - want me to resend the link?"))
        replies.append(("Quick Nudge",
            f"Hey {name}, just checking in - did you manage to finish the race weekend review? The results page is where it really clicks for most drivers 👍"))

    elif goal_key == 'check_review':
        replies.append(("Check Link V1",
            f"Hey {name}, just checking in. Is the link I sent working for you as I see you haven't started yet?"))
        replies.append(("Check Link V2",
            f"Hi {name}, just wanted to make sure the link came through ok - let me know if you need me to resend it 👍"))
        replies.append(("Value Reminder",
            f"Hey {name}, just circling back on the race weekend review I sent over. Takes about 5 minutes and shows exactly where the gains are hiding for you.\n\nLet me know if the link didn't work 👍"))

    elif goal_key == 'offer_training':
        replies.append(("Results-Led",
            f"Hey {name}, great to see you completed the race weekend review!\n\nBased on your results, you've qualified for our free training that many drivers are using to ensure they are on point from the first round.\n\nWant me to send it over?"))
        if progress.get('day1_score'):
            replies.append(("Score Reference",
                f"Hey {name}, your review flagged some really interesting patterns. The free training covers exactly how to fix those gaps.\n\nWant me to send the link?"))
        replies.append(("FOMO Angle",
            f"Hey {name}, your race weekend review results actually flagged a couple of areas that our Free Training covers in detail - specifically how the top drivers manage those exact patterns you scored on.\n\nWant me to send you the link?\nCraig"))

    elif goal_key == 'check_blueprint':
        replies.append(("Platform Check",
            f"Hey {name}, just checking in - did you get a chance to look at the Podium Contenders Blueprint I sent over?\n\nMost drivers smash through Day 1 in about 20 minutes and say it completely changed how they think about their race weekends.\n\nThe link's still active if you want to dive in - let me know if you had any issues with it"))
        replies.append(("Quick Nudge",
            f"Hey {name}, just checking everything is ok with the training link I sent you?"))

    elif goal_key == 'push_day1':
        replies.append(("Platform Check",
            f"Hi {name}, I see you signed into the free training but didn't go much further - was everything ok with the link and the platform for you?"))
        replies.append(("Value Highlight",
            f"Hey {name}, Day 1 of the training only takes about 20 minutes and drivers are telling me it's been a game-changer for understanding where they're leaving time on track.\n\nYour link's still active - want me to resend it?"))

    elif goal_key == 'push_day2':
        score_ref = ""
        if progress.get('day1_score'):
            score_ref = f"\n\nYou scored {progress['day1_score']}/100 on Day 1 - there's definitely some low-hanging fruit to improve on."
        replies.append(("Score + Next Step",
            f"Hey {name}, great work on completing Day 1 of the free training!{score_ref}\n\nDay 2 is the 5-Pillar Assessment - it shows you exactly which areas will give you the biggest gains. Takes about 15 mins.\n\nReady to dive in?"))
        replies.append(("Engagement Check",
            f"Hey {name}, great work on completing the first day of the free training - how was it for you?"))

    elif goal_key == 'book_call':
        perf_hook = ""
        if driver.championship:
            perf_hook = f"\n- How to apply this specifically to {driver.championship}"
        replies.append(("Strategy Call Offer",
            f"Hey {name}, you've now completed both days of the training - solid work!\n\nYou've unlocked your free championship strategy session. This is where we map out your specific performance roadmap.\n\nOn the call we'll cover:\n- Your biggest mental performance gap (and how to fix it){perf_hook}\n- The exact system you need for consistent results\n- Whether the Flow Performance programme is right for you\n\nGot a few slots this week - want to grab one?"))
        replies.append(("Casual Approach",
            f"Hey {name}, saw you smashed through all the training - nice one! 💪\n\nThe next step is a Strategy Call where we look at your results together and figure out the best path forward for you.\n\nNo pressure, no hard sell - just a real conversation about your racing goals.\n\nShall I send the booking link?"))

    elif goal_key == 'confirm_call':
        replies.append(("Prep & Confirm",
            f"Hey {name}, looking forward to our strategy call!\n\nJust to make sure we get the most out of it, have a think about:\n- Your #1 goal for this season\n- The biggest thing that holds you back on track\n\nSee you soon! 🏁"))
        replies.append(("Excitement Builder",
            f"Hey {name}, just wanted to confirm our call is good to go. Based on your training results, I've already spotted some quick wins we can talk through.\n\nSee you there! 💪"))

    elif goal_key == 'lm_flow_profile':
        replies.append(("Flow Profile Follow-up",
            f"Hi {name}, how was the Flow Profile for you and did you think the profile it generated was accurate?\n\nAlso, did you see we are running some free training at the moment on the mental approach to your riding and how it can help your performance.\n\nDo you want me to send you the link?"))
        if progress.get('flow_profile_result'):
            replies.append(("Reference Result",
                f"Hey {name}, I saw your Flow Profile came back as '{progress['flow_profile_result']}', that's really interesting.\n\nWe actually have a free training that goes deeper into how to use that profile on track. Drivers in JuniorGP and WSBK are using it right now.\n\nWant me to send you the link?"))

    elif goal_key == 'lm_mindset_quiz':
        replies.append(("Mindset Quiz Follow-up",
            f"Hey {name}, how were the questions in the Mindset Quiz for you? Did any of them make you think?\n\nAlso, did you see we are running some free training at the moment on the mental approach to your riding and how it can help your performance.\n\nDo you want me to send you the link?"))
        if progress.get('mindset_result'):
            replies.append(("Reference Score",
                f"Hey {name}, I see you scored {progress['mindset_result']} on the Mindset Quiz. Some really interesting patterns in there.\n\nWe have a free training that covers exactly how to work on the areas the quiz highlighted. Drivers across multiple championships are using it.\n\nWant me to send you the link?"))

    elif goal_key == 'lm_sleep_test':
        replies.append(("Sleep Test Follow-up",
            f"Hey {name}, how did you find the Sleep Test? Were you surprised by your score?\n\nAlso, did you see we are running some free training at the moment on the mental approach to your riding and how it can help your performance. Sleep is a big part of it.\n\nDo you want me to send you the link?"))
        if progress.get('sleep_score'):
            replies.append(("Reference Score",
                f"Hey {name}, I see you scored {progress['sleep_score']} on the Sleep Test. That tells me a lot about where you can improve off the bike too.\n\nWe have a free training that covers all of this, including how sleep directly impacts your lap times and consistency.\n\nWant me to send you the link?"))

    elif goal_key == 'lm_follow_up':
        # Legacy fallback
        lm_ref = ""
        if progress.get('flow_profile_result'):
            lm_ref = f"I see you got '{progress['flow_profile_result']}' on your Flow Profile"
        elif progress.get('mindset_result'):
            lm_ref = f"I noticed you completed the Mindset Quiz ({progress['mindset_result']})"
        elif progress.get('sleep_score'):
            lm_ref = f"I see you scored {progress['sleep_score']} on the Sleep Test"
        replies.append(("Lead Magnet Follow-up",
            f"Hey {name}, {lm_ref}, really interesting results.\n\nWe are running some free training at the moment on the mental approach to your riding and how it can help your performance.\n\nDo you want me to send you the link?"))

    elif goal_key == 'client':
        replies.append(("Check-in",
            f"Hey {name}, just checking in - how are things going since we started working together? Anything you need from me? 💪"))

    elif goal_key == 'first_outreach':
        if driver.championship:
            replies.append(("Championship-Based",
                f"Hey {name}, I see you're racing in {driver.championship} - how's the prep going for this season?"))
        replies.append(("Generic Opener",
            f"Hey {name}, I came across your profile - how's the racing going?"))

    # Always ensure at least one reply
    if not replies:
        replies.append(("General Follow-up",
            f"Hey {name}, just checking in - how are things going? Let me know if you need anything! 👍"))

    return replies


# --- CONSTANTS ---
REPLY_TEMPLATES = {
    # --- COLD OUTREACH RESPONSES ---
    "Great Work (Reply)": """Thanks for the reply {name},
That’s Great work well done!

Not sure if you know, I’m a Flow Performance Coach. A bit different from the usual driver-coach.

I work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.

I’ve built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.

Want me to send it over?""",

    "Productive (Reply)": """Thanks for the reply {name},
Sounds like you had a productive weekend.

Not sure if you know, I’m a Flow Performance Coach. A bit different from the usual driver-coach.

I work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.

I’ve built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.

Want me to send it over?""",

    "Tough Weekend (Reply)": """Thanks for the reply {name}, it Sounds like you had a tough weekend.

Not sure if you know, I’m a Flow Performance Coach. A bit different from the usual driver-coach.

I work with drivers in many championships on the mental side of racing, helping them access the Flow State where performance becomes automatic, consistent, and confident under pressure.

I’ve built a free post-race assessment tool that shows exactly where your gains are hiding and how to unlock them in time for the next round.

Want me to send it over?""",

    "Send Link (Yes)": """Superb, {name} Here is the link to The Post-Race Weekend Performance Score
https://improve-driver.scoreapp.com

This short review zeroes in on where you’re losing lap time, where any gaps are showing up and how to fill them 🚀

At the bottom of the results page is some free training on how to fill those gaps 👍🏻""",

    # --- REVIEW STALLED ---
    "Stalled: Review Started": """Hey {name}, I see you started the Race Weekend Review but didn't get to the results page - that's where the good stuff is.

Your results break down exactly where you're losing time and why - most drivers tell me they had no idea THAT was the thing holding them back.

Plus the results page unlocks access to free training that covers how to fix those exact gaps before the next round.

Takes about 3 minutes to finish - want me to resend the link?""",

    # --- PIPELINE FOLLOW-UPS ---
    "Follow-Up (Link Sent) V1": """Hey, {name} just checking in. Is the link I sent working for you as I see you haven't started yet?""",

    "Follow-Up (Link Sent) V2": """Hey {name}, just checking everything is ok with the link I sent you?""",

    "Follow-Up (Link Sent) V3": """Hi {name}, just wanted to make sure the link came through ok - let me know if you need me to resend it 👍""",

    "Follow-Up (Review 2 Days) V1": """Hey {name}
Just checking in - did you get a chance to go through the post-race review I sent over?
Takes about 5 minutes and shows exactly where the gains are hiding for you.
Let me know if the link didn't work or if you had any issues with it 👍""",

    "Follow-Up (Review 2 Days) V2": """{name} - wanted to circle back on the race weekend assessment
Most drivers who complete it say the same thing: 'I didn't realise THAT was what was holding me back'
If you're still interested, the link's below. If not, no worries - good luck with the rest of the season 👍""",

    "Offer Free Training": """Hey {name}, Great to see you will be lining up on the grid this season
We have some pre-season free training that many drivers are using to ensure they are on point from the first round this season.
Want me to send it over?""",

    "Send Blueprint Link": """OK {name} here you go, instant access to the Podium Contenders Blueprint
https://academy.caminocoaching.co.uk/podium-contenders-blueprint/order/

📚 What you'll learn:
✓ Day 1: The 7 biggest mistakes costing you lap times
✓ Day 2: The 5-pillar system for accessing flow state on command
✓ Day 3: Your race weekend mental preparation protocol

Complete all 3 days, and you'll unlock a free strategy call where we'll create your personalised performance roadmap for 2026.
See you inside! 🏁
Craig""",

    "Follow-Up (Review Done → Blueprint)": """Hey {name},
Saw you completed the Race Weekend Review — nice one. Most drivers never even get that far. They just keep doing the same thing and wondering why nothing changes.
Your results actually flagged a couple of areas that the Free Training covers in detail, specifically how the top drivers manage those exact patterns you scored on.

Want me to send you the link?
Craig""",

    # --- TRAINING PROGRESS NUDGES ---
    "Stalled: Signed In": """Hi {name} I see you signed into the free training but didn't go much further was everything ok with the link and the platform for you?""",

    "Stalled: Day 1 Only": """Hey, {name}, Great work on completing the first day of the free training how was it for you?""",

    "Stalled: Day 2 Only": """Hey {name}, I see you completed the first 2 days of the Free Training but missed the third, is everything ok with the link and platform for you?""",

    "Stalled: Day 3 Only": """Hey {name}, I see you completed the Free Training but haven't booked your free strategy call yet.
I have a few slots open this week if you want to dial in your plan for the season?""",

    # --- RESCUE DMs ---
    "Rescue: Day 1 Nudge": """Hey {name}! 👋

Noticed you signed up for the Podium Contenders Blueprint but haven't done Day 1 yet.

The 7 Biggest Mistakes assessment only takes 20 mins and drivers are telling me it's been a game-changer for understanding where they're leaving time on track.

Your link's still active - want me to resend it?

Let me know if you have any questions!""",

    "Rescue: Day 2 Nudge": """Hey {name}!

Loved seeing your Day 1 results - some really interesting patterns there.

Day 2's 5-Pillar Assessment is where it all comes together though. It shows you exactly which areas will give you the biggest gains.

Takes about 15 mins - you ready to dive in?

Here's your link: https://academy.caminocoaching.co.uk/podium-contenders-blueprint/order/""",

    "Rescue: Book Strategy Call": """Hey {name}!

You've done Day 1 AND Day 2 - that's awesome! You're clearly serious about this.

The next step is a Strategy Call where we look at your results together and figure out the best path forward for you.

No pressure, no hard sell - just a real conversation about your racing goals.

I've got some spots open - shall I send the booking link?"""
}

def render_unified_card_content(driver, dashboard, key_suffix="", default_event_name=None):
    """
    Renders the rich contact card (2 columns).
    NOTE: @st.fragment removed — it conflicts with @st.dialog causing double
    render cycles and slow card opening. The dialog already isolates reruns.
    Used in:
    1. Race Outreach (Inline)
    2. Funnel Dashboard (Dialog)
    3. Database Page (Dialog)
    """

    # DEFENSIVE: Ensure driver always has a usable email for button keys & lookups.
    # Social media leads loaded before slug-fix may still have empty email.
    if not driver.email and driver.full_name:
        slug = driver.full_name.lower().strip().replace(' ', '_')
        slug = "".join([c for c in slug if c.isalnum() or c == '_'])
        driver.email = f"no_email_{slug}"
        # Store alias so _find_driver_by_key can resolve it
        if hasattr(dashboard, 'data_loader') and hasattr(dashboard.data_loader, '_key_aliases'):
            dashboard.data_loader._key_aliases[driver.email] = next(
                (k for k, v in dashboard.drivers.items() if v is driver), driver.email
            )

    uc1, uc2 = st.columns(2)
    
    # Ensure we have a usable identifier for buttons/keys
    effective_email = driver.email
    if not effective_email:
        # Generate slug consistent with app.py logic
        safe_name = driver.full_name or "unknown_driver"
        slug = safe_name.lower().strip().replace(' ', '_')
        slug = "".join([c for c in slug if c.isalnum() or c == '_'])
        effective_email = f"no_email_{slug}"

    # === FOLLOW-UP STATUS BANNER (top of card) ===
    needs_fu, fu_days = _driver_needs_follow_up(driver)
    fu_sent_recently, fu_sent_time = _has_recent_follow_up_sent(driver)

    if fu_sent_recently and fu_sent_time:
        hours_since = int((datetime.now() - fu_sent_time).total_seconds() / 3600)
        st.success(f"✅ **Follow-up sent** {hours_since}h ago — awaiting reply")
    elif needs_fu:
        first_name = driver.first_name or driver.full_name.split()[0] if driver.full_name else "mate"
        msg_template = "placeholder"  # Will be replaced by AI generator below
        _ = msg_template  # suppress unused
        stage_label = driver.current_stage.value if hasattr(driver.current_stage, 'value') else str(driver.current_stage)

        if fu_days >= 7:
            st.error(f"🔴 **Follow up needed** — {stage_label} for {fu_days} days")
        elif fu_days >= 3:
            st.warning(f"🟠 **Follow up needed** — {stage_label} for {fu_days} days")
        else:
            st.info(f"🟡 **Follow up needed** — {stage_label} for {fu_days} days")

    # ═══════════════════════════════════════════════════════════════
    # LEFT COLUMN: Message Thread + AI Message Generator
    # ═══════════════════════════════════════════════════════════════
    with uc1:
        existing_notes = driver.notes or ""

        # --- LOAD SAVED THREAD ---
        _saved_thread = ""
        _thread_platform = None  # 'FB' or 'IG'

        # Check for [THREAD] block (new format from extension or paste)
        _saved_match = _re.search(
            r'\[THREAD\](.*?)\[/THREAD\]',
            existing_notes, flags=_re.DOTALL
        )
        if _saved_match:
            _saved_thread = _saved_match.group(1).strip()
        # Fallback: old Chrome extension format
        if not _saved_thread:
            _captured_match = _re.search(
                r'\[\d{2} \w{3} \d{2}:\d{2} 📱 (?:FB|IG)\] Captured thread:(.*?)(?=\[|\Z)',
                existing_notes, flags=_re.DOTALL
            )
            if _captured_match:
                _saved_thread = _captured_match.group(1).strip()

        # Detect platform from saved notes marker
        _plat_match = _re.search(r'📱 (FB|IG)', existing_notes)
        if _plat_match:
            _thread_platform = _plat_match.group(1)
        # Detect from thread content patterns
        elif _saved_thread:
            if 'You sent' in _saved_thread or 'messenger' in _saved_thread.lower():
                _thread_platform = 'FB'
            elif 'Liked a message' in _saved_thread or 'Reacted' in _saved_thread:
                _thread_platform = 'IG'
        # Fallback: check what URLs the driver has
        if not _thread_platform:
            if driver.facebook_url and not driver.instagram_url:
                _thread_platform = 'FB'
            elif driver.instagram_url and not driver.facebook_url:
                _thread_platform = 'IG'

        # Platform badge
        if _thread_platform == 'FB':
            _plat_badge = '<span style="background:#1877F2;color:white;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;margin-left:8px;">Facebook</span>'
        elif _thread_platform == 'IG':
            _plat_badge = '<span style="background:#E1306C;color:white;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;margin-left:8px;">Instagram</span>'
        else:
            _plat_badge = ''

        # Use saved thread or any previously pasted text from session state for AI
        _paste_ss_key = f"_thread_input_{effective_email}_{key_suffix}"
        _pasted_from_ss = st.session_state.get(_paste_ss_key, "").strip()
        conversation_thread = _saved_thread or _pasted_from_ss

        # ═══════════════════════════════════════════════════════════════
        # HANDLE REPLY - primary action, shown first
        # ═══════════════════════════════════════════════════════════════

        # Build progress & determine goal
        _progress = _build_driver_progress(driver, dashboard=dashboard)
        _goal_key, _goal_label, _goal_desc = _determine_reply_goal(driver, _progress)

        # Progress bar + goal in one compact block
        _steps = [
            ('messaged', '📨'), ('replied', '↩️'), ('race_review_done', '📊'),
            ('link_sent', '🔗'), ('registered', '📚'), ('day1_done', 'D1'),
            ('day2_done', 'D2'), ('call_booked', '📞'), ('client', '🏆'),
        ]
        _check_items = []
        for _sk, _sl in _steps:
            _done = _progress.get(_sk, False)
            _check_items.append(f'<span style="opacity:{"1" if _done else "0.25"}">{_sl}</span>')
        st.markdown(
            '<div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:6px;padding:6px 12px;'
            'margin:8px 0 4px 0;font-size:13px;color:#111827;letter-spacing:1px;">'
            + " → ".join(_check_items)
            + '</div>',
            unsafe_allow_html=True
        )
        st.caption(f"Next: **{_goal_label}**")

        # Generate ONE direct reply
        _thread_reply = _generate_thread_reply(driver, conversation_thread, _goal_key, dashboard=dashboard)
        if _thread_reply:
            _reply_label, _reply_msg = _thread_reply
        else:
            _all_replies = _generate_handle_reply_messages(
                driver, _progress, _goal_key, conversation_thread
            )
            _reply_label, _reply_msg = _all_replies[0] if _all_replies else ("Follow-up", f"Hey {driver.first_name or 'mate'}, just checking in 👍")

        # Reply box - one-click copy via st.code copy button
        st.code(_reply_msg, language=None)

        if st.button("✅ Mark Sent", key=f"fu_sent_{effective_email}_{key_suffix}", use_container_width=True):
            timestamp = datetime.now().strftime("%d %b %H:%M")
            fu_entry = f"[{timestamp} ✅] Follow-up sent ({_reply_label})"
            updated_notes = f"{fu_entry}\n{existing_notes}" if existing_notes.strip() else fu_entry
            driver.notes = updated_notes
            dashboard.add_new_driver(
                effective_email, driver.first_name, driver.last_name,
                driver.facebook_url or "", ig_url=driver.instagram_url or "",
                championship=driver.championship or "", notes=updated_notes
            )
            st.toast(f"✅ Follow-up marked as sent for {driver.first_name}!")
            # Close the card dialog
            st.session_state.pop('_open_driver_card', None)
            if 'calendar_selected_driver' in st.session_state:
                del st.session_state['calendar_selected_driver']
                st.session_state['_cal_dismissed'] = True
            st.session_state['_stage_just_updated'] = True
            st.rerun(scope="app")

        # ═══════════════════════════════════════════════════════════════
        # THREAD DISPLAY - below reply message (used less often)
        # ═══════════════════════════════════════════════════════════════
        st.markdown(f"#### 💬 Thread {_plat_badge}", unsafe_allow_html=True)

        # --- SHOW CAPTURED THREAD (if exists) ---
        if _saved_thread:
            st.code(_saved_thread, language=None)
            # Find when it was captured
            _ts_match = _re.search(r'\[(\d{2} \w{3} \d{2}:\d{2}) 📱\]', existing_notes)
            if _ts_match:
                st.caption(f"📱 Captured: {_ts_match.group(1)}")
        else:
            st.caption("No messages captured yet")

        # --- ACTION BUTTONS: Fetch or Paste ---
        _btn1, _btn2 = st.columns(2)
        with _btn1:
            # Open Messenger to trigger Chrome extension capture
            _fb_url = driver.facebook_url
            if _fb_url:
                _fb_msg_url = _fb_url if 'messenger.com' in str(_fb_url) else f"https://www.messenger.com/t/{str(_fb_url).rstrip('/').split('/')[-1]}"
                st.markdown(f'<a href="{_fb_msg_url}" target="_blank" style="text-decoration:none;display:block;">'
                    f'<div style="background:#1877F2;color:white;padding:8px;border-radius:6px;text-align:center;font-weight:bold;">'
                    f'📱 Open Messenger</div></a>', unsafe_allow_html=True)
                st.caption("Then click '📤 Send Thread' in the extension")
            else:
                st.caption("No FB URL — add one to fetch messages")

        with _btn2:
            if st.button("🔄 Refresh Thread", key=f"refresh_thread_{effective_email}_{key_suffix}", use_container_width=True):
                # Clear AI cache so it regenerates with latest thread
                _ai_ck = f"_ai_msg_{effective_email}_{key_suffix}"
                if _ai_ck in st.session_state:
                    del st.session_state[_ai_ck]
                st.session_state['_stage_just_updated'] = True
                st.rerun(scope="app")

        # --- PASTE BOX (below thread, fallback for manual paste) ---
        with st.expander("📋 Paste thread manually", expanded=not bool(_saved_thread)):
            pasted_text = st.text_area(
                "Paste conversation",
                value=_saved_thread,
                height=200,
                key=_paste_ss_key,
                placeholder="Paste your Messenger/IG conversation here..."
            )

            if st.button("💾 Save Thread", key=f"save_thread_{effective_email}_{key_suffix}", use_container_width=True):
                if pasted_text.strip():
                    timestamp = datetime.now().strftime("%d %b %H:%M")
                    thread_block = f"[{timestamp} 📱] [THREAD]\n{pasted_text.strip()}\n[/THREAD]"

                    # Remove old thread blocks
                    cleaned_notes = _re.sub(r'\[\d{2} \w{3} \d{2}:\d{2} 📱\] \[THREAD\].*?\[/THREAD\]\n?', '', existing_notes, flags=_re.DOTALL).strip()
                    cleaned_notes = _re.sub(r'\[\d{2} \w{3} \d{2}:\d{2} 📱 (?:FB|IG)\] Captured thread:.*?(?=\[|\Z)', '', cleaned_notes, flags=_re.DOTALL).strip()

                    updated_notes = f"{thread_block}\n{cleaned_notes}" if cleaned_notes else thread_block
                    driver.notes = updated_notes
                    dashboard.add_new_driver(
                        effective_email, driver.first_name, driver.last_name,
                        driver.facebook_url or "", ig_url=driver.instagram_url or "",
                        championship=driver.championship or "", notes=updated_notes
                    )
                    _ai_ck = f"_ai_msg_{effective_email}_{key_suffix}"
                    if _ai_ck in st.session_state:
                        del st.session_state[_ai_ck]
                    st.toast("💾 Thread saved!")
                    st.session_state['_stage_just_updated'] = True
                    st.rerun(scope="app")

        # --- LOG HISTORY & TEMPLATES (collapsed at bottom) ---
        _other_notes = _re.sub(r'\[\d{2} \w{3} \d{2}:\d{2} 📱\] \[THREAD\].*?\[/THREAD\]\n?', '', existing_notes, flags=_re.DOTALL).strip()
        _other_notes = _re.sub(r'\[\d{2} \w{3} \d{2}:\d{2} 📱 (?:FB|IG)\] Captured thread:.*?(?=\[|\Z)', '', _other_notes, flags=_re.DOTALL).strip()
        if _other_notes:
            with st.expander("📋 Log History", expanded=False):
                st.text(_other_notes)

        with st.expander("📝 Manual Template", expanded=False):
            template_options = ["(Draft / Custom)"] + list(REPLY_TEMPLATES.keys())
            if default_event_name:
                template_options.insert(1, "✨ Auto-Generate (Race Context)")

            tmpl_key = st.selectbox(
                "Choose Template",
                options=template_options,
                key=f"uni_tpl_{effective_email}_{key_suffix}"
            )

            draft_msg = ""
            if tmpl_key == "✨ Auto-Generate (Race Context)" and default_event_name:
                mock_raw = {'original_name': driver.full_name, 'match_status': 'match_found', 'match': driver}
                draft_msg = dashboard.generate_outreach_message(mock_raw, default_event_name)
            elif tmpl_key in REPLY_TEMPLATES:
                raw_msg = REPLY_TEMPLATES[tmpl_key]
                fn = driver.first_name or (driver.full_name.split(' ')[0] if driver.full_name else "Mate")
                draft_msg = raw_msg.replace("{name}", fn)

            msg_key = f"uni_msg_{effective_email}_{key_suffix}"
            prev_tpl_key = f"prev_tpl_{effective_email}_{key_suffix}"
            if prev_tpl_key not in st.session_state:
                st.session_state[prev_tpl_key] = "(Draft / Custom)"
            if tmpl_key != st.session_state[prev_tpl_key]:
                if tmpl_key != "(Draft / Custom)":
                    st.session_state[msg_key] = draft_msg
                st.session_state[prev_tpl_key] = tmpl_key

            final_msg = st.text_area("Message", key=msg_key, height=200)
            st.code(final_msg, language=None)

    # ═══════════════════════════════════════════════════════════════
    # RIGHT COLUMN: Contact Info + Aligned Action Buttons
    # ═══════════════════════════════════════════════════════════════
    with uc2:
        # === RIDER NAME (prominent at top) ===
        display_name = driver.full_name or f"{driver.first_name or ''} {driver.last_name or ''}".strip() or "Unknown Driver"
        st.markdown(f"## {display_name}")
        # Show "otherwise known as" when preferred name differs from first name
        _pref = getattr(driver, 'preferred_name', None)
        _formal = (driver.first_name or '').strip()
        if _pref and _pref.lower().strip() != _formal.lower():
            st.caption(f"*(otherwise known as {_pref})*")

        # Helper to get LIVE values
        fb_key = f"uni_fb_{effective_email}_{key_suffix}"
        ig_key = f"uni_ig_{effective_email}_{key_suffix}"
        live_fb = st.session_state.get(fb_key, driver.facebook_url)
        live_ig = st.session_state.get(ig_key, driver.instagram_url)

        # Compact info block
        curr_stage_display = driver.current_stage.value if hasattr(driver.current_stage, 'value') else str(driver.current_stage)

        # Status badges row
        badges = f"**{curr_stage_display}**"
        fu_sent, _ = _has_recent_follow_up_sent(driver)
        if fu_sent:
            badges += " ✅"
        if driver.current_stage in [FunnelStage.STRATEGY_CALL_BOOKED]:
            badges += " 🔥"
        if driver.championship:
            badges += f" · {driver.championship}"
        st.markdown(badges)

        # Email display (hide internal slugs) — links to GoHighLevel CRM
        _ghl_url = "https://app.usegoplus.com/v2/location/C03hMrgoj4FLALDMqpWr/contacts/smart_list/All"
        if driver.email and not driver.email.startswith("no_email_"):
            st.markdown(f'✉️ <a href="{_ghl_url}" target="_blank" style="color:inherit;text-decoration:underline dotted;">{driver.email}</a>', unsafe_allow_html=True)
        if driver.phone:
            st.caption(f"📱 {driver.phone}")
        
        # Socials — BIG BUTTONS so you can immediately see if URLs are saved
        def _make_url(val, platform):
            if not val: return None
            s_val = str(val).strip()
            if s_val.lower().startswith("http"): return s_val
            if platform == "fb": return f"https://www.facebook.com/{s_val}"
            if platform == "ig": return f"https://www.instagram.com/{s_val}"
            return s_val

        fb_col, ig_col = st.columns(2)
        with fb_col:
            st.markdown("**Facebook**")
            if live_fb:
                fb_url = _make_url(live_fb, 'fb')
                # Pass driver name to Chrome extension via URL hash
                from urllib.parse import quote
                _driver_hash = f"#ag_driver={quote(driver.full_name)}"
                st.markdown(f'<a href="{fb_url}{_driver_hash}" target="_blank" style="text-decoration:none;display:block;">'
                    f'<div style="background:#1877F2;color:white;padding:10px 8px;border-radius:6px;text-align:center;font-weight:bold;">'
                    f'👤 Open Profile</div></a>', unsafe_allow_html=True)
            else:
                _fb_input = st.text_input("Paste FB URL", value="", key=f"_fb_url_in_{effective_email}_{key_suffix}",
                    placeholder="facebook.com/...", label_visibility="collapsed")
                if st.button("💾 Save FB", key=f"_save_fb_{effective_email}_{key_suffix}", use_container_width=True):
                    if _fb_input.strip():
                        driver.facebook_url = _fb_input.strip()
                        dashboard.add_new_driver(
                            effective_email, driver.first_name, driver.last_name,
                            driver.facebook_url, ig_url=driver.instagram_url or "",
                            championship=driver.championship or "", notes=driver.notes or ""
                        )
                        st.session_state['_stage_just_updated'] = True
                        st.toast("✅ Facebook URL saved!")
                        st.rerun(scope="app")

        with ig_col:
            st.markdown("**Instagram**")
            if live_ig:
                ig_url = _make_url(live_ig, 'ig')
                from urllib.parse import quote
                _driver_hash = f"#ag_driver={quote(driver.full_name)}"
                st.markdown(f'<a href="{ig_url}{_driver_hash}" target="_blank" style="text-decoration:none;display:block;">'
                    f'<div style="background:#E1306C;color:white;padding:10px 8px;border-radius:6px;text-align:center;font-weight:bold;">'
                    f'📸 Open Profile</div></a>', unsafe_allow_html=True)
            else:
                _ig_input = st.text_input("Paste IG URL", value="", key=f"_ig_url_in_{effective_email}_{key_suffix}",
                    placeholder="instagram.com/...", label_visibility="collapsed")
                if st.button("💾 Save IG", key=f"_save_ig_{effective_email}_{key_suffix}", use_container_width=True):
                    if _ig_input.strip():
                        driver.instagram_url = _ig_input.strip()
                        dashboard.add_new_driver(
                            effective_email, driver.first_name, driver.last_name,
                            driver.facebook_url or "", ig_url=driver.instagram_url,
                            championship=driver.championship or "", notes=driver.notes or ""
                        )
                        st.session_state['_stage_just_updated'] = True
                        st.toast("✅ Instagram URL saved!")
                        st.rerun(scope="app")

        # Search links when URLs are missing — prominent buttons + full search params
        if not live_fb or not live_ig:
            # Cache search links in session state to avoid re-computing
            _search_cache_key = f"_search_{effective_email}_{key_suffix}"
            _cached_links = st.session_state.get(_search_cache_key)
            if _cached_links and _cached_links.get('name') == driver.full_name:
                deep_links = _cached_links['links']
            else:
                if hasattr(dashboard, 'race_manager') and hasattr(dashboard.race_manager, 'social_finder'):
                    finder = dashboard.race_manager.social_finder
                else:
                    from funnel_manager import SocialFinder
                    finder = SocialFinder()
                deep_links = finder.generate_deep_search_links(driver.full_name, default_event_name)
                st.session_state[_search_cache_key] = {'name': driver.full_name, 'links': deep_links}

            st.caption("🔎 Find Missing Socials")
            s_col1, s_col2 = st.columns(2)
            with s_col1:
                if not live_fb:
                    fb_search = deep_links.get('👥 Facebook Direct', '#')
                    st.markdown(f'<a href="{fb_search}" target="_blank" style="text-decoration:none;display:block;">'
                        f'<div style="background:#1877F2;color:white;padding:8px 6px;border-radius:6px;text-align:center;font-weight:bold;">'
                        f'🔎 Search Facebook</div></a>', unsafe_allow_html=True)
            with s_col2:
                if not live_ig:
                    ig_search = deep_links.get('📸 Instagram Direct', '#')
                    st.markdown(f'<a href="{ig_search}" target="_blank" style="text-decoration:none;display:block;">'
                        f'<div style="background:#C13584;color:white;padding:8px 6px;border-radius:6px;text-align:center;font-weight:bold;">'
                        f'🔎 Search Instagram</div></a>', unsafe_allow_html=True)

            # Extra search links — Google dorks for racing context
            with st.expander("🔍 More Search Options", expanded=False):
                core_link = deep_links.get('🔍 Core Discovery', '#')
                st.markdown(f"[🔍 Google Racing Search]({core_link})")
                ig_backup = deep_links.get('(Backup) IG Google', '#')
                st.markdown(f"[📸 Google → Instagram]({ig_backup})")
                if '🏁 Event check' in deep_links:
                    st.markdown(f"[🏁 Event Results]({deep_links['🏁 Event check']})")
                st.markdown(f"[📋 Racing Org Check]({deep_links.get('📋 Racing Org Check', '#')})")
                st.markdown(f"[⏱️ Lap Times / MyLaps]({deep_links.get('⏱️ Lap Times', '#')})")
            
        # SOURCE / LEAD MAGNETS
        st.divider()
        st.caption("🔍 Source / Lead Magnets")
        driver_tags = getattr(driver, 'tags', None)
        if driver_tags:
            st.markdown(f"**Tags:** `{driver_tags}`")
        
        # Lead Magnet Results
        lm_lines = []
        if driver.flow_profile_result: lm_lines.append(f"**Flow Profile:** {driver.flow_profile_result}")
        if driver.sleep_score: lm_lines.append(f"**Sleep Score:** {driver.sleep_score}")
        if driver.mindset_result: lm_lines.append(f"**Mindset:** {driver.mindset_result}")
        if driver.day1_score: lm_lines.append(f"**Day 1 Score:** {driver.day1_score}")
        
        if lm_lines:
            for l in lm_lines: st.markdown(l)
        elif not driver_tags:
            st.caption("No lead magnet data found.")
            
        # STAGE ACTIONS (Link Sent)
        # Replaced with comprehensive "Next Step" buttons below
            
        st.divider()

        # --- ALL BUTTONS ALIGNED TOGETHER ---
        st.markdown("#### ⏩ Actions")

        # Helper: Save form data from session state before stage change
        def _save_form_data_if_changed():
            """Read current form values from session state and save if they've changed"""
            fb_key = f"uni_fb_{effective_email}_{key_suffix}"
            ig_key = f"uni_ig_{effective_email}_{key_suffix}"
            first_key = f"uni_first_{effective_email}_{key_suffix}"
            last_key = f"uni_last_{effective_email}_{key_suffix}"
            champ_key = f"uni_champ_{effective_email}_{key_suffix}"
            notes_key = f"uni_notes_{effective_email}_{key_suffix}"

            # Get values from session state (form inputs)
            new_fb = st.session_state.get(fb_key, driver.facebook_url or "")
            new_ig = st.session_state.get(ig_key, driver.instagram_url or "")
            new_first = st.session_state.get(first_key, driver.first_name or "")
            new_last = st.session_state.get(last_key, driver.last_name or "")
            new_champ = st.session_state.get(champ_key, driver.championship or "")
            new_notes = st.session_state.get(notes_key, driver.notes or "")

            # Check if anything changed
            changed = (
                new_fb != (driver.facebook_url or "") or
                new_ig != (driver.instagram_url or "") or
                new_first != (driver.first_name or "") or
                new_last != (driver.last_name or "") or
                new_champ != (driver.championship or "") or
                new_notes != (driver.notes or "")
            )

            if changed:
                # Update driver in memory and Airtable
                dashboard.add_new_driver(
                    effective_email, new_first, new_last, new_fb,
                    ig_url=new_ig, championship=new_champ, notes=new_notes
                )
                return True
            return False

        # Show outreach activity from Chrome extension (logged in notes)
        _notes_text = driver.notes or ""
        import re as _re_outreach
        _outreach_entries = _re_outreach.findall(r'\[.*?\] 📤 (\w+ outreach sent \(.*?\))', _notes_text)
        if _outreach_entries:
            _outreach_html = " &nbsp;|&nbsp; ".join([f"✅ {e}" for e in _outreach_entries])
            st.markdown(f'<div style="background:#1a3a2a;border:1px solid #10b981;border-radius:8px;padding:8px 12px;'
                f'margin-bottom:8px;color:#34d399;font-size:13px;font-weight:600;">{_outreach_html}</div>',
                unsafe_allow_html=True)

        # Show race results history (from Speedhive imports)
        from funnel_manager import parse_race_results, get_results_summary
        _race_results = parse_race_results(_notes_text)
        if _race_results:
            _summary = get_results_summary(_notes_text, race_results_json=getattr(driver, 'race_results_json', None))
            _trend_icon = {"improving": "📈", "declining": "📉", "stable": "➡️", "new": "🆕"}.get(_summary.get("trend", ""), "")

            # Summary line
            _sum_parts = []
            if _summary.get("best_pos"):
                _sum_parts.append(f"Best: P{_summary['best_pos']} at {_summary.get('best_circuit', '?')}")
            if _summary.get("best_lap"):
                _sum_parts.append(f"Fastest: {_summary['best_lap']}")
            _sum_parts.append(f"{_trend_icon} {_summary.get('trend', '').title()}")

            st.markdown(f'<div style="background:#1a2a3a;border:1px solid #3b82f6;border-radius:8px;padding:8px 12px;'
                f'margin-bottom:8px;color:#60a5fa;font-size:13px;font-weight:600;">'
                f'📊 {" &nbsp;|&nbsp; ".join(_sum_parts)} &nbsp;({len(_race_results)} results)</div>',
                unsafe_allow_html=True)

            with st.expander(f"📊 Race Results ({len(_race_results)})", expanded=False):
                # Build results table
                _table_html = '<table style="width:100%;border-collapse:collapse;font-size:12px;color:#e5e7eb;">'
                _table_html += '<tr style="border-bottom:1px solid #374151;color:#9ca3af;">'
                _table_html += '<th style="text-align:left;padding:4px;">Date</th>'
                _table_html += '<th style="text-align:left;padding:4px;">Circuit</th>'
                _table_html += '<th style="text-align:left;padding:4px;">Session</th>'
                _table_html += '<th style="text-align:center;padding:4px;">Pos</th>'
                _table_html += '<th style="text-align:right;padding:4px;">Best Lap</th>'
                _table_html += '<th style="text-align:center;padding:4px;">Status</th>'
                _table_html += '</tr>'

                _prev_pos = None
                for _rr in _race_results[:20]:  # Show last 20
                    _pos = _rr.get("pos", "?")
                    _arrow = ""
                    if _prev_pos and isinstance(_pos, int) and isinstance(_prev_pos, int):
                        if _pos < _prev_pos:
                            _arrow = ' <span style="color:#10b981;">↑</span>'
                        elif _pos > _prev_pos:
                            _arrow = ' <span style="color:#ef4444;">↓</span>'
                    _prev_pos = _pos

                    _status_icon = {"Normal": "✅", "DNF": "🔴", "DNS": "⚫", "DQ": "🟡"}.get(
                        _rr.get("status", ""), "")
                    _session_short = _rr.get("session", "")
                    if len(_session_short) > 30:
                        _session_short = _session_short[:28] + "…"

                    _table_html += f'<tr style="border-bottom:1px solid #1f2937;">'
                    _table_html += f'<td style="padding:4px;">{_rr.get("date", "")}</td>'
                    _table_html += f'<td style="padding:4px;">{_rr.get("circuit", "")}</td>'
                    _table_html += f'<td style="padding:4px;">{_session_short}</td>'
                    _table_html += f'<td style="text-align:center;padding:4px;font-weight:bold;">P{_pos}{_arrow}</td>'
                    _table_html += f'<td style="text-align:right;padding:4px;">{_rr.get("best_lap", "")}</td>'
                    _table_html += f'<td style="text-align:center;padding:4px;">{_status_icon}</td>'
                    _table_html += '</tr>'

                _table_html += '</table>'
                st.markdown(_table_html, unsafe_allow_html=True)

        def _update_stage_fast(stage, label="Updated"):
            """Update stage + close dialog + show confirmation"""
            _save_form_data_if_changed()
            dashboard.update_driver_stage(effective_email, stage)
            st.session_state['_stage_just_updated'] = True
            # Store toast for AFTER rerun (st.toast gets lost during rerun)
            driver_name = driver.first_name or driver.full_name or "Driver"
            st.session_state['_stage_toast'] = f"✅ {driver_name} → {stage.value}"
            # Close the dialog so user returns to pipeline/calendar
            if '_open_driver_card' in st.session_state:
                del st.session_state['_open_driver_card']
            if 'calendar_selected_driver' in st.session_state:
                del st.session_state['calendar_selected_driver']
                # Prevent stale eventClick in streamlit_calendar from
                # immediately reopening the dialog for the same driver
                st.session_state['_cal_dismissed'] = True
            st.rerun(scope="app")

        # Pipeline stage buttons — Row 1: Early funnel
        c1, c2, c3 = st.columns(3)
        if c1.button("🚀 Messaged", key=f"q_msg_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.MESSAGED, "Marked as Messaged!")
        if c2.button("↩️ Replied", key=f"q_rep_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.REPLIED, "Marked as Replied!")
        if c3.button("🔗 Link Sent", key=f"q_lnk_btn_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.LINK_SENT, "Link Sent!")

        # Row 2: Mid funnel — Review & Training
        r1, r2, r3 = st.columns(3)
        if r1.button("📊 Review Done", key=f"q_rrc_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.RACE_REVIEW_COMPLETE, "Review Complete!")
        if r2.button("📚 Registered", key=f"q_reg_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.BLUEPRINT_STARTED, "Registered!")
        if r3.button("✅ Day 1", key=f"q_d1_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.DAY1_COMPLETE, "Day 1 Complete!")

        # Row 3: Late funnel
        t1, t2, t3, t4 = st.columns(4)
        if t1.button("✅ Day 2", key=f"q_d2_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.DAY2_COMPLETE, "Day 2 Complete!")
        if t2.button("📞 Call Booked", key=f"q_call_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.STRATEGY_CALL_BOOKED, "Call Booked!")
        if t3.button("📵 No Show", key=f"q_noshow_{effective_email}_{key_suffix}", use_container_width=True):
            # Log the no-show in notes
            timestamp = datetime.now().strftime("%d %b %H:%M")
            noshow_note = f"[{timestamp}] 📵 Strategy call no-show"
            existing_notes = driver.notes or ""
            driver.notes = f"{noshow_note}\n{existing_notes}" if existing_notes.strip() else noshow_note
            _update_stage_fast(FunnelStage.STRATEGY_CALL_NO_SHOW, "Marked as No Show")
        if t4.button("🏆 Client Won", key=f"q_client_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.CLIENT, "Client Won!")

        # Disqualify row
        d1, d2, d3 = st.columns(3)
        if d1.button("🚫 No Socials", key=f"dq_ns_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.NO_SOCIALS, "Marked No Socials")
        if d2.button("🔇 No Reply", key=f"dq_dnr_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.DOES_NOT_REPLY, "Marked Does Not Reply")
        if d3.button("❌ Not A Fit", key=f"dq_naf_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.NOT_A_FIT, "Marked Not A Fit")

        # Follow-up timer row
        st.caption("📅 Set Follow-Up Timer")
        now = datetime.now()
        f1, f2, f3 = st.columns(3)
        if f1.button("+3d", key=f"fu_3d_{effective_email}_{key_suffix}", use_container_width=True):
            dashboard.data_loader.save_driver_details(effective_email, follow_up_date=now + timedelta(days=3))
            st.session_state['_stage_just_updated'] = True
            st.toast(f"Follow-up: {(now + timedelta(days=3)).strftime('%a %d %b')}"); st.rerun(scope="app")
        if f2.button("+1wk", key=f"fu_1w_{effective_email}_{key_suffix}", use_container_width=True):
            dashboard.data_loader.save_driver_details(effective_email, follow_up_date=now + timedelta(weeks=1))
            st.session_state['_stage_just_updated'] = True
            st.toast(f"Follow-up: {(now + timedelta(weeks=1)).strftime('%a %d %b')}"); st.rerun(scope="app")
        if f3.button("+1mo", key=f"fu_1m_{effective_email}_{key_suffix}", use_container_width=True):
            dashboard.data_loader.save_driver_details(effective_email, follow_up_date=now + timedelta(days=30))
            st.session_state['_stage_just_updated'] = True
            st.toast(f"Follow-up: {(now + timedelta(days=30)).strftime('%a %d %b')}"); st.rerun(scope="app")

        # Delete (collapsed)
        with st.expander("🗑️ Delete", expanded=False):
            if st.button("Yes, Delete Permanently", key=f"del_yes_{effective_email}_{key_suffix}", type="primary", use_container_width=True):
                dashboard.delete_driver(effective_email)
                # Clear dialog keys BEFORE rerun — prevents hang from trying
                # to re-open the deleted driver's card on the next render
                st.session_state.pop('_open_driver_card', None)
                st.session_state.pop('calendar_selected_driver', None)
                # Don't clear cache_resource — it triggers a full 30s+ reload.
                # The driver is already removed from memory by delete_driver().
                # Next natural cache expiry (60s TTL) picks up the AT deletion.
                st.session_state['_stage_just_updated'] = True
                st.toast(f"Deleted {driver.first_name}"); st.rerun(scope="app")

        st.divider()

        # Update Details Form (collapsed by default to save space)
        with st.expander("✏️ Update Details", expanded=False):
            with st.form(key=f"uni_upd_{driver.email}_{key_suffix}"):
                c_name1, c_name2 = st.columns(2)
                with c_name1:
                    u_first = st.text_input("First Name", value=driver.first_name, key=f"uni_first_{driver.email}_{key_suffix}")
                with c_name2:
                    u_last = st.text_input("Last Name", value=driver.last_name, key=f"uni_last_{driver.email}_{key_suffix}")
                
                # UX FIX: Hide "no_email_" slugs from the user. Treat them as empty.
                display_email = driver.email
                if display_email.startswith("no_email_"):
                    display_email = ""
                
                u_email = st.text_input("Email (Optional)", value=display_email, key=f"uni_email_{driver.email}_{key_suffix}", placeholder="e.g. driver@example.com")
                
                u_fb = st.text_input("Facebook URL", value=driver.facebook_url or "", key=f"uni_fb_{driver.email}_{key_suffix}")
                u_ig = st.text_input("Instagram URL", value=driver.instagram_url or "", key=f"uni_ig_{driver.email}_{key_suffix}")
                
                # Auto-Populate Championship from Global Logic
                curr_champ = driver.championship or ""
                global_champ = st.session_state.get('global_championship', '')
                
                final_champ_val = curr_champ
                if global_champ and global_champ.lower() not in curr_champ.lower():
                    if final_champ_val:
                        final_champ_val = f"{curr_champ}, {global_champ}"
                    else:
                        final_champ_val = global_champ
                
                u_champ = st.text_input("Championship", value=final_champ_val, key=f"uni_champ_{driver.email}_{key_suffix}")
                
                # STAGE DROPDOWN (Allow Manual Move)
                stage_options = [s.value for s in FunnelStage]
                
                # Robustly get current stage string
                curr_stage_val = driver.current_stage.value if hasattr(driver.current_stage, 'value') else str(driver.current_stage)
                
                # Match to options or default
                try:
                    default_idx = stage_options.index(curr_stage_val)
                except ValueError:
                    default_idx = 0 # Default to first if unknown
                
                u_stage = st.selectbox(
                    "Current Stage", 
                    options=stage_options, 
                    index=default_idx,
                    key=f"uni_stage_{driver.email}_{key_suffix}"
                )

                 # Follow Up Date
                default_date = driver.follow_up_date.date() if driver.follow_up_date else None
                u_follow = st.date_input("📅 Next Follow-Up", value=default_date, key=f"uni_fu_{driver.email}_{key_suffix}")
                
                # Notes (conversation log is on the LEFT, this is for the save form)
                _form_notes = driver.notes or ""
                u_notes_from_left = st.session_state.get(f"uni_notes_{driver.email}_{key_suffix}", _form_notes)
                u_notes = st.text_area("Notes", value=u_notes_from_left, height=80,
                                      key=f"uni_notes_{driver.email}_{key_suffix}",
                                      label_visibility="collapsed")
                
                # --- EXPLICIT MOVE TO AIRTABLE BUTTON (for visuals) ---
                # Although Update does it, user likes the explicit button?
                # The form submit handles Everything.
                # Let's add a note or separate button OUTSIDE form if needed.
                # "Save Updates" implies sync.
                
                if st.form_submit_button("💾 Save Updates (Sync & Migrate)"):
                    ts_follow = datetime.combine(u_follow, datetime.min.time()) if u_follow else None
                    
                    # LOGIC: If user left email blank, keep the old ID (even if it was a slug)
                    # If user entered a NEW email, use that (this will technically create a new migrated entry)
                    final_email = u_email.strip()
                    if not final_email:
                        final_email = driver.email
                    
                    # 1. Update Stage if changed
                    # Get current stage string safely
                    curr_stage_val = driver.current_stage.value if hasattr(driver.current_stage, 'value') else str(driver.current_stage)
                    
                    if u_stage != curr_stage_val:
                        new_enum = next((s for s in FunnelStage if s.value == u_stage), None)
                        if new_enum:
                            dashboard.update_driver_stage(driver.email, new_enum)
                            st.toast(f"Moved to {u_stage}!")

                    # 2. Update Details
                    dashboard.add_new_driver(
                        final_email, u_first, u_last, u_fb, ig_url=u_ig, championship=u_champ, notes=u_notes, follow_up_date=ts_follow
                    )
                    st.toast(f"Updated & Synced {u_first}!")
                    st.rerun(scope="app")  # Full rerun, name/details change affects pipeline
