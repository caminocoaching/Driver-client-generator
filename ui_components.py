import streamlit as st
import urllib.parse
import re as _re
from datetime import datetime, timedelta
from funnel_manager import FunnelStage

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
    """Extract the last message from the driver (not 'You') in a captured thread."""
    if not notes_or_thread:
        return ""
    lines = notes_or_thread.strip().split('\n')
    for line in reversed(lines):
        line = line.strip()
        # Match "Them: ..." or "  Driver: ..." patterns from captured threads
        if line.startswith('Them:') or line.startswith('  Them:'):
            return line.split(':', 1)[1].strip()
        # Also match named driver lines like "  John: ..."
        if ':' in line and not line.startswith('You:') and not line.startswith('  You:'):
            parts = line.split(':', 1)
            if len(parts) == 2 and len(parts[0].strip()) < 30:
                return parts[1].strip()
    return ""


def _has_recent_follow_up_sent(driver, within_hours=48):
    """Check if a follow-up was marked as sent recently (via notes marker)."""
    notes = driver.notes or ""
    # Look for ✅ FU_SENT markers in notes
    matches = _re.findall(r'\[(\d{2} \w{3} \d{2}:\d{2}) ✅\]', notes)
    if not matches:
        return False, None
    # Parse the most recent one
    try:
        latest = datetime.strptime(matches[0], "%d %b %H:%M")
        # Add current year since format doesn't include year
        latest = latest.replace(year=datetime.now().year)
        hours_ago = (datetime.now() - latest).total_seconds() / 3600
        return hours_ago <= within_hours, latest
    except (ValueError, IndexError):
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
        trend_map = {'improving': '📈 improving', 'declining': '📉', 'stable': '➡️ consistent', 'new': ''}
        if trend_map.get(trend):
            parts.append(trend_map[trend])
        if parts:
            return " · ".join(parts), detail
    return None, detail


def _perf_opener(detail, first_name, event_name=""):
    """Generate a performance-driven opening line for outreach messages.

    Args:
        detail: dict from _build_perf_line()
        first_name: driver's first name
        event_name: circuit/event name (optional)

    Returns: personalised opening string, or generic opener if no data
    """
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
    season_best = detail.get('season_best')

    # Best result determines tone
    if race_pos:
        if race_pos <= 3:
            return f"Hey {first_name}, P{race_pos} at {circuit} - that's a mega result! How did it feel up there?"
        elif season_best and race_pos <= season_best:
            return f"Hey {first_name}, P{race_pos} at {circuit} - best finish of the season so far! What changed?"
        elif trend == 'improving':
            return f"Hey {first_name}, I see you grabbed P{race_pos} at {circuit} and the trend's heading the right way - how's it feeling out there?"
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


def generate_ai_message(driver, conversation_thread="", performance_data=None, event_name=""):
    """Generate a contextually appropriate follow-up message based on the
    driver's funnel stage, conversation thread, performance data, and Blueprint knowledge.

    Args:
        driver: Driver object
        conversation_thread: captured thread text (optional)
        performance_data: dict with 'saved' (get_results_summary result) and/or
                         'live' (list of Speedhive session dicts for this driver)
        event_name: circuit/event name for context

    Returns (message_text, message_type, explanation)
    """
    stage = driver.current_stage
    # Use display_name (preferred social media name) for messages, not formal first_name
    first_name = getattr(driver, 'display_name', None) or driver.first_name or (driver.full_name.split()[0] if driver.full_name else "mate")
    track = driver.championship or ""
    last_msg = _extract_last_their_message(conversation_thread)
    sentiment = _classify_sentiment(last_msg)

    # Extract performance context
    perf_saved = (performance_data or {}).get('saved', {})
    perf_live = (performance_data or {}).get('live', [])
    perf_line, perf_detail = _build_perf_line(perf_saved, perf_live)

    # ── CONTACT: First outreach — performance-driven opener ──
    if stage in [FunnelStage.CONTACT, None]:
        opener = _perf_opener(perf_detail, first_name, event_name)
        return opener, "Cold outreach (performance)", f"First contact. Perf: {perf_line or 'none'}"

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
        msg = f"""Hey {first_name}, I see you started the Race Weekend Review but didn't get to the results page - that's where the good stuff is.

Your results break down exactly where you're losing time and why - most drivers tell me they had no idea THAT was the thing holding them back.

Plus the results page unlocks access to free training that covers how to fix those exact gaps before the next round.

Takes about 3 minutes to finish - want me to resend the link?"""
        return msg, "Review stalled nudge", "Review link sent but not completed. FOMO for results + free training."

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
    "Follow-Up (Link Sent Check)": """Hi {name} did you manage to take a look at the race weekend review I sent over?""",

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
https://academy.caminocoaching.co.uk/driver-podium-blueprint/order/

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

Here's your link: https://academy.caminocoaching.co.uk/driver-podium-blueprint/order/""",

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

        st.markdown("#### 💬 Message Thread")

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

        # --- PASTE BOX (fallback for manual paste) ---
        with st.expander("📋 Paste thread manually", expanded=not bool(_saved_thread)):
            pasted_text = st.text_area(
                "Paste conversation",
                value=_saved_thread,
                height=200,
                key=f"_thread_input_{effective_email}_{key_suffix}",
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

        # Use saved or pasted thread for AI
        conversation_thread = _saved_thread or pasted_text.strip() if 'pasted_text' in dir() else _saved_thread

        # Show log history (non-thread notes)
        _other_notes = _re.sub(r'\[\d{2} \w{3} \d{2}:\d{2} 📱\] \[THREAD\].*?\[/THREAD\]\n?', '', existing_notes, flags=_re.DOTALL).strip()
        _other_notes = _re.sub(r'\[\d{2} \w{3} \d{2}:\d{2} 📱 (?:FB|IG)\] Captured thread:.*?(?=\[|\Z)', '', _other_notes, flags=_re.DOTALL).strip()
        if _other_notes:
            with st.expander("📋 Log History", expanded=False):
                st.text(_other_notes)

        st.divider()

        # --- AI MESSAGE GENERATOR ---
        st.markdown("#### 🤖 Suggested Follow-Up")

        # Cache AI message — only regenerate if stage/thread changes
        _ai_cache_key = f"_ai_msg_{effective_email}_{key_suffix}"
        _ai_cache = st.session_state.get(_ai_cache_key)
        _stage_val = driver.current_stage.value if hasattr(driver.current_stage, 'value') else str(driver.current_stage)
        if _ai_cache and _ai_cache.get('stage') == _stage_val and _ai_cache.get('thread_len') == len(conversation_thread):
            ai_msg, ai_type, ai_explanation = _ai_cache['msg'], _ai_cache['type'], _ai_cache['expl']
        else:
            ai_msg, ai_type, ai_explanation = generate_ai_message(driver, conversation_thread)
            st.session_state[_ai_cache_key] = {'stage': _stage_val, 'thread_len': len(conversation_thread),
                                                'msg': ai_msg, 'type': ai_type, 'expl': ai_explanation}

        st.caption(f"**{ai_type}** — {ai_explanation}")
        st.info("📩 Suggested follow-up — copy and send:")
        st.code(ai_msg, language=None)

        # Mark follow-up sent button
        if st.button("✅ Mark Follow-up Sent", key=f"fu_sent_{effective_email}_{key_suffix}", use_container_width=True, type="primary"):
            timestamp = datetime.now().strftime("%d %b %H:%M")
            fu_entry = f"[{timestamp} ✅] Follow-up sent ({ai_type})"
            updated_notes = f"{fu_entry}\n{existing_notes}" if existing_notes.strip() else fu_entry
            driver.notes = updated_notes
            dashboard.add_new_driver(
                effective_email, driver.first_name, driver.last_name,
                driver.facebook_url or "", ig_url=driver.instagram_url or "",
                championship=driver.championship or "", notes=updated_notes
            )
            st.toast(f"✅ Follow-up marked as sent for {driver.first_name}!")
            st.session_state['_stage_just_updated'] = True
            st.rerun(scope="app")  # Close dialog and return to pipeline

        st.divider()

        # --- TEMPLATE SELECTOR (manual override) ---
        with st.expander("📝 Manual Template / Custom Message", expanded=False):
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
                fn = getattr(driver, 'display_name', None) or driver.first_name or (driver.full_name.split(' ')[0] if driver.full_name else "Mate")
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
            st.caption("Copy for DM:")
            st.code(final_msg, language=None)

    # ═══════════════════════════════════════════════════════════════
    # RIGHT COLUMN: Contact Info + Aligned Action Buttons
    # ═══════════════════════════════════════════════════════════════
    with uc2:
        # === DRIVER NAME (prominent at top) ===
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

        # Email display (hide internal slugs)
        if driver.email and not driver.email.startswith("no_email_"):
            st.caption(f"✉️ {driver.email}")
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
                    f'<div style="background:#4CAF50;color:white;padding:10px 8px;border-radius:6px;text-align:center;font-weight:bold;">'
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
            _summary = get_results_summary(_notes_text)
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
        if r1.button("📊 Review Started", key=f"q_rw_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.RACE_WEEKEND, "Marked Review Started!")
        if r2.button("✅ Review Done", key=f"q_rrc_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.RACE_REVIEW_COMPLETE, "Review Complete!")
        if r3.button("📚 Blueprint Sent", key=f"q_bls_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.BLUEPRINT_LINK_SENT, "Blueprint Link Sent!")

        # Row 3: Late funnel
        t1, t2 = st.columns(2)
        if t1.button("📞 Call Booked", key=f"q_call_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.STRATEGY_CALL_BOOKED, "Call Booked!")
        if t2.button("🏆 Client Won", key=f"q_client_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.CLIENT, "Client Won!")

        # Disqualify row
        d1, d2 = st.columns(2)
        if d1.button("🚫 No Socials", key=f"dq_ns_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.NO_SOCIALS, "Marked No Socials")
        if d2.button("❌ Not A Fit", key=f"dq_naf_{effective_email}_{key_suffix}", use_container_width=True):
            _update_stage_fast(FunnelStage.NOT_A_FIT, "Marked Not A Fit")

        st.divider()
        if st.button("✕ Close Card", key=f"close_card_{effective_email}_{key_suffix}", use_container_width=True, type="primary"):
            st.session_state['_stage_just_updated'] = True
            st.rerun(scope="app")

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
                    st.rerun(scope="app")  # Full rerun — name/details change affects pipeline

        # Explicit Move Button (Outside form, for quick action)
        if st.button("✈️ Move to Airtable (Force)", key=f"uni_mv_{driver.email}_{key_suffix}", help="Force sync this driver to Airtable and delete from Google Sheets"):
             count = dashboard.migrate_driver_to_airtable(driver.email)
             if count:
                 st.success("Moved to Airtable!")
                 st.rerun(scope="app")
             else:
                 st.warning("Already synced or failed.")
