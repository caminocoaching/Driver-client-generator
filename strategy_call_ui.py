#!/usr/bin/env python3
"""
Strategy Call UI — Streamlit components for the Strategy Call system.
Renders the application form, pre-call prep, and post-call analysis.
"""

import streamlit as st
from datetime import datetime
import json
try:
    from strategy_call import (
        APPLICATION_QUESTIONS,
        CALL_FRAMEWORK,
        ANALYSIS_CRITERIA,
        generate_precall_prep,
        generate_analysis_report,
        score_call_section,
        swap_terminology,
        _detect_discipline,
        _extract_indicators,
        get_gold_standard_list,
        get_gold_transcript,
        compare_to_gold_standard,
        format_comparison_report,
        load_gold_standard,
    )
except ImportError:
    # Driver app has different strategy_call.py structure
    from strategy_call import (
        APPLICATION_QUESTIONS,
        swap_terminology,
    )
    # Provide stubs for missing functions
    CALL_FRAMEWORK = {}
    ANALYSIS_CRITERIA = {}
    def generate_precall_prep(*a, **kw): return ""
    def generate_analysis_report(*a, **kw): return ""
    def score_call_section(*a, **kw): return {}
    def _detect_discipline(data): return any(w in str(data).lower() for w in ['car', 'kart', 'f4', 'gt', 'porsche', 'formula'])
    def _extract_indicators(*a, **kw): return {}
    def get_gold_standard_list(): return []
    def get_gold_transcript(*a, **kw): return ""
    def compare_to_gold_standard(*a, **kw): return {}
    def format_comparison_report(*a, **kw): return ""
    def load_gold_standard(): return {}


# =============================================================================
# DATA PERSISTENCE — Save/load applications via Airtable notes or session
# =============================================================================
def _save_application(dashboard, data):
    """Save application data to the rider's notes in Airtable."""
    email = data.get("email", "").strip()
    if not email:
        slug = f"{data.get('first_name', '')} {data.get('last_name', '')}".lower().strip().replace(' ', '_')
        slug = "".join([c for c in slug if c.isalnum() or c == '_'])
        email = f"no_email_{slug}"

    # Format application as structured text
    app_text = "[STRATEGY_CALL_APPLICATION]\n"
    for q in APPLICATION_QUESTIONS:
        val = data.get(q["id"], "")
        app_text += f"{q['label']}: {val}\n"
    app_text += f"Submitted: {datetime.now().strftime('%d %b %Y %H:%M')}\n"
    app_text += "[/STRATEGY_CALL_APPLICATION]"

    # Find or create rider
    rider = dashboard._find_rider(email)
    if not rider:
        dashboard.add_new_rider(
            email, data.get("first_name", ""), data.get("last_name", ""),
            fb_url="", ig_url="",
            championship=data.get("championship", ""),
            notes=app_text
        )
    else:
        # Append to existing notes
        import re
        existing = rider.notes or ""
        existing = re.sub(
            r'\[STRATEGY_CALL_APPLICATION\].*?\[/STRATEGY_CALL_APPLICATION\]\n?',
            '', existing, flags=re.DOTALL
        ).strip()
        rider.notes = f"{app_text}\n{existing}" if existing else app_text
        dashboard.add_new_rider(
            rider.email, rider.first_name, rider.last_name,
            rider.facebook_url or "", ig_url=rider.instagram_url or "",
            championship=rider.championship or "", notes=rider.notes
        )

    return email


def _load_application_from_rider(rider):
    """Extract application data from a rider's notes."""
    if not rider or not rider.notes:
        return None

    import re
    match = re.search(
        r'\[STRATEGY_CALL_APPLICATION\](.*?)\[/STRATEGY_CALL_APPLICATION\]',
        rider.notes, re.DOTALL
    )
    if not match:
        return None

    data = {
        "first_name": rider.first_name,
        "last_name": rider.last_name,
        "email": rider.email,
    }
    lines = match.group(1).strip().split("\n")
    for line in lines:
        if ":" in line:
            key_text, val = line.split(":", 1)
            val = val.strip()
            # Match back to question IDs
            for q in APPLICATION_QUESTIONS:
                if q["label"].lower()[:30] in key_text.lower():
                    data[q["id"]] = val
                    break

    return data


# =============================================================================
# RENDER: Live Call Script
# =============================================================================

# =============================================================================
# CALL 1 — Linear flow: script sections + questions with answer fields
# =============================================================================
CALL1_FLOW = [
    # --- Pre-call data capture ---
    {"type": "header", "text": "📋 Pre-Call Data Capture"},
    {"type": "question", "q": "Response time", "key": "response_time"},
    {"type": "question", "q": "Response type", "key": "response_type"},
    {"type": "question", "q": "Full Name", "key": "full_name"},
    {"type": "question", "q": "IMPROVE Scores", "key": "improve_scores"},
    {"type": "question", "q": "Marks out of 10 on app", "key": "app_marks"},

    # --- Detective Questions ---
    {"type": "header", "text": "🔍 The Detective — Dig Deep"},
    {"type": "question", "q": "What are the top 2 or 3 struggles you are facing during a race weekend?", "key": "race_struggles"},
    {"type": "question", "q": "How long has ___ been holding you back?", "key": "duration_struggle"},
    {"type": "question", "q": "What's this costing you emotionally? How does it feel when you're ___", "key": "emotional_cost"},
    {"type": "question", "q": "What have you tried to fix this? What happened?", "key": "previous_attempts"},

    # --- Racer Investment ---
    {"type": "header", "text": "💰 Racer Investment Worksheet"},
    {"type": "script", "text": "You completed the Racer Investment Worksheet on Day 1 of the Free Training?"},
    {"type": "question", "q": "How much have you spent on equipment/track time trying to go faster?", "key": "racer_investment"},

    # --- Goals & Pain ---
    {"type": "header", "text": "🎯 Goals & Pain Amplification"},
    {"type": "question", "q": "What's your goal for this season?", "key": "season_goal"},
    {"type": "question", "q": "___ struggling with this? How have you managed that long?", "key": "how_managed"},
    {"type": "question", "q": "If nothing changes, where will you be next season?", "key": "next_season"},

    # --- Roadmap & 5 Pillars ---
    {"type": "header", "text": "🗺️ The Roadmap — 5 Pillars"},
    {"type": "script", "text": """Based on what you've shared, here's exactly what needs to happen. Grab a pen...

The lowest score is their No. 1 Priority: ___

You can either do that alone or you can get my help to do it if we both think it's a good fit."""},
    {"type": "question", "q": "Where do you want to go from here? (They'll ask about working together)", "key": "go_from_here"},

    # --- Programme Presentation ---
    {"type": "header", "text": "📦 The Programme — Push Pitch"},
    {"type": "script", "text": """"Happy to explain. Just remember — I won't be able to offer you anything today. I need to speak with the other riders first and really think about this. But I can walk you through what working together looks like."

[PAUSE — Let that land]

"Remember the 5 pillars from the free training? Here's what you actually get in the programme:

Pillar 1 — MINDSET: Daily training videos to rewire your approach, plus 1-on-1 coaching calls after each module throughout your season.

Pillar 2 — PREPARATION: A complete race weekend structure so you're fast from the out-lap instead of taking 20 minutes to get up to speed.

Pillar 3 — FLOW: 6 modules teaching you how to get into flow state consistently — the same mental state where your fastest laps happen effortlessly.

Pillar 4 — FEEDBACK: The In The Zone app on your phone. Works without signal. You feed in session details, it tells you exactly what to focus on next session to improve.

Pillar 5 — FUNDING: The complete sponsorship blueprint. This is how riders fund their racing without begging for money.\""""},
    {"type": "question", "q": "On a scale of 1-10, how close do you think that would come to fixing ___?", "key": "programme_fit_score"},

    # --- Summary ---
    {"type": "header", "text": "📝 Summary — Their Words Back to Them"},
    {"type": "question", "q": "You're Struggling with...", "key": "struggling_with"},
    {"type": "question", "q": "You've Tried...", "key": "theyve_tried"},
    {"type": "question", "q": "You Need... A proven process that's going to give you the way to perform consistently at your best to achieve ___", "key": "they_need"},

    # --- Tiedowns ---
    {"type": "header", "text": "🔒 The Tiedowns (5 minutes)"},
    {"type": "question", "q": "Can you commit to 20 minutes a day for the training?", "key": "commit_20min"},
    {"type": "script", "text": """Desire: "Does this sound like something you'd want to do?"

"Excellent, do you have any other questions?"

WAIT FOR THEM TO SAY "HOW MUCH IS IT" ⏳"""},
    {"type": "question", "q": "Their questions / reaction:", "key": "tiedown_reaction"},

    # --- Payment Options ---
    {"type": "header", "text": "💳 Payment Options"},
    {"type": "script", "text": """Good question. Now, I am not saying we have space right now; I am happy to discuss the payment options, but I need to be clear I have to take the other calls before we know whether there is space on the programme or not. Is that okay?

So for the full Flow Performance Programme with lifetime access, we have three options:

**Plan A** — £4,000 one-time payment by credit card or bank transfer — immediate full access.
**Plan B** — 8 months of £550 per month
**Plan C** — 16 months of £275 per month

On all the plans you have lifetime access to the platform. You're not just paying for this season, you're investing in your performance on any bike you ride in your racing career. Most riders go back through the training every off-season.

Most riders choose Plan B."""},
    {"type": "question", "q": "Which plan makes the most sense for their budget?", "key": "plan_chosen"},

    # --- Future Pacing & Book Call 2 ---
    {"type": "header", "text": "🔮 Future Pacing & Book Call 2"},
    {"type": "script", "text": """So ___ you've got a decision to make in the next 24 hours. Let me paint two pictures:

3 months from now you're going to be somewhere and you're going to be someone, the question is who will you be?

**Version 1 — No Action Taken:** Things stay the same, you decide the time's not right to get started. £4000 is too much to invest in yourself. 3 months from now: You're still ___, still frustrated, still finishing not where you want to be. Nowhere near your goal of ___. You saved 4k but what has it actually cost you?

**Or Version 2 — You Invest in Yourself:** 3 months from now: You've applied the mental frameworks, you understand why you struggled before, you've had breakthrough weekends. People are taking notice, asking what you've changed. Instead of frustration, you're driving home knowing no one on your bike with your budget on that track in those conditions could have ridden better than you did today. You're excited about your progress and you're running where you should be. AND you've secured your first 12k sponsor and you are talking to many more.

In 3 months you're going to be someone — which one do you want to be?"""},
    {"type": "question", "q": "Which version do they choose?", "key": "future_choice"},
    {"type": "script", "text": """**Book Call 2:** https://calendly.com/caminocoaching/rider-fit-call"""},
    {"type": "question", "q": "Call 2 booked for:", "key": "call2_datetime"},
    {"type": "question", "q": "Is there anything you feel you've missed that I can add to my notes?", "key": "missed_anything"},
]

# =============================================================================
# CALL 2 — Rider Fit Call (Close)
# =============================================================================
CALL2_FLOW = [
    # --- Recap ---
    {"type": "header", "text": "📋 Recap from Call 1"},
    {"type": "script", "text": "Ok so just let me recap what we covered last time..."},
    {"type": "question", "q": "You want to ___", "key": "c2_want_to"},
    {"type": "question", "q": "You're ___ with ___", "key": "c2_struggling_with"},
    {"type": "question", "q": "You have tried ___", "key": "c2_tried"},
    {"type": "script", "text": """The £4,000 investment felt manageable.
You're ready to work on the mental side rather than more equipment."""},
    {"type": "question", "q": "Still accurate?", "key": "c2_still_accurate"},

    # --- Coachability ---
    {"type": "header", "text": "🧠 Coachability Check"},
    {"type": "script", "text": """One question: How coachable are you?

We've found our process works best when riders follow the steps exactly. I only like to work with people who are coachable, open to feedback, and ready to take action quickly.

This works best for riders who are fully committed and follow the program to the T.

Is that you?"""},
    {"type": "question", "q": "How coachable are you? (Their answer)", "key": "c2_coachability"},
    {"type": "question", "q": "Genuinely ready for daily practice?", "key": "c2_daily_ready"},
    {"type": "question", "q": "Racing, home, and work schedule allows for proper implementation?", "key": "c2_schedule_ok"},
    {"type": "script", "text": "Perfect, just wanted to be absolutely sure."},

    # --- Good News / Bad News ---
    {"type": "header", "text": "🎉 The Offer — Good News / Bad News"},
    {"type": "script", "text": """"I've got some good news and some bad news for you. Which would you like first?"

(The "bad" news = limited spots or only space for action-takers. The "good" news = you want to offer them a place.)"""},
    {"type": "script", "text": """So I'm very keen to have you in the program and would like to offer you a spot. Congratulations, we're looking forward to working with you on this."""},
    {"type": "question", "q": "How are you feeling — excited, nervous, or a bit of both?", "key": "c2_feeling"},
    {"type": "script", "text": """(Reassure if nervous: "That's totally normal! Investing in yourself is a big step, but you're in good hands.")"""},

    # --- Close & Payment ---
    {"type": "header", "text": "💳 Close & Payment"},
    {"type": "script", "text": """Here's what happens next: Once we take payment you'll get instant access to the training platform, and we'll book your first kickoff call so you're not left guessing. Most riders tell me they see changes even in the first week.

"Perfect. I'll get your member area set up and our first call booked. From my notes, you preferred [payment option], so I have that ready."

"When you're ready, I'll take your card details."

"You've made an excellent choice. This is exactly what will transform your racing. Here's what happens next... You will get an email in the next 30 minutes."

**Picture 1:** Six months from now, you've mastered the mental game. You're the rider setting lap records, enjoying every session, achieving goals you didn't think possible.

**Picture 2:** Six months of the same struggles, same frustrations, still wondering if you'll ever breakthrough.

Which future do you prefer? Then let's make Picture 1 your reality."""},
    {"type": "question", "q": "Payment method confirmed:", "key": "c2_payment_method"},
    {"type": "question", "q": "Payment taken? (Y/N + amount)", "key": "c2_payment_taken"},

    # --- Objection Handling ---
    {"type": "header", "text": "⚡ Objection Handling"},
    {"type": "script", "text": """"Do you have any questions or concerns before we get you set up?"

If "I still need to think about it":
• "What has changed between our first call and now?"
• "What's the real reason you are hesitating?"
• "What would you need to feel comfortable moving forward?"
• "Would it help to get started with the Starter plan and upgrade later if things go well?"
• "Should we set a check-in date to finalise your spot?"

If it's the logistics of moving money around etc, take the £500 deposit. Explain that it's company policy!"""},
    {"type": "question", "q": "What objections came up?", "key": "c2_objections"},
    {"type": "question", "q": "How were they resolved?", "key": "c2_resolution"},
    {"type": "question", "q": "If 3rd call needed — what's the one thing to figure out?", "key": "c2_third_call_reason"},
]


def render_live_call_script(dashboard):
    """Render a live call script with the framework, questions, and answer fields."""

    st.markdown("""
    <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
                padding: 24px; border-radius: 16px; margin-bottom: 20px; border: 1px solid #e94560;">
        <h2 style="color: #e94560; margin: 0 0 8px 0;">📞 Live Call Script</h2>
        <p style="color: #a8b2d1; margin: 0; font-size: 0.95em;">
            Follow the script. Fill in answers as you go. Save your notes at the end.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # --- Select rider ---
    search_q = st.text_input("🔍 Who are you calling?", key="live_call_search",
                             placeholder="Search by name...")
    
    selected_rider = None
    candidate_data = {}
    
    if search_q and len(search_q) >= 2:
        matches = [r for r in dashboard.riders.values()
                   if search_q.lower() in r.full_name.lower() or search_q.lower() in str(r.email).lower()]
        if matches:
            names = [f"{r.full_name} ({r.championship or '—'})" for r in matches[:10]]
            choice = st.selectbox("Select rider:", names, key="live_call_rider")
            idx = names.index(choice)
            selected_rider = matches[idx]
            candidate_data = {
                "first_name": selected_rider.first_name,
                "last_name": selected_rider.last_name,
                "email": selected_rider.email,
                "championship": selected_rider.championship or "",
            }
            # Try loading app data from notes
            app_data = _load_application_from_rider(selected_rider)
            if app_data:
                candidate_data.update(app_data)
        else:
            st.warning("No matches found.")
            return
    else:
        st.info("Type a name to load the rider and start the call script.")
        return

    # =========================================================================
    # AUTO-POPULATE: Build pre-fill dict from ALL available data
    # =========================================================================
    prefill = {}

    # --- Full name ---
    prefill["full_name"] = selected_rider.full_name

    # --- IMPROVE / Day 1 score ---
    day1 = getattr(selected_rider, 'day1_score', None)
    if day1:
        prefill["improve_scores"] = f"Overall: {day1:.0f}%"
        prefill["_day1_raw"] = f"{day1:.0f}/100"

    # --- Day 2 pillar scores + weakest area ---
    day2 = getattr(selected_rider, 'day2_scores', None)
    if day2 and isinstance(day2, dict) and day2:
        pillar_labels = {
            'mindset': 'Mindset', 'preparation': 'Preparation',
            'flow': 'Flow', 'feedback': 'Feedback', 'sponsorship': 'Sponsorship'
        }
        score_parts = []
        for k, label in pillar_labels.items():
            v = day2.get(k)
            if v is not None:
                score_parts.append(f"{label}: {v:.0f}")
        if score_parts:
            existing = prefill.get("improve_scores", "")
            prefill["improve_scores"] = f"{existing}\n5 Pillars: {', '.join(score_parts)}" if existing else f"5 Pillars: {', '.join(score_parts)}"
        # Weakest pillar
        scored = {k: v for k, v in day2.items() if v is not None}
        if scored:
            weakest_key = min(scored, key=scored.get)
            weakest_label = pillar_labels.get(weakest_key, weakest_key)
            prefill["_weakest_pillar"] = f"{weakest_label} ({scored[weakest_key]:.0f}/10)"

    # --- App marks (commitment + seriousness from application) ---
    commit = candidate_data.get("commitment_level")
    serious = candidate_data.get("seriousness")
    if commit or serious:
        parts = []
        if commit: parts.append(f"Commitment: {commit}/10")
        if serious: parts.append(f"Seriousness: {serious}/10")
        prefill["app_marks"] = ", ".join(parts)

    # --- Race results / trend ---
    try:
        from funnel_manager import get_results_summary
        _notes = selected_rider.notes or ""
        _rjson = getattr(selected_rider, 'race_results_json', None)
        perf = get_results_summary(_notes, race_results_json=_rjson)
        if perf and perf.get('count', 0) > 0:
            trend = perf.get('trend', 'unknown')
            ext_trend = perf.get('extended_trend', '')
            best_pos = perf.get('best_pos')
            latest = perf.get('latest', {})

            trend_str = "📈 On the incline" if trend == 'improving' else (
                "📉 On the decline" if trend == 'declining' else (
                "➡️ Consistent" if trend == 'stable' else "—"))
            if ext_trend:
                trend_str += f" ({ext_trend.replace('_', ' ')})"
            prefill["_trend"] = trend_str

            results_parts = []
            if latest.get('pos'):
                results_parts.append(f"Latest: P{latest['pos']} at {latest.get('circuit', '?')}")
            if best_pos:
                results_parts.append(f"Season best: P{best_pos}")
                prefill["_best_result"] = f"P{best_pos}"
            if latest.get('best_lap') and latest['best_lap'] != '00.000':
                results_parts.append(f"Best lap: {latest['best_lap']}")
            if perf.get('season_narrative'):
                results_parts.append(perf['season_narrative'])
            prefill["_results"] = " | ".join(results_parts) if results_parts else ""
    except Exception:
        pass

    # --- Season goal from application ---
    if candidate_data.get("season_goal"):
        prefill["season_goal"] = candidate_data["season_goal"]

    # --- Mental barrier from application ---
    if candidate_data.get("mental_barrier"):
        prefill["race_struggles"] = candidate_data["mental_barrier"]

    # --- Call selector ---
    call_num = st.radio(
        "Call:", [1, 2], horizontal=True, key="live_call_num",
        format_func=lambda x: f"Call {x} — {'Championship Strategy Call' if x == 1 else 'Rider Fit Call (Close)'}"
    )

    flow = CALL1_FLOW if call_num == 1 else CALL2_FLOW
    is_driver = _detect_discipline(candidate_data)

    # =========================================================================
    # DATA SUMMARY BAR — compact overview at a glance
    # =========================================================================
    day1_display = f"{prefill.get('_day1_raw', '—')}"
    weakest_display = prefill.get('_weakest_pillar', '—')
    best_display = prefill.get('_best_result', '—')
    trend_display = prefill.get('_trend', '—')
    serious_display = candidate_data.get('seriousness', '—')
    if serious_display != '—':
        serious_display = f"{serious_display}/10"

    st.markdown(f"""
    <div style="background: linear-gradient(90deg, #0d1b2a 0%, #1b2838 100%);
                padding: 16px 24px; border-radius: 12px; margin-bottom: 16px;
                border: 1px solid #2a4a6b; display: flex; gap: 0;">
        <table style="width: 100%; color: #a8b2d1; border-collapse: collapse;">
            <tr style="border-bottom: 1px solid #2a4a6b;">
                <th style="padding: 8px 12px; color: #4fc3f7; text-align: center;">Day 1 Score</th>
                <th style="padding: 8px 12px; color: #ff7043; text-align: center;">⚠️ Weakest Pillar</th>
                <th style="padding: 8px 12px; color: #66bb6a; text-align: center;">Best Result</th>
                <th style="padding: 8px 12px; color: #ab47bc; text-align: center;">Trend</th>
                <th style="padding: 8px 12px; color: #ffa726; text-align: center;">Seriousness</th>
            </tr>
            <tr>
                <td style="padding: 10px 12px; text-align: center; font-size: 1.2em; font-weight: bold; color: white;">{day1_display}</td>
                <td style="padding: 10px 12px; text-align: center; font-size: 1.1em; font-weight: bold; color: #ff7043;">{weakest_display}</td>
                <td style="padding: 10px 12px; text-align: center; font-size: 1.2em; font-weight: bold; color: white;">{best_display}</td>
                <td style="padding: 10px 12px; text-align: center; font-size: 1.1em; font-weight: bold; color: white;">{trend_display}</td>
                <td style="padding: 10px 12px; text-align: center; font-size: 1.2em; font-weight: bold; color: white;">{serious_display}</td>
            </tr>
        </table>
    </div>
    """, unsafe_allow_html=True)

    # Day 2 Pillar Scores — broken out
    day2 = getattr(selected_rider, 'day2_scores', None)
    if day2 and isinstance(day2, dict) and day2:
        pillar_order = [
            ('mindset', 'Mindset'), ('preparation', 'Preparation'),
            ('flow', 'Flow'), ('feedback', 'Feedback'), ('sponsorship', 'Sponsorship')
        ]
        scored = {k: v for k, v in day2.items() if v is not None}
        weakest_key = min(scored, key=scored.get) if scored else None

        pcols = st.columns(len(pillar_order))
        for i, (key, label) in enumerate(pillar_order):
            with pcols[i]:
                val = day2.get(key)
                if val is not None:
                    is_weak = (key == weakest_key)
                    color = "#ff5252" if is_weak else "#4fc3f7"
                    flag = " ⚠️" if is_weak else ""
                    st.markdown(f"""
                    <div style="text-align: center; padding: 8px; border-radius: 8px;
                                background: {'#3e1111' if is_weak else '#112233'};
                                border: 1px solid {color};">
                        <div style="color: {color}; font-size: 0.8em;">{label}{flag}</div>
                        <div style="color: white; font-size: 1.4em; font-weight: bold;">{val:.0f}/10</div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div style="text-align: center; padding: 8px; border-radius: 8px;
                                background: #1a1a2e; border: 1px solid #333;">
                        <div style="color: #666; font-size: 0.8em;">{label}</div>
                        <div style="color: #444; font-size: 1.4em;">—</div>
                    </div>
                    """, unsafe_allow_html=True)

    # Extra context row
    ctx_cols = st.columns(3)
    with ctx_cols[0]:
        st.markdown(f"**Championship:** {selected_rider.championship or '—'}")
        if prefill.get('app_marks'):
            st.markdown(f"**App Scores:** {prefill['app_marks']}")
    with ctx_cols[1]:
        if candidate_data.get('season_goal'):
            st.markdown(f"**Season Goal:** {candidate_data['season_goal']}")
        if candidate_data.get('mental_barrier'):
            st.markdown(f"**Mental Barrier:** {candidate_data['mental_barrier']}")
    with ctx_cols[2]:
        if prefill.get('_results'):
            st.markdown(f"**Results:** {prefill['_results']}")
        stage_val = selected_rider.current_stage.value if hasattr(selected_rider.current_stage, 'value') else str(selected_rider.current_stage)
        st.markdown(f"**Stage:** {stage_val}")

    # Season notes
    _notes_text = selected_rider.notes or ""
    if _notes_text:
        import re
        _clean = re.sub(r'\[THREAD\].*?\[/THREAD\]', '', _notes_text, flags=re.DOTALL)
        _clean = re.sub(r'\[STRATEGY_CALL_APPLICATION\].*?\[/STRATEGY_CALL_APPLICATION\]', '', _clean, flags=re.DOTALL)
        _clean = re.sub(r'\[CALL_ANALYSIS.*?\[/CALL_ANALYSIS\]', '', _clean, flags=re.DOTALL)
        _clean = re.sub(r'\[STRATEGY_CALL_\d.*?\[/STRATEGY_CALL_\d\]', '', _clean, flags=re.DOTALL)
        _clean = _clean.strip()
        if _clean:
            with st.expander("📋 Season Notes"):
                st.text(_clean[:2000])

    st.markdown("---")

    # --- Session key for saving answers ---
    session_prefix = f"call_{call_num}_{selected_rider.email}_"

    # --- Call 2: Auto-load Call 1 answers for recap pre-fill ---
    if call_num == 2:
        c1_prefix = f"call_1_{selected_rider.email}_"
        # Map Call 1 answer keys → Call 2 recap keys
        c1_to_c2 = {
            'season_goal': 'c2_want_to',
            'struggling_with': 'c2_struggling_with',
            'race_struggles': 'c2_struggling_with',
            'theyve_tried': 'c2_tried',
            'previous_attempts': 'c2_tried',
        }
        for c1_key, c2_key in c1_to_c2.items():
            c2_widget = f"{session_prefix}{c2_key}"
            c1_widget = f"{c1_prefix}{c1_key}"
            if c2_widget not in st.session_state and c2_key not in prefill:
                c1_val = st.session_state.get(c1_widget, '').strip()
                if c1_val:
                    prefill[c2_key] = c1_val

    # --- Render the linear flow ---
    from strategy_call import swap_terminology as _swap

    for item in flow:
        if item["type"] == "header":
            st.markdown(f"""
            <div style="background: linear-gradient(90deg, #1e3a5f 0%, #0d1b2a 100%);
                        padding: 16px 20px; border-radius: 12px; margin: 16px 0 8px 0;
                        border-left: 4px solid #e94560;">
                <h3 style="color: #e94560; margin: 0;">{item['text']}</h3>
            </div>
            """, unsafe_allow_html=True)

        elif item["type"] == "script":
            text = item["text"]
            if is_driver:
                text = _swap(text, to_driver=True)
            st.markdown(text)

        elif item["type"] == "question":
            q_text = item["q"]
            if is_driver:
                q_text = _swap(q_text, to_driver=True)
            # Auto-populate from prefill if no answer typed yet
            widget_key = f"{session_prefix}{item['key']}"
            default_val = ""
            if widget_key not in st.session_state and item["key"] in prefill:
                default_val = prefill[item["key"]]
            st.text_area(
                q_text,
                value=default_val if widget_key not in st.session_state else st.session_state[widget_key],
                key=widget_key,
                height=60,
                placeholder="Type their answer here..."
            )

    # --- Save call notes ---
    st.markdown("---")
    st.markdown("### 💾 Save Call Notes")
    
    overall_notes = st.text_area(
        "Overall impressions / additional notes:",
        key=f"{session_prefix}overall_notes",
        height=100,
        placeholder="How did the call go? Any red flags? Key observations..."
    )
    
    outcome = st.selectbox(
        "Call outcome:",
        ["In Progress", "Call 2 Booked", "Closed — Enrolled", "Objections — Follow Up",
         "Not a Fit", "No Show"],
        key=f"{session_prefix}outcome"
    )

    if st.button("💾 Save Call Notes to Rider Record", type="primary", use_container_width=True,
                  key=f"{session_prefix}save"):
        notes_lines = []
        call_title = "Championship Strategy Call" if call_num == 1 else "Rider Fit Call"
        notes_lines.append(f"[STRATEGY_CALL_{call_num} {datetime.now().strftime('%d %b %Y %H:%M')}]")
        notes_lines.append(f"Call: {call_title}")
        notes_lines.append(f"Outcome: {outcome}")
        notes_lines.append("")

        # Collect answers from all question items in the flow
        current_header = ""
        for item in flow:
            if item["type"] == "header":
                current_header = item["text"]
            elif item["type"] == "question":
                answer = st.session_state.get(f"{session_prefix}{item['key']}", "").strip()
                if answer:
                    if current_header:
                        notes_lines.append(f"— {current_header} —")
                        current_header = ""  # Only print header once
                    notes_lines.append(f"Q: {item['q']}")
                    notes_lines.append(f"A: {answer}")
                    notes_lines.append("")

        if overall_notes.strip():
            notes_lines.append(f"Notes: {overall_notes.strip()}")
        notes_lines.append(f"[/STRATEGY_CALL_{call_num}]")

        call_block = "\n".join(notes_lines)

        # Save to rider
        existing = selected_rider.notes or ""
        updated = f"{call_block}\n\n{existing}" if existing.strip() else call_block
        selected_rider.notes = updated
        dashboard.add_new_rider(
            selected_rider.email, selected_rider.first_name, selected_rider.last_name,
            selected_rider.facebook_url or "", ig_url=selected_rider.instagram_url or "",
            championship=selected_rider.championship or "", notes=updated
        )

        # Auto-move to CLIENT stage if closed
        if outcome == "Closed — Enrolled":
            from funnel_manager import FunnelStage
            selected_rider.current_stage = FunnelStage.CLIENT
            selected_rider.sale_closed_date = datetime.now()
            dashboard.add_new_rider(
                selected_rider.email, selected_rider.first_name, selected_rider.last_name,
                selected_rider.facebook_url or "", ig_url=selected_rider.instagram_url or "",
                championship=selected_rider.championship or "", notes=updated
            )
            st.toast(f"🎉 {selected_rider.full_name} moved to CLIENT! Congratulations!")
        else:
            st.toast(f"💾 Call {call_num} notes saved for {selected_rider.full_name}!")

        st.session_state[f'{session_prefix}saved_notes'] = call_block

    if st.session_state.get(f'{session_prefix}saved_notes'):
        st.download_button(
            "📋 Download Call Notes",
            st.session_state[f'{session_prefix}saved_notes'],
            file_name=f"call{call_num}_{selected_rider.first_name}_{datetime.now().strftime('%Y%m%d')}.txt",
            mime="text/plain",
            use_container_width=True
        )


# =============================================================================
# RENDER: Pre-Call Preparation
# =============================================================================
def render_precall_prep(dashboard):
    """Render the pre-call preparation interface."""

    st.markdown("""
    <div style="background: linear-gradient(135deg, #0d1b2a 0%, #1b2838 50%, #233d4d 100%);
                padding: 24px; border-radius: 16px; margin-bottom: 20px; border: 1px solid #52b788;">
        <h2 style="color: #52b788; margin: 0 0 8px 0;">🎯 Pre-Call Preparation</h2>
        <p style="color: #a8b2d1; margin: 0; font-size: 0.95em;">
            Generates your personalised script overlay with data-driven insights for maximum conversion.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Source selection
    source = st.radio(
        "Load candidate data from:",
        ["🔍 Search Rider Database", "📝 Paste Raw Data"],
        horizontal=True, key="precall_source"
    )

    candidate_data = None

    if source == "🔍 Search Rider Database":
        search_q = st.text_input("Search by name or email:", key="precall_search")
        if search_q and len(search_q) >= 2:
            matches = []
            for rider in dashboard.riders.values():
                if search_q.lower() in rider.full_name.lower() or search_q.lower() in str(rider.email).lower():
                    matches.append(rider)

            if matches:
                names = [f"{r.full_name} ({r.championship or 'No championship'})" for r in matches[:10]]
                choice = st.selectbox("Select rider:", names, key="precall_rider_select")
                idx = names.index(choice)
                selected_rider = matches[idx]

                # Try to load existing application
                app_data = _load_application_from_rider(selected_rider)
                if app_data:
                    candidate_data = app_data
                    st.success(f"✅ Found application data for {selected_rider.full_name}")
                else:
                    candidate_data = {
                        "first_name": selected_rider.first_name,
                        "last_name": selected_rider.last_name,
                        "email": selected_rider.email,
                        "championship": selected_rider.championship or "",
                    }
                    st.warning("No application on file. Basic data loaded.")
            else:
                st.warning("No matches found.")

    elif source == "📝 Paste Raw Data":
        raw = st.text_area(
            "Paste candidate data (assessment results, application, etc.):",
            height=200, key="precall_raw"
        )
        if raw:
            candidate_data = _parse_raw_data(raw)
            if candidate_data:
                st.success(f"✅ Parsed data for: {candidate_data.get('first_name', 'Unknown')}")

    if candidate_data:
        call_num = st.radio("Call Number:", [1, 2], horizontal=True, key="precall_call_num",
                           format_func=lambda x: f"Call {x} — {'Championship Strategy Call' if x == 1 else 'Rider Fit Call'}")

        if st.button("🎯 Generate Pre-Call Script", type="primary", use_container_width=True, key="gen_precall"):
            with st.spinner("Generating personalised script overlay..."):
                prep = generate_precall_prep(candidate_data, call_number=call_num)
                st.session_state['current_precall_prep'] = prep
                st.session_state['current_candidate'] = candidate_data

        if 'current_precall_prep' in st.session_state:
            st.markdown("---")
            st.markdown(st.session_state['current_precall_prep'])

            st.download_button(
                "📋 Download Script as Text",
                st.session_state['current_precall_prep'],
                file_name=f"precall_{candidate_data.get('first_name', 'candidate')}_{datetime.now().strftime('%Y%m%d')}.md",
                mime="text/markdown",
                use_container_width=True
            )


def _parse_raw_data(raw_text):
    """Parse unstructured text to extract candidate data."""
    data = {}
    patterns = {
        "first_name": r"(?:first\s*name|name)[:\s]+([^\n,]+)",
        "last_name": r"(?:last\s*name|surname)[:\s]+([^\n,]+)",
        "email": r"[\w.-]+@[\w.-]+\.\w+",
        "phone": r"(?:phone|tel|mobile)[:\s]*([\+\d\s()-]+)",
        "age": r"(?:age)[:\s]*(\d+)",
        "championship": r"(?:championship|series|class)[:\s]+([^\n]+)",
        "season_goal": r"(?:goal|target|aim)[:\s]+([^\n]+)",
        "mental_barrier": r"(?:barrier|struggle|challenge|mental)[:\s]+([^\n]+)",
        "commitment_level": r"(?:commitment|committed|determination)[:\s]*(\d+)",
        "current_level": r"(?:current\s*level|performance\s*level)[:\s]+([^\n]+)",
    }
    import re
    for key, pattern in patterns.items():
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            data[key] = match.group(1).strip() if match.lastindex else match.group(0).strip()
    return data if data else None


# =============================================================================
# RENDER: Post-Call Analysis
# =============================================================================
def render_postcall_analysis(dashboard):
    """Render the post-call analysis interface."""

    st.markdown("""
    <div style="background: linear-gradient(135deg, #1a1a2e 0%, #2d1b69 50%, #4a1a8a 100%);
                padding: 24px; border-radius: 16px; margin-bottom: 20px; border: 1px solid #bb86fc;">
        <h2 style="color: #bb86fc; margin: 0 0 8px 0;">📊 Post-Call Analysis</h2>
        <p style="color: #a8b2d1; margin: 0; font-size: 0.95em;">
            Score your call against the Championship Strategy Call Framework.
            Identify strengths, missed opportunities, and actionable improvements.
        </p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])
    with col1:
        rider_search = st.text_input("Which rider was this call with?", key="postcall_rider")
    with col2:
        call_type = st.selectbox("Call Type:", ["Call 1 (Strategy)", "Call 2 (Fit/Close)"], key="postcall_type")

    st.markdown("---")

    st.markdown("### 📝 Score Each Category")
    st.caption("Rate each area 1-10 based on the call. Use the checkpoints as a guide.")

    scores = []
    for key, criteria in ANALYSIS_CRITERIA.items():
        with st.expander(f"**{criteria['label']}** — {criteria['description']}", expanded=True):
            st.markdown("**Checkpoints to evaluate:**")
            for cp in criteria["checkpoints"]:
                st.markdown(f"- {cp}")

            col_s, col_n = st.columns([1, 3])
            with col_s:
                score = st.slider(f"{criteria['label']} Score", 1, criteria["max_score"],
                    value=5, key=f"score_{key}", label_visibility="collapsed")
            with col_n:
                notes = st.text_area(f"Notes on {criteria['label']}", key=f"notes_{key}",
                    height=80, label_visibility="collapsed",
                    placeholder=f"What went well? What was missed? Specific examples...")
            scores.append(score_call_section(key, score, notes))

    st.markdown("---")
    st.markdown("### 💡 Strengths & Missed Opportunities")

    col1, col2 = st.columns(2)
    with col1:
        strengths = st.text_area("**Strengths**", key="postcall_strengths", height=120,
                                placeholder="e.g., Great rapport building...")
    with col2:
        missed = st.text_area("**Missed Opportunities**", key="postcall_missed", height=120,
                             placeholder="e.g., Didn't use the reverse frame...")

    actionable = st.text_area("**🎯 One Key Improvement**", key="postcall_action", height=80,
        placeholder="The single most impactful thing to improve...")

    st.markdown("---")
    st.markdown("### 📄 Call Transcript (Optional)")
    transcript = st.text_area("Paste the call transcript:", key="postcall_transcript", height=200,
        placeholder="Paste the full call transcript here...")

    if st.button("📊 Generate Analysis Report", type="primary", use_container_width=True, key="gen_analysis"):
        call_notes = ""
        if strengths: call_notes += f"### ✅ Strengths\n{strengths}\n\n"
        if missed: call_notes += f"### ❌ Missed Opportunities\n{missed}\n\n"
        if actionable: call_notes += f"### 🎯 Actionable Advice\n{actionable}\n\n"

        report = generate_analysis_report(scores, call_notes)
        st.session_state['current_analysis_report'] = report

        if rider_search:
            for rider in dashboard.riders.values():
                if rider_search.lower() in rider.full_name.lower():
                    ts = datetime.now().strftime("%d %b %Y")
                    analysis_block = f"[CALL_ANALYSIS {ts}]\n{report}\n[/CALL_ANALYSIS]\n"
                    rider.notes = f"{analysis_block}\n{rider.notes}" if rider.notes else analysis_block
                    dashboard.add_new_rider(
                        rider.email, rider.first_name, rider.last_name,
                        rider.facebook_url or "", ig_url=rider.instagram_url or "",
                        championship=rider.championship or "", notes=rider.notes
                    )
                    st.toast(f"💾 Analysis saved to {rider.full_name}'s record")
                    break

    if 'current_analysis_report' in st.session_state:
        st.markdown("---")
        st.markdown(st.session_state['current_analysis_report'])
        st.download_button("📋 Download Analysis Report",
            st.session_state['current_analysis_report'],
            file_name=f"call_analysis_{rider_search or 'unknown'}_{datetime.now().strftime('%Y%m%d')}.md",
            mime="text/markdown", use_container_width=True)

    # Gold Standard Comparison
    st.markdown("---")
    st.markdown("### 🏆 Compare Against Gold Standard")
    gold_calls = get_gold_standard_list()
    if gold_calls:
        gold_labels = [c["label"] for c in gold_calls]
        gold_choice = st.selectbox("Compare against:", gold_labels, key="gold_compare_select")
        selected_gold = gold_calls[gold_labels.index(gold_choice)]
        st.markdown(f"**Outcome:** {selected_gold['outcome']}")
        st.caption(selected_gold["notes"])

        if transcript and st.button("🏆 Run Gold Standard Comparison", key="run_gold_compare", use_container_width=True):
            comparison = compare_to_gold_standard(transcript, selected_gold["entry_key"], selected_gold["call_key"])
            if comparison:
                st.session_state['gold_comparison_report'] = format_comparison_report(comparison, gold_choice)
        if 'gold_comparison_report' in st.session_state:
            st.markdown(st.session_state['gold_comparison_report'])
    else:
        st.info("No gold standard calls available.")


# =============================================================================
# RENDER: Gold Standard Reference
# =============================================================================
def render_gold_standard(dashboard):
    """View gold standard call transcripts as reference."""

    st.markdown("""
    <div style="background: linear-gradient(135deg, #1a1a2e 0%, #3a1f04 50%, #6b3a00 100%);
                padding: 24px; border-radius: 16px; margin-bottom: 20px; border: 1px solid #ffa726;">
        <h2 style="color: #ffa726; margin: 0 0 8px 0;">🏆 Gold Standard Calls</h2>
        <p style="color: #a8b2d1; margin: 0; font-size: 0.95em;">
            Reference transcripts from successful closes.
        </p>
    </div>
    """, unsafe_allow_html=True)

    gold_data = load_gold_standard()
    if not gold_data:
        st.warning("No gold standard data found.")
        return

    for key, entry in gold_data.items():
        disc_badge = "🏎️" if entry["discipline"] == "driver" else "🏍️"
        with st.expander(f"{disc_badge} **{entry['name']}** — {entry['outcome']}", expanded=False):
            st.markdown(f"**Notes:** {entry['notes']}")
            for call_key in ["call_1", "call_2"]:
                if call_key not in entry:
                    continue
                call = entry[call_key]
                st.markdown(f"#### 📞 {call['title']} ({call['duration']})")
                transcript = call.get("transcript", "")
                if transcript:
                    st.text(transcript[:500] + "..." if len(transcript) > 500 else transcript)
                    with st.expander(f"📄 Full Transcript ({len(transcript):,} chars)"):
                        st.text(transcript)
                    st.download_button(f"📋 Download", transcript,
                        file_name=f"gold_{key}_{call_key}.txt", mime="text/plain",
                        key=f"dl_{key}_{call_key}")
                st.markdown("---")


# =============================================================================
# MAIN RENDER: Strategy Call Hub
# =============================================================================
def render_strategy_call_hub(dashboard):
    """Main entry point — renders the full Strategy Call section."""

    sub_tab = st.radio(
        "Strategy Call Section:",
        ["📞 Live Call Script", "🎯 Pre-Call Prep", "📊 Post-Call Analysis", "🏆 Gold Standard"],
        horizontal=True,
        key="strategy_sub_tab",
        label_visibility="collapsed"
    )

    if sub_tab == "📞 Live Call Script":
        render_live_call_script(dashboard)
    elif sub_tab == "🎯 Pre-Call Prep":
        render_precall_prep(dashboard)
    elif sub_tab == "📊 Post-Call Analysis":
        render_postcall_analysis(dashboard)
    elif sub_tab == "🏆 Gold Standard":
        render_gold_standard(dashboard)
