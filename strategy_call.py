"""
Strategy Call System
====================
Implements the Championship Strategy Call Framework for Flow Performance sales.
- Pre-Call Preparation (Mode 1): Analyze candidate data, generate personalized script overlay
- Post-Call Analysis (Mode 2): Score calls against gold standard, provide coaching
- Application Questions: Replace Typeform with in-app questionnaire
- Dynamic Terminology: Auto-swap rider/driver terms based on candidate type
"""

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# =============================================================================
# APPLICATION QUESTIONS (replaces Typeform)
# =============================================================================

APPLICATION_QUESTIONS = [
    {"id": "first_name", "label": "First Name", "type": "text", "required": True},
    {"id": "last_name", "label": "Last Name", "type": "text", "required": True},
    {"id": "email", "label": "Email", "type": "text", "required": True},
    {"id": "phone", "label": "Phone Number", "type": "text", "required": True},
    {"id": "age", "label": "What Is Your Age? (If Under 18, Parents Will Need To Be On The Call)", "type": "text", "required": True},
    {"id": "performance_level", "label": "What is your current level of performance?", "type": "text", "required": True},
    {"id": "championship", "label": "What Championship do you race in?", "type": "text", "required": True},
    {"id": "racing_inspiration", "label": "What initially inspired you to pursue racing at this level?", "type": "textarea", "required": True},
    {"id": "season_goal", "label": "What's your No1 racing goal for this season?", "type": "textarea", "required": True},
    {"id": "assessment_surprise", "label": "Your IMPROVE Assessment revealed specific performance gaps. Which category surprised you most with its score?", "type": "textarea", "required": True},
    {"id": "blueprint_breakthrough", "label": "After completing the Podium Contenders Blueprint, 3-day training. Which topic created your biggest breakthrough moment?", "type": "textarea", "required": True},
    {"id": "mental_barrier", "label": "What's the #1 mental barrier you're committed to eliminating this season?", "type": "textarea", "required": True},
    {"id": "commitment_level", "label": "How committed are you to solving this barrier this season?", "type": "select",
     "options": ["10/10 - Whatever it takes", "8-9/10 - Very committed", "6-7/10 - Fairly committed", "Below 6 - Exploring options"], "required": True},
    {"id": "full_potential_feeling", "label": "If you were performing at your full potential consistently, how would racing feel different?", "type": "textarea", "required": True},
    {"id": "racer_type", "label": "What best describes you?", "type": "select",
     "options": ["Professional Racer", "Semi-Professional", "Serious Amateur", "Club Racer", "Aspiring Professional"], "required": True},
    {"id": "funding_source", "label": "Who funds your racing?", "type": "select",
     "options": ["Self-Funded", "Family Funded (Bank of Mum and Dad)", "Sponsored", "Mix of Self & Sponsor", "Looking for Sponsorship"], "required": True},
    {"id": "financial_ready", "label": "If accepted into Flow Performance, do you have the financial resources to invest in elite-level mental training right now?", "type": "select",
     "options": ["Yes, I have the resources", "I'd need to arrange it but could do so", "I'd need a payment plan", "Not sure yet"], "required": True},
    {"id": "willingness_invest", "label": "How willing are you to invest time, focus, and effort into achieving your racing goals?", "type": "select",
     "options": ["100% - All in", "Very willing", "Willing but cautious", "Need more info first"], "required": True},
    {"id": "seriousness_scale", "label": "On a scale of 1-10, how serious are you about breakthrough performance?", "type": "slider", "min": 1, "max": 10, "required": True},
    {"id": "three_year_vision", "label": "Where do you see yourself 3 years from now?", "type": "textarea", "required": True},
    {"id": "anything_else", "label": "Anything else we should know?", "type": "textarea", "required": False},
]

# =============================================================================
# DYNAMIC TERMINOLOGY SWAP (Rider ↔ Driver)
# =============================================================================

RIDER_TO_DRIVER_SWAPS = {
    "Rider": "Driver", "rider": "driver", "Riders": "Drivers", "riders": "drivers",
    "Ride": "Drive", "ride": "drive", "Riding": "Driving", "riding": "driving",
    "Bike": "Car", "bike": "car", "Bikes": "Cars", "bikes": "cars",
    "Machine": "Car", "machine": "car",
    "Handlebars": "Steering Wheel", "handlebars": "steering wheel",
    "Saddle": "Cockpit", "saddle": "cockpit",
    "MotoGP": "F1", "BSB": "BTCC",
    "Lean Angle": "G-Force", "lean angle": "g-force",
    "arm pump": "pedal feel/fatigue",
    "lap time": "lap time",  # stays the same
}

def swap_terminology(text: str, to_driver: bool = True) -> str:
    """Swap rider/motorcycle terms to driver/car terms or vice versa."""
    if not to_driver:
        return text
    result = text
    # Sort by length (longest first) to avoid partial replacements
    for old, new in sorted(RIDER_TO_DRIVER_SWAPS.items(), key=lambda x: -len(x[0])):
        result = result.replace(old, new)
    return result


# =============================================================================
# CHAMPIONSHIP STRATEGY CALL FRAMEWORK (The Master Script)
# =============================================================================

CALL_1_FRAMEWORK = """
## CHAMPIONSHIP STRATEGY CALL — CALL 1 (The Discovery Call)

### Stage 1: Opener & Rapport (2-3 minutes)
"Hey {name}, how are you doing? Good to finally speak to you!"

"So the idea of this call really is a bit of fact-finding from both sides — for you to ask me any questions, and obviously just to get to know you. I know a little bit more than I did three days ago because you've gone through the mini course."

"How did you find that mini course?"

*Let them talk — build rapport. Listen for emotional cues.*

"And so maybe you could start by telling me why you decided to book a call at the end of it? Because some people might go, yeah, that's all interesting stuff, thanks a lot. But you went, no, let's book a call. Why is that?"

---

### Stage 2: The Detective — Pain Amplification (10-15 minutes)
*This is the CORE of the call. Dig deep into their struggles.*

"Tell me about your racing. What championship are you in? What's your current level?"

{detective_notes}

**Key Questions to Ask:**
- "What's been your biggest frustration this season?"
- "When you're on track, what's going through your mind at the crucial moments?"
- "What have you tried before to fix this?"
- "How long has this been going on?"
- "What's it costing you — results-wise, financially, emotionally?"

*Score their pain on a scale of 1-10. You need 7+ to proceed confidently.*

---

### Stage 3: The Dream — Future Pacing (5 minutes)
"If we could wave a magic wand and fix all of this, what would your racing look like?"

{dream_notes}

"What would that mean to you personally? To your family? Your sponsors?"

*Get them emotionally connected to the outcome.*

---

### Stage 4: The Bridge — Credibility & Case Studies (5 minutes)
"Can I share something with you? Because your situation reminds me of Sam Wilford..."

"Sam came to us in a similar position. He was talented, everyone knew it, but he couldn't put it together consistently. Within the programme, he went on to secure a fully-funded ride in the CEV Moto2 Championship, racing alongside Fermín Aldeguer, Alonso López and many more current MotoGP stars."

"On a scale of 1-10 how close do you think that programme would come to fixing {their_struggles}?"

---

### Stage 5: The Framework — Present Your Solution
"You're Struggling with.... {their_struggles}"
"You've Tried.... {what_theyve_tried}"
"You Need.... A proven process that's going to give you the way to perform consistently at your best to achieve {their_goal}"

---

### Stage 6: The Tiedowns (5 minutes)

**Desire:** "Does this sound like something you'd want to do?"

**Investment:** "Shall we talk about the investment needed to join Flow Performance?"

"Firstly, can you commit to 20 minutes a day for the training?"

"Regarding the finances, when I used to work 1-on-1 with {riders_or_drivers}, all this would have cost over £9,000 per season. But because we now have a training platform, you don't need to pay for my time, travel, and accommodation."

"You get lifetime access to it all for only £4,000."

"How does that sound to you?"

**Payment:** "Looking at the investment, if we had space and we both decide it's a good fit, how would you want to make the payment — on a credit card or would you want to chunk it down into a few payments?"

"I'm not saying there is space yet, but let's explore that for a minute."

**Timing:** "If we both decide it's a good fit, is there anything stopping you from starting immediately?"

---

### Stage 7: Book Call 2
"I need to complete my other interviews."

"Let's reconnect tomorrow at [time] and I'll let you know my decision."

"So {name}, you've got a decision to make in the next 24 hours."

**Paint Two Pictures:**

"Let me paint two pictures: 3 months from now you're going to be somewhere and you're going to be someone — the question is, who will you be?"

**Version 1 — No Action:** "Things stay the same, you decide the time's not right to get started, £4000 is too much to invest in yourself. 3 months from now: You're still {their_current_struggle}, still frustrated, still finishing not where you want to be. Nowhere near your goal of {their_goal}. You saved 4k but what has it actually cost you?"

**Version 2 — You Invest:** "3 months from now: You've applied the mental frameworks, you understand why you struggled before, you've had breakthrough weekends. People are taking notice, asking what you have changed? Instead of frustration, you're driving home knowing no one on your {bike_or_car} with your budget on that track in those conditions could have {ridden_or_driven} better than you did today. You're excited about your progress and you're running where you should be. AND you've secured your first £12k sponsor and you are talking to many more."

"In the 3 months, you're going to be someone — which one do you want to be?"

**Book Second Call:** [calendly.com/caminocoaching/rider-fit-call]

*"Is there anything you feel you've missed that I can add to my notes before we head off?"*
"""

CALL_2_FRAMEWORK = """
## CHAMPIONSHIP STRATEGY CALL — CALL 2 (The Close)

### Recap
"Ok so just let me recap what we covered last time..."

"You want to {their_goal}"
"You're {their_current_struggle} with {their_struggles}"
"You have tried {what_theyve_tried}"
"The £4,000 investment felt manageable"
"You're ready to work on the mental side rather than more equipment"

"Still accurate?"

---

### Coachability Check
"One question: *How coachable are you?*"

"We've found our process works best when {riders_or_drivers} follow the steps exactly."

"*I only like to work with people who are coachable, open to feedback, and ready to take action quickly.*"

"This works best for {riders_or_drivers} who are fully committed and follow the program to the T."

"*Is that you?*"

"What about your commitment level — are you genuinely ready for daily practice?"
"Your racing, home, and work schedule allows for proper implementation?"

---

### Good News / Bad News
"I've got some good news and some bad news for you. Which would you like first?"

**GOOD NEWS:** "Look, I loved your application and my notes on your situation. Particularly {specific_thing_1} and {specific_thing_2}. So I'm very keen to have you in the program and would like to offer you a spot. Congratulations, we're looking forward to working with you on this."

**BAD NEWS:** "I'm afraid you're going to be stuck with the Camino family for the next 6 months!"

"How are you feeling — excited, nervous, or a bit of both?"

---

### Close
"Perfect. I'll get your member area set up and our first call booked. From my notes, you preferred [payment option], so I have that ready. When you're ready, I'll take your card details."

"You've made an excellent choice. This is exactly what will transform your racing. Here's what happens next... you will get an email in the next 30 minutes."

---

### Objection Handling
"Do you have any questions or concerns before we get you set up?"

- "What has changed between our first call and now?"
- "What's the real reason you are hesitating?"
- "What would you need to feel comfortable moving forward?"
- "Would it help to get started with the Starter plan and upgrade later?"
- "Should we set a check-in date to finalise your spot?"

**"I still need to think about it":**
- Ask what's changed since the first call?
- If it's logistics of moving money: take the £500 deposit. Company policy!
- IF it MUST go to a 3rd call: set it up so there's no way they can stall for a 4th.
"""

# =============================================================================
# PRE-CALL PREPARATION (Mode 1)
# =============================================================================

def analyze_candidate_data(answers: Dict[str, str]) -> Dict[str, any]:
    """Extract key indicators from candidate application answers."""
    analysis = {
        "name": f"{answers.get('first_name', '')} {answers.get('last_name', '')}".strip(),
        "age": answers.get("age", "Unknown"),
        "championship": answers.get("championship", "Unknown"),
        "performance_level": answers.get("performance_level", "Unknown"),
        "season_goal": answers.get("season_goal", "Not specified"),
        "mental_barrier": answers.get("mental_barrier", "Not specified"),
        "assessment_surprise": answers.get("assessment_surprise", "Not specified"),
        "blueprint_breakthrough": answers.get("blueprint_breakthrough", "Not specified"),
        "full_potential_feeling": answers.get("full_potential_feeling", "Not specified"),
        "three_year_vision": answers.get("three_year_vision", "Not specified"),
        "anything_else": answers.get("anything_else", ""),
    }

    # Coachability indicator
    commitment = answers.get("commitment_level", "")
    if "10/10" in commitment or "Whatever it takes" in commitment:
        analysis["coachability"] = "🟢 HIGH — Commitment 10/10, ready to do whatever it takes"
    elif "8-9" in commitment or "Very committed" in commitment:
        analysis["coachability"] = "🟡 GOOD — Strong commitment, prime candidate"
    else:
        analysis["coachability"] = "🟠 MODERATE — May need extra conviction building"

    # Financial qualification
    funding = answers.get("funding_source", "")
    financial = answers.get("financial_ready", "")
    if "Family" in funding or "Mum and Dad" in funding:
        analysis["financial_flag"] = "⚠️ Family-funded — parents may need to be on the call"
    elif "Yes" in financial:
        analysis["financial_flag"] = "🟢 Financially ready — has resources"
    elif "payment plan" in financial.lower():
        analysis["financial_flag"] = "🟡 Needs payment plan — present options"
    else:
        analysis["financial_flag"] = "🟠 Financial uncertainty — qualify carefully"

    # Pain gap
    analysis["pain_gap"] = f"Current: {answers.get('performance_level', 'N/A')} → Goal: {answers.get('season_goal', 'N/A')}"

    # Seriousness
    seriousness = answers.get("seriousness_scale", "5")
    try:
        score = int(seriousness)
        if score >= 8:
            analysis["seriousness"] = f"🟢 {score}/10 — Very serious"
        elif score >= 6:
            analysis["seriousness"] = f"🟡 {score}/10 — Fairly serious"
        else:
            analysis["seriousness"] = f"🟠 {score}/10 — May need more warming up"
    except ValueError:
        analysis["seriousness"] = f"❓ {seriousness}"

    return analysis


def generate_script_overlay(answers: Dict[str, str], is_driver: bool = True) -> str:
    """Generate the personalized Call 1 script with bracketed insights."""
    analysis = analyze_candidate_data(answers)
    name = analysis["name"]

    # Build detective notes from candidate data
    detective_notes = []
    if analysis.get("assessment_surprise"):
        detective_notes.append(
            f"[>> NOTE: Candidate's biggest assessment surprise was: \"{analysis['assessment_surprise']}\". "
            f"Dig into this — ask how it affects their race weekends. <<]"
        )
    if analysis.get("mental_barrier"):
        detective_notes.append(
            f"[>> NOTE: Their #1 mental barrier is: \"{analysis['mental_barrier']}\". "
            f"This is their core pain point — amplify this throughout the call. <<]"
        )
    if analysis.get("blueprint_breakthrough"):
        detective_notes.append(
            f"[>> NOTE: Breakthrough moment from Blueprint: \"{analysis['blueprint_breakthrough']}\". "
            f"Reference this to show the programme already started working for them. <<]"
        )

    detective_str = "\n".join(detective_notes) if detective_notes else "[No pre-call data available — use standard discovery questions]"

    # Build dream notes
    dream_notes = []
    if analysis.get("full_potential_feeling"):
        dream_notes.append(f"[>> NOTE: When performing at full potential, they said: \"{analysis['full_potential_feeling']}\". Mirror this language back to them. <<]")
    if analysis.get("three_year_vision"):
        dream_notes.append(f"[>> NOTE: 3-year vision: \"{analysis['three_year_vision']}\". Connect the programme to this long-term goal. <<]")

    dream_str = "\n".join(dream_notes) if dream_notes else ""

    # Key indicators summary
    summary = f"""
## 🎯 PRE-CALL BRIEF: {name}
**Championship:** {analysis['championship']} | **Level:** {analysis['performance_level']} | **Age:** {analysis['age']}

### Key Indicators
| Indicator | Status |
|-----------|--------|
| Coachability | {analysis['coachability']} |
| Financial | {analysis['financial_flag']} |
| Pain Gap | {analysis['pain_gap']} |
| Seriousness | {analysis['seriousness']} |

### ⚡ Quick Hits for the Call
- **Their Goal:** {analysis['season_goal']}
- **Their Barrier:** {analysis['mental_barrier']}
- **What Surprised Them:** {analysis['assessment_surprise']}
- **Additional Context:** {analysis.get('anything_else', 'None')}

---
"""

    # Generate the full script
    script = CALL_1_FRAMEWORK.format(
        name=name,
        detective_notes=detective_str,
        dream_notes=dream_str,
        their_struggles=analysis.get("mental_barrier", "[ASK ON CALL]"),
        what_theyve_tried="[ASK ON CALL — what have you tried before?]",
        their_goal=analysis.get("season_goal", "[ASK ON CALL]"),
        their_current_struggle=analysis.get("mental_barrier", "[their current struggle]"),
        riders_or_drivers="drivers" if is_driver else "riders",
        bike_or_car="car" if is_driver else "bike",
        ridden_or_driven="driven" if is_driver else "ridden",
        specific_thing_1="[SPECIFIC THING 1 from call]",
        specific_thing_2="[SPECIFIC THING 2 from call]",
    )

    if is_driver:
        script = swap_terminology(script, to_driver=True)
        summary = swap_terminology(summary, to_driver=True)

    return summary + script


# =============================================================================
# GOLD STANDARD CALL BENCHMARKS
# Sam Hirst (2 calls → CLOSED) and Angela Brunson (2 calls → CLOSED)
# These are the target patterns every call should aim for.
# =============================================================================

GOLD_STANDARD = {
    "sam_hirst": {
        "name": "Sam Hirst",
        "outcome": "CLOSED — 2-call process",
        "call_1_highlights": {
            "opener": "Asked 'How did you find the mini course?' — let Sam talk freely, built natural rapport before any selling.",
            "detective": "Spent 15+ minutes in pain discovery. Key moment: Sam admitted 'I've hit my potential previously but only in dribs and drabs' — Craig dug into WHY the consistency gaps happen.",
            "dream": "Got Sam to articulate his own vision: 'I really want to get to my potential' — Craig let Sam sell himself on the outcome.",
            "framework": "Positioned the struggle clearly: 'You're struggling with consistency. You've tried bits and pieces. You need a proven system.'",
            "tiedowns": "Presented £4,000 investment naturally. Used 'If we had space and we both decide it's a good fit' — non-pressured.",
            "push_pull": "Masterful: 'I have a few more calls this week with prospects, so I'd like to speak to everyone first before I decide who I'd like to work with.' — Craig is choosing THEM, not selling.",
            "close": "Booked Call 2 with dad present (funding decision-maker). Set homework between calls.",
        },
        "call_2_highlights": {
            "recap": "Opened with clear recap of situation, desire, and goal from Call 1.",
            "coachability": "Asked directly: 'How coachable are you?' — qualified commitment before offering spot.",
            "good_bad_news": "Used the 'good news/bad news' frame perfectly. Good: offered a spot. Bad: 'stuck with us for 6 months!'",
            "close": "Took payment on the call. Smooth transition: 'From my notes you preferred [payment option], so I have that ready.'",
        },
        "key_techniques": [
            "Let the prospect talk 60%+ of the time — Craig listened more than he spoke",
            "Referenced pre-call data naturally without reading from a script",
            "Used 'my notes' language throughout — positions Craig as selective interviewer, not salesperson",
            "Dad was included on Call 2 (under 18/funding) — planned ahead in Call 1",
            "Paint Two Pictures technique: 3 months with vs without the programme",
        ],
    },
    "angela_brunson": {
        "name": "Angela Brunson",
        "outcome": "CLOSED — 2-call process",
        "call_1_highlights": {
            "opener": "Asked 'Why did you decide to book a call?' — got Angela to state her own reasons for being there.",
            "detective": "Deep discovery: Angela revealed specific frustrations — 'I know I can do better but something holds me back.' Craig asked what she'd tried before and how long the issue had persisted.",
            "framework": "Crystal clear positioning: 'You're struggling with [X], You've tried [Y], You need [Z]' — textbook execution.",
            "tiedowns": "Investment discussion felt natural. Used the platform vs 1-on-1 comparison (£9k → £4k) effectively.",
            "credibility": "Shared relatable case studies that matched Angela's situation specifically.",
        },
        "call_2_highlights": {
            "recap": "Started with exact recap: 'You want to [goal]. You're [struggling with X]. You tried [Y]. The £4,000 felt manageable.'",
            "good_bad_news": "Perfect execution of the good news/bad news close.",
            "objections": "When Angela hesitated, Craig asked 'What's changed since our first call?' — textbook objection isolation.",
            "close": "Smooth payment collection on the call.",
        },
        "key_techniques": [
            "Strong emotional connection — Angela felt heard and understood",
            "Used Assessment data in conversation: referenced specific scores and gaps",
            "Kept asking scaling questions: 'On a scale of 1-10...' to gauge progress",
            "Two Pictures technique delivered with conviction — Angela could visualise both futures",
            "Coachability check before offering spot — filters and flatters simultaneously",
        ],
    },
}


# =============================================================================
# POST-CALL ANALYSIS (Mode 2)
# Compares against Gold Standard (Sam Hirst & Angela Brunson)
# =============================================================================

SCRIPT_STAGES = [
    "Stage 1: Opener & Rapport",
    "Stage 2: The Detective — Pain Amplification",
    "Stage 3: The Dream — Future Pacing",
    "Stage 4: The Bridge — Credibility & Case Studies",
    "Stage 5: The Framework — Present Solution",
    "Stage 6: The Tiedowns",
    "Stage 7: Book Call 2 / Close",
]

def analyze_call_transcript(transcript: str, candidate_answers: Optional[Dict] = None) -> Dict:
    """Analyze a call transcript against the Championship Strategy Call Framework
    and the Gold Standard calls (Sam Hirst & Angela Brunson).
    Returns a structured analysis dict for display."""

    transcript_lower = transcript.lower()
    word_count = len(transcript.split())
    analysis = {
        "stages_detected": [],
        "adherence_score": 0,
        "pain_amplification_score": 0,
        "push_pull_score": 0,
        "objection_handling_score": 0,
        "strengths": [],
        "missed_opportunities": [],
        "actionable_advice": [],
        "gold_standard_comparison": [],
    }

    # --- Stage Detection ---
    stage_markers = {
        "Opener & Rapport": ["how are you", "good to speak", "tell me why you decided to book", "mini course", "how did you find", "why you decided to book"],
        "The Detective": ["biggest frustration", "what's been going on", "how long has this been", "what have you tried", "what's going through your mind", "struggles", "costs you", "what's it costing", "frustrated", "struggling"],
        "The Dream": ["magic wand", "what would it look like", "full potential", "what would that mean", "if we could fix"],
        "The Bridge": ["sam wilford", "case study", "similar position", "scale of 1-10", "programme would come to fixing", "moto2", "motogp"],
        "Present Solution": ["you're struggling with", "you've tried", "you need", "proven process", "you're struggling"],
        "The Tiedowns": ["investment", "£4,000", "4000", "4,000", "payment plan", "credit card", "chunk it down", "20 minutes a day", "commit to 20 minutes"],
        "Book Call 2 / Close": ["reconnect tomorrow", "second call", "good news and bad news", "offer you a spot", "card details", "good news", "bad news", "let you know my decision"],
    }

    detected_count = 0
    for stage_name, markers in stage_markers.items():
        found_markers = [m for m in markers if m in transcript_lower]
        if found_markers:
            detected_count += 1
            analysis["stages_detected"].append(f"✅ {stage_name}")
        else:
            analysis["stages_detected"].append(f"❌ {stage_name} — NOT DETECTED")

    analysis["adherence_score"] = round((detected_count / len(stage_markers)) * 100)

    # --- Pain Amplification Analysis ---
    pain_keywords = ["frustrat", "struggle", "cost you", "how long", "what happens when", "worst",
                     "difficult", "challenge", "stuck", "holding you back", "letting you down",
                     "costing you", "what does that feel like", "tell me more", "dig deeper"]
    pain_count = sum(transcript_lower.count(k) for k in pain_keywords)
    analysis["pain_amplification_score"] = min(100, pain_count * 10)

    if pain_count >= 7:
        analysis["strengths"].append("🏆 Excellent pain amplification — matches Gold Standard depth (Sam Hirst level)")
    elif pain_count >= 4:
        analysis["strengths"].append("Good pain discovery, but could go deeper")
        analysis["gold_standard_comparison"].append("📌 **Sam Hirst Call 1:** Craig spent 15+ min in The Detective. Sam said 'I've hit my potential but only in dribs and drabs' — Craig dug into WHY the consistency gaps happen. Aim for this depth.")
    else:
        analysis["missed_opportunities"].append("⚠️ Pain amplification too shallow — The Detective stage needs much more depth")
        analysis["gold_standard_comparison"].append("📌 **Gold Standard:** In Sam's call, Craig asked follow-up after follow-up until Sam articulated his own pain. In Angela's call, she revealed 'I know I can do better but something holds me back.' You need 5+ pain-probing questions minimum.")

    # --- Push/Pull Analysis ---
    push_pull_markers = {
        "reverse_frame": ["i need to complete my other interviews", "let me speak to everyone first", "i'll let you know my decision", "speak to everyone first", "other interviews"],
        "takeaway": ["not saying there is space", "limited spots", "not sure we have room", "only work with", "only like to work with"],
        "urgency": ["decision to make", "24 hours", "spots fill up", "this week", "decision to make in the next"],
        "selectivity": ["who i'd like to work with", "decide who", "i'm choosing", "not everyone gets a spot"],
    }

    pp_count = 0
    pp_found = []
    for category, markers in push_pull_markers.items():
        if any(m in transcript_lower for m in markers):
            pp_count += 1
            pp_found.append(category)

    analysis["push_pull_score"] = round((pp_count / len(push_pull_markers)) * 100)

    if pp_count >= 3:
        analysis["strengths"].append("🏆 Strong Push/Pull — matches Gold Standard selectivity positioning")
    elif pp_count >= 1:
        missing = [k for k in push_pull_markers if k not in pp_found]
        analysis["missed_opportunities"].append(f"Push/Pull partially used but missing: {', '.join(missing)}")
        analysis["gold_standard_comparison"].append("📌 **Sam Hirst Call 1:** Craig said 'I have a few more calls this week with prospects, so I'd like to speak to everyone first before I decide who I'd like to work with.' — This flips the dynamic: Craig is the one choosing, not selling.")
    else:
        analysis["missed_opportunities"].append("⚠️ No Push/Pull detected — this is critical for maintaining the 58% conversion rate")
        analysis["gold_standard_comparison"].append("📌 **Gold Standard technique:** 'I need to complete my other interviews' + 'Not saying there is space yet' + '24-hour decision' = the triple Push/Pull that Sam & Angela both responded to.")

    # --- Objection Handling ---
    objection_markers = ["what's changed", "real reason", "feel comfortable", "payment plan", "deposit",
                        "company policy", "what would you need", "what's stopping you", "anything stopping"]
    obj_count = sum(1 for m in objection_markers if m in transcript_lower)
    analysis["objection_handling_score"] = min(100, obj_count * 25)

    if obj_count >= 3:
        analysis["strengths"].append("Handled objections effectively — multiple frameworks deployed")
    elif obj_count >= 1:
        analysis["strengths"].append("Some objection handling present")
    elif "think about it" in transcript_lower or "not sure" in transcript_lower:
        analysis["missed_opportunities"].append("Objections were raised but not handled with framework responses")
        analysis["gold_standard_comparison"].append("📌 **Angela Brunson Call 2:** When Angela hesitated, Craig asked: 'What's changed since our first call?' — This isolates the real objection. Then: 'What would you need to feel comfortable moving forward?'")

    # --- Two Pictures Technique ---
    two_pictures_markers = ["two pictures", "version 1", "version 2", "3 months from now",
                           "three months from now", "picture 1", "picture 2", "paint two"]
    if any(m in transcript_lower for m in two_pictures_markers):
        analysis["strengths"].append("✅ Used the 'Two Pictures' future-pacing technique")
    else:
        analysis["missed_opportunities"].append("Missed the 'Two Pictures' technique — this is a powerful closer")
        analysis["gold_standard_comparison"].append("📌 **Gold Standard:** 'Let me paint two pictures: 3 months from now...' Version 1 = no action (still stuck). Version 2 = invested (breakthrough results). Both Sam and Angela responded strongly to this.")

    # --- Coachability Check ---
    coachability_markers = ["how coachable", "coachable are you", "follow the process", "follow our process", "committed to taking action"]
    if any(m in transcript_lower for m in coachability_markers):
        analysis["strengths"].append("✅ Performed coachability check — qualifies AND flatters the prospect")
    else:
        if word_count > 2000:  # Only flag on longer transcripts (likely full calls)
            analysis["missed_opportunities"].append("No coachability check detected — this both qualifies and creates buy-in")
            analysis["gold_standard_comparison"].append("📌 **Sam Hirst Call 2:** Craig asked: 'How coachable are you? I only like to work with people who are coachable, open to feedback, and ready to take action quickly.' — This filters AND makes them prove they deserve the spot.")

    # --- Prospect Talk Ratio ---
    # Estimate based on conversation markers
    craig_markers = transcript_lower.count("craig") + transcript_lower.count("@") // 2
    prospect_lines = len([l for l in transcript.split('\n') if l.strip() and 'craig' not in l.lower() and '@' not in l])
    if craig_markers > 0 and prospect_lines > 0:
        ratio = prospect_lines / (craig_markers + prospect_lines) * 100
        if ratio >= 50:
            analysis["strengths"].append(f"Good talk ratio — prospect speaking ~{int(ratio)}% of the time")
        else:
            analysis["gold_standard_comparison"].append(f"📌 **Gold Standard:** Craig lets the prospect talk 60%+ of the time. You're at ~{int(ratio)}%. Ask more questions, talk less.")

    # --- Generate Actionable Advice ---
    if analysis["adherence_score"] < 70:
        analysis["actionable_advice"].append("📋 Follow the script flow more closely. The framework converts at 58% — trust the process.")
    if analysis["pain_amplification_score"] < 50:
        analysis["actionable_advice"].append("🔍 Spend more time in 'The Detective' stage. In Sam's gold standard call, Craig asked 5+ pain questions before moving to solutions. Ask: 'What does that cost you?' and 'How long has this been going on?'")
    if analysis["push_pull_score"] < 50:
        analysis["actionable_advice"].append("⚖️ Add more Push/Pull: 'I need to speak to my other candidates first' creates the scarcity that drives the 58% close rate.")
    if analysis["objection_handling_score"] < 25 and word_count > 2000:
        analysis["actionable_advice"].append("🛡️ Prepare objection responses: 'What's changed since Call 1?', 'What would you need to feel comfortable?', and always have the £500 deposit as a fallback.")
    if not analysis["actionable_advice"]:
        analysis["actionable_advice"].append("🏆 Strong call! You're matching the Gold Standard patterns. Focus on getting the commitment scale to 10/10 before moving to tiedowns.")

    # --- Overall Score ---
    analysis["overall_score"] = round(
        (analysis["adherence_score"] * 0.35 +
         analysis["pain_amplification_score"] * 0.30 +
         analysis["push_pull_score"] * 0.20 +
         analysis["objection_handling_score"] * 0.15)
    )

    return analysis


def format_analysis_report(analysis: Dict) -> str:
    """Format the post-call analysis into a readable report."""

    # Gold standard comparison section
    gs_section = ""
    if analysis.get("gold_standard_comparison"):
        gs_section = "\n### 🏆 Gold Standard Comparison (Sam Hirst & Angela Brunson)\n"
        gs_section += "\n".join("- " + g for g in analysis["gold_standard_comparison"])
        gs_section += "\n"

    report = f"""
## 📊 Post-Call Analysis Report

### Overall Score: {analysis['overall_score']}/100
*Benchmarked against Gold Standard: Sam Hirst (CLOSED) & Angela Brunson (CLOSED)*

| Category | Score | Gold Standard Target |
|----------|-------|---------------------|
| Script Adherence | {analysis['adherence_score']}% | 85%+ |
| Pain Amplification | {analysis['pain_amplification_score']}% | 70%+ |
| Push/Pull Dynamics | {analysis['push_pull_score']}% | 75%+ |
| Objection Handling | {analysis['objection_handling_score']}% | 50%+ |

### Script Stages Detected
{chr(10).join(analysis['stages_detected'])}

### ✅ Strengths
{chr(10).join('- ' + s for s in analysis['strengths']) if analysis['strengths'] else '- No specific strengths flagged'}

### ⚠️ Missed Opportunities
{chr(10).join('- ' + m for m in analysis['missed_opportunities']) if analysis['missed_opportunities'] else '- No missed opportunities — great job!'}
{gs_section}
### 🎯 Actionable Advice (to maintain 58% conversion)
{chr(10).join('- ' + a for a in analysis['actionable_advice'])}
"""
    return report
