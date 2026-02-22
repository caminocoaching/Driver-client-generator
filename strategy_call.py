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
# POST-CALL ANALYSIS (Mode 2)
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
    """Analyze a call transcript against the Championship Strategy Call Framework.
    Returns a structured analysis dict for display."""

    transcript_lower = transcript.lower()
    analysis = {
        "stages_detected": [],
        "adherence_score": 0,
        "pain_amplification_score": 0,
        "push_pull_score": 0,
        "objection_handling_score": 0,
        "strengths": [],
        "missed_opportunities": [],
        "actionable_advice": [],
    }

    # --- Stage Detection ---
    stage_markers = {
        "Opener & Rapport": ["how are you", "good to speak", "tell me why you decided to book", "mini course", "how did you find"],
        "The Detective": ["biggest frustration", "what's been going on", "how long has this been", "what have you tried", "what's going through your mind", "struggles", "costs you"],
        "The Dream": ["magic wand", "what would it look like", "full potential", "what would that mean"],
        "The Bridge": ["sam wilford", "case study", "similar position", "scale of 1-10", "programme would come to fixing"],
        "Present Solution": ["you're struggling with", "you've tried", "you need", "proven process"],
        "The Tiedowns": ["investment", "£4,000", "4000", "payment plan", "credit card", "chunk it down", "20 minutes a day"],
        "Book Call 2 / Close": ["reconnect tomorrow", "second call", "good news and bad news", "offer you a spot", "card details"],
    }

    detected_count = 0
    for stage_name, markers in stage_markers.items():
        found = any(m in transcript_lower for m in markers)
        if found:
            detected_count += 1
            analysis["stages_detected"].append(f"✅ {stage_name}")
        else:
            analysis["stages_detected"].append(f"❌ {stage_name} — NOT DETECTED")

    analysis["adherence_score"] = round((detected_count / len(stage_markers)) * 100)

    # --- Pain Amplification Analysis ---
    pain_keywords = ["frustrat", "struggle", "cost you", "how long", "what happens when", "worst", "difficult", "challenge", "stuck"]
    pain_count = sum(1 for k in pain_keywords if k in transcript_lower)
    analysis["pain_amplification_score"] = min(100, pain_count * 15)

    if pain_count >= 5:
        analysis["strengths"].append("Strong pain amplification — dug deep into the candidate's struggles")
    elif pain_count >= 3:
        analysis["missed_opportunities"].append("Pain amplification could go deeper. Ask more 'what does that cost you?' questions")
    else:
        analysis["missed_opportunities"].append("⚠️ Insufficient pain amplification. The Detective stage needs much more depth")

    # --- Push/Pull Analysis ---
    push_pull_markers = {
        "reverse_frame": ["i need to complete my other interviews", "let me speak to everyone first", "i'll let you know my decision"],
        "takeaway": ["not saying there is space", "limited spots", "not sure we have room", "only work with"],
        "urgency": ["decision to make", "24 hours", "spots fill up", "this week"],
    }

    pp_count = 0
    for category, markers in push_pull_markers.items():
        if any(m in transcript_lower for m in markers):
            pp_count += 1

    analysis["push_pull_score"] = round((pp_count / len(push_pull_markers)) * 100)

    if pp_count >= 2:
        analysis["strengths"].append("Good use of Push/Pull dynamics — created scarcity and selectivity")
    else:
        analysis["missed_opportunities"].append("Push/Pull could be stronger. Use more takeaway language ('I need to check if we have space')")

    # --- Objection Handling ---
    objection_markers = ["what's changed", "real reason", "feel comfortable", "payment plan", "deposit", "company policy"]
    obj_count = sum(1 for m in objection_markers if m in transcript_lower)
    analysis["objection_handling_score"] = min(100, obj_count * 25)

    if obj_count >= 2:
        analysis["strengths"].append("Handled objections effectively with proven frameworks")
    elif "think about it" in transcript_lower or "not sure" in transcript_lower:
        analysis["missed_opportunities"].append("Objections were raised but handling could be stronger. Use: 'What would you need to feel comfortable?'")

    # --- Generate Actionable Advice ---
    if analysis["adherence_score"] < 70:
        analysis["actionable_advice"].append("📋 Follow the script flow more closely. The framework converts at 58% — trust the process.")
    if analysis["pain_amplification_score"] < 50:
        analysis["actionable_advice"].append("🔍 Spend more time in 'The Detective' stage. Ask at least 5 pain-probing questions before moving to solutions.")
    if analysis["push_pull_score"] < 50:
        analysis["actionable_advice"].append("⚖️ Add more Push/Pull: 'I need to speak to my other candidates first' creates powerful scarcity.")
    if not analysis["actionable_advice"]:
        analysis["actionable_advice"].append("🏆 Strong call! Focus on getting the commitment scale to 10/10 before moving to tiedowns.")

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
    report = f"""
## 📊 Post-Call Analysis Report

### Overall Score: {analysis['overall_score']}/100

| Category | Score |
|----------|-------|
| Script Adherence | {analysis['adherence_score']}% |
| Pain Amplification | {analysis['pain_amplification_score']}% |
| Push/Pull Dynamics | {analysis['push_pull_score']}% |
| Objection Handling | {analysis['objection_handling_score']}% |

### Script Stages Detected
{chr(10).join(analysis['stages_detected'])}

### ✅ Strengths
{chr(10).join('- ' + s for s in analysis['strengths']) if analysis['strengths'] else '- No specific strengths flagged'}

### ⚠️ Missed Opportunities
{chr(10).join('- ' + m for m in analysis['missed_opportunities']) if analysis['missed_opportunities'] else '- No missed opportunities — great job!'}

### 🎯 Actionable Advice (to maintain 58% conversion)
{chr(10).join('- ' + a for a in analysis['actionable_advice'])}
"""
    return report
