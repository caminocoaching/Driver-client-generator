#!/usr/bin/env python3
"""
Strategy Call System
====================
Implements the Championship Strategy Call Framework with:
- Application Questions (replacing Typeform)
- Pre-Call Preparation (personalized script overlay)
- Post-Call Analysis (scoring against gold standard)
- Dynamic Driver/Driver terminology
"""

import streamlit as st
from datetime import datetime
import re
import json

# =============================================================================
# TERMINOLOGY SWAP — Driver ↔ Driver
# =============================================================================
RIDER_TO_DRIVER_SWAPS = {
    "driver": "driver",
    "drivers": "drivers",
    "ride": "drive",
    "riding": "driving",
    "bike": "car",
    "bikes": "cars",
    "machine": "car",
    "handlebars": "steering wheel",
    "saddle": "cockpit",
    "MotoGP": "F1",
    "BSB": "BTCC",
    "Moto2": "GT3",
    "lean angle": "G-force",
    "arm pump": "pedal feel",
    "on your bike": "in your car",
    "on that track": "on that circuit",
}

def swap_terminology(text, to_driver=False):
    """Swap driver/driver terminology in text."""
    if not to_driver or not text:
        return text
    result = text
    for driver_term, driver_term in RIDER_TO_DRIVER_SWAPS.items():
        # Case-preserving replacement
        result = re.sub(
            re.escape(driver_term),
            driver_term,
            result,
            flags=re.IGNORECASE
        )
        # Title case
        result = re.sub(
            re.escape(driver_term.title()),
            driver_term.title(),
            result
        )
    return result


# =============================================================================
# APPLICATION QUESTIONS (replacing Typeform)
# =============================================================================
APPLICATION_QUESTIONS = [
    {"id": "first_name", "label": "First Name", "type": "text", "required": True},
    {"id": "last_name", "label": "Last Name", "type": "text", "required": True},
    {"id": "email", "label": "Email", "type": "text", "required": True},
    {"id": "phone", "label": "Phone Number", "type": "text", "required": True},
    {"id": "age", "label": "What Is Your Age? (If Under 18 Your Parents Will Need To Be On The Call)", "type": "text", "required": True},
    {"id": "current_level", "label": "What is your current level of performance?", "type": "text", "required": True},
    {"id": "championship", "label": "What Championship do you race in?", "type": "text", "required": True},
    {"id": "inspiration", "label": "What initially inspired you to pursue racing at this level?", "type": "textarea", "required": True},
    {"id": "season_goal", "label": "What's your No.1 racing goal for this season?", "type": "textarea", "required": True},
    {"id": "surprise_score", "label": "Your IMPROVE Assessment revealed specific performance gaps. Which category surprised you most with its score?", "type": "textarea", "required": True},
    {"id": "breakthrough_topic", "label": "After completing the Podium Contenders Blueprint 3-day training, which topic created your biggest breakthrough moment?", "type": "textarea", "required": True},
    {"id": "mental_barrier", "label": "What's the #1 mental barrier you're committed to eliminating this season?", "type": "textarea", "required": True},
    {"id": "commitment_level", "label": "How committed are you to solving this barrier this season?", "type": "slider", "min": 1, "max": 10, "required": True},
    {"id": "full_potential", "label": "If you were performing at your full potential consistently, how would racing feel different?", "type": "textarea", "required": True},
    {"id": "driver_type", "label": "What best describes you?", "type": "select",
     "options": ["Professional Racer", "Dedicated Amateur", "Track Day Enthusiast", "Aspiring Professional"],
     "required": True},
    {"id": "funding_source", "label": "Who funds your racing?", "type": "select",
     "options": ["Self-Funded", "Family-Funded", "Sponsor-Funded", "Mix of Self + Sponsors", "Looking for Sponsorship"],
     "required": True},
    {"id": "financial_ready", "label": "If accepted into Flow Performance, do you have the financial resources to invest in elite-level mental training right now?", "type": "select",
     "options": ["Yes, I'm ready to invest", "I'd need to arrange funding", "I'd need a payment plan", "Not sure yet"],
     "required": True},
    {"id": "effort_willingness", "label": "How willing are you to invest time, focus, and effort into achieving your racing goals?", "type": "slider", "min": 1, "max": 10, "required": True},
    {"id": "seriousness", "label": "On a scale of 1-10, how serious are you about breakthrough performance?", "type": "slider", "min": 1, "max": 10, "required": True},
    {"id": "three_year_vision", "label": "Where do you see yourself 3 years from now?", "type": "textarea", "required": True},
    {"id": "anything_else", "label": "Anything else we should know?", "type": "textarea", "required": False},
]


# =============================================================================
# CHAMPIONSHIP STRATEGY CALL FRAMEWORK (Master Script)
# =============================================================================
CALL_FRAMEWORK = {
    "call_1": {
        "title": "Championship Strategy Call (Push Call)",
        "stages": [
            {
                "stage": 1,
                "name": "Intro & Rapport",
                "script": """Hey {name}, it's Craig from Camino Coaching. How are you?

Great. You scheduled a call with me for about now, is it still a good time to chat?

Fantastic. You went through our training right, the Podium Contenders Blueprint — how did you find it?

Great stuff. Well listen, thanks for your interest in booking a call to explore what I've got and see if it would be of value to you and help you achieve your racing goals.

This first call is to find out where you're at, where you're stuck and what your goals are. What I've found to work best on these calls is to ask you a few questions, get to know your situation better and then I can start making some recommendations if I feel we can help.

Sound good?""",
                "coaching_notes": "Build rapport fast. Mirror their energy. Reference something specific from their application.",
                "data_keys": ["first_name", "championship", "current_level"]
            },
            {
                "stage": 2,
                "name": "The Reverse Frame",
                "script": """Awesome. And just a heads up, you don't need to worry about being sold anything on this call.

If I can speak openly, I only have the capacity to work with a handful of drivers each month and I do have some specific criteria for who I can help.

So even if we really hit it off, if it's ok with you, we'll jump off the line so I can go over my notes from the call, I'll share that with my team and then we'll be back in touch.

So whatever the outcome of this call, we'll both have a chance to go away and think, and we can reconvene in the next few days. Sound ok?""",
                "coaching_notes": "CRITICAL: This removes sales pressure. The prospect must feel they are being interviewed, not sold to. This is key to the 58% conversion rate.",
                "data_keys": []
            },
            {
                "stage": 3,
                "name": "The Detective",
                "script": """Great stuff. That's all sounding very exciting!

I'd love to dive into a few things on your application form if you wouldn't mind?

You mentioned that your goal is {season_goal}. Awesome goal.
- Have you ever achieved that kind of result before?
- How did you come up with that goal?
- Why is it important to you?

You put that you're a {commitment_level}/10 on being committed. What's keeping you from a 10?

Oh and by the way, not a question here, just a quick statement… I noticed on the 'what's your biggest mental barrier' question, you said {mental_barrier} and I LOVE THAT. That's exactly the kind of self-awareness we're looking for.

What are the top 2 or 3 struggles you are facing during a race weekend?

How long has this been holding you back?

What's this costing you emotionally? How does it feel when you're struggling with this?

What have you tried to fix this? What happened?

What does a race weekend cost on average?""",
                "coaching_notes": "DIG DEEP here. The more pain you uncover, the stronger the close. Reference their IMPROVE scores. Their lowest score = biggest lever.",
                "data_keys": ["season_goal", "commitment_level", "mental_barrier", "surprise_score"]
            },
            {
                "stage": 4,
                "name": "Pain Amplification",
                "script": """So what have you tried so far? Have you worked with any other coaches to get this fixed? How did that go?

Have you seen any results at all this year or has it been a constant struggle?

So, what shifted recently to make this a priority?
Why is it important to you?
How is that affecting your confidence?
What are you going to do if you don't get this figured out?
Why not just stay where you are?
Surely you can't keep going like this, right?

If nothing changes, where will you be next season?""",
                "coaching_notes": "This is where you amplify the gap between where they ARE and where they WANT to be. Use their own words back to them. Silence is powerful here — let them sit in the discomfort.",
                "data_keys": ["current_level", "season_goal"]
            },
            {
                "stage": 5,
                "name": "Create Doubt & Education",
                "script": """Can I share something that might be eye-opening?

Most drivers I speak with have been focusing on external solutions - better suspension, stickier tires, more track time. But here's what the research shows about peak performance...

\"This all makes sense to me {name}. You're certainly not the first person to be struggling with all this.

In fact, do you mind if I go off on a little tangent real quick, because it might be helpful for you to know a bit of what's going on behind the scenes and why it's so much harder right now, would that be helpful?\"""",
                "coaching_notes": "Position Flow Performance as the ONLY solution that addresses the root cause. Create doubt about all other approaches they've tried.",
                "data_keys": ["first_name"]
            },
            {
                "stage": 6,
                "name": "Wrap It Up — Summary",
                "script": """\"{name}, I really appreciate you being so honest with me. So here's what I'm hearing...\"

You're feeling... [REMIND THEM OF PAIN]
You've tried... [REMIND THEM OF WHAT HASN'T WORKED]
You need... [REMIND THEM OF THEIR GOAL AND BIGGEST WANTS]

\"Is that right?\"
WAIT FOR A YES

\"Well I can definitely help with this.\"
[INSERT A TESTIMONIAL HERE IF POSSIBLE — \"In fact, you remind me of...\"]

\"Well look, I'd love to lay out a bit of a plan for you and how I can help you achieve {season_goal}, but let me ask you first...\"

\"If I can show you exactly how to fix these issues and help you achieve your goals, how quickly are you wanting to get started on fixing this?\"""",
                "coaching_notes": "Summarise using THEIR words. This is a commitment check. If they say 'immediately' or 'as soon as possible', you're in a strong position.",
                "data_keys": ["first_name", "season_goal"]
            },
            {
                "stage": 7,
                "name": "The Roadmap & 5 Pillars",
                "script": """Based on what you've shared, here's exactly what needs to happen. Grab a pen...

The lowest score from your assessment is your No.1 Priority: {surprise_score}

The 5 pillars needed to hold your performance roof up:

Pillar 1 - MINDSET: Racing Mind is the first module. Personal 1-on-1 coaching calls throughout your season to rewire your approach and eliminate the frustration cycle you're stuck in.

Pillar 2 - PREPARATION: A complete pre-session routine and race weekend structure so you're fast from the out-lap instead of taking 20 minutes to get up to speed.

Pillar 3 - FLOW: 6 comprehensive modules teaching you exactly how to get into flow state in free practice, qualifying, and races - the same mental state where your fastest laps happen effortlessly.

Pillar 4 - FEEDBACK: Our exclusive In The Zone app on your phone works without signal at the track. You feed in details from your previous session, and it tells you exactly what to focus on in the next session to improve.

Pillar 5 - FUNDING: The complete blueprint that enabled Sam Wilford to fund his dream - leaving his job in London, moving to Mallorca, training daily with professional drivers, and competing with AGR for 4 seasons in Junior GP Moto2 Championship.

Where do you want to go from here?""",
                "coaching_notes": "They'll ask about working together. Let THEM ask. If they don't, prompt with 'Where do you want to go from here?'",
                "data_keys": ["surprise_score"]
            },
            {
                "stage": 8,
                "name": "The Push Pitch & Investment",
                "script": """\"Happy to explain how that works. Just remember - I won't be able to offer you anything today. I need to speak with the other drivers first and really think about this. But I can walk you through what working together looks like.\"

*Sell your heart out* - Full program details, success stories, transformation process.

Remember, the 5 pillars needed to hold your performance roof up? Let me break down what you're actually getting in the programme...

On a scale of 1-10 how close do you think that programme would come to fixing your struggles?

You're Struggling with... {mental_barrier}
You've Tried... [what they said]
You Need... A proven process that's going to give you the way to perform consistently at your best to achieve {season_goal}

*The Tiedowns (5 minutes)*

Desire: \"Does this sound like something you'd want to do?\"

Shall we talk about the investment needed to join Flow Performance?

Firstly, can you commit to 20 minutes a day for the training?

Regarding the finances, when I used to work 1-on-1 with drivers, all this would have cost over £9,000 per season. But because we now have a training platform, you don't need to pay for my time, travel, and accommodation.

You get lifetime access to it all for only £4,000.

How does that sound to you?

Investment: \"Looking at the investment, if we had space and we both decide it's a good fit, how would you want to make the payment — on a credit card or would you want to chunk it down into a few payments?\"

I'm not saying there is space yet, but let's explore that for a minute.

Timing: If we both decide it's a good fit, is there anything stopping you from starting immediately?""",
                "coaching_notes": "The 'I can't offer you anything today' line is CRUCIAL. It's the reverse frame that makes them chase you. Never skip this.",
                "data_keys": ["mental_barrier", "season_goal"]
            },
            {
                "stage": 9,
                "name": "Future Pacing & Book Call 2",
                "script": """Let me paint two pictures: 3 months from now you're going to be somewhere and you're going to be someone, the question is who will you be?

Version 1 - No Action Taken:
Things stay the same, you decide the time's not right to get started. £4000 is too much to invest in yourself. 3 months from now: You're still struggling with {mental_barrier}, still frustrated, still finishing not where you want to be. Nowhere near your goal of {season_goal}. You saved 4k but what has it actually cost you?

Or Version 2 - You Invest in Yourself:
3 months from now: You've applied the mental frameworks, you understand why you struggled before, you've had breakthrough weekends. People are taking notice, asking what you've changed. Instead of frustration, you're driving home knowing no one on your bike with your budget on that track in those conditions could have ridden better than you did today. You're excited about your progress.

AND You've secured your first 12k sponsor and you are talking to many more.

In 3 months you're going to be someone — which one do you want to be?

Book Call 2: I need to complete my other interviews. Let's reconnect tomorrow at [time] and I'll let you know my decision.

So {name}, you've got a decision to make in the next 24 hours.

*Is there anything you feel you've missed that I can add to my notes before we head off?*""",
                "coaching_notes": "Book the second call BEFORE hanging up. Get a specific time. Send a calendar invite immediately after.",
                "data_keys": ["first_name", "mental_barrier", "season_goal"]
            },
        ]
    },
    "call_2": {
        "title": "Driver Fit Call (Pull Close - 2nd Call)",
        "stages": [
            {
                "stage": 1,
                "name": "Reconnect & Temperature Check",
                "script": """Hey {name}, nice to speak to you again. How have you been feeling since our last call?

*Good? - move on.*
*Are they worried, not sure or fear has crept in? - work on that FIRST before officially offering them a spot.*""",
                "coaching_notes": "If they're nervous, address it immediately. Fear = buying signal. They're nervous because they WANT it.",
                "data_keys": ["first_name"]
            },
            {
                "stage": 2,
                "name": "Recap & Requalify",
                "script": """Well listen, I've had a good think and I've spoken to the team and I have some good news and bad news.

Before I get into that, would you mind if I quickly recap our last call to make sure we're both on the same page?

Recap #1 - Their situation: You want to {season_goal}
Recap #2 - You're struggling with {mental_barrier}
Recap #3 - The result they want

The £4,000 investment felt manageable.
You're ready to work on the mental side rather than more equipment.

Still accurate?

One question: How coachable are you?

We've found our process works best when drivers follow the steps exactly.

I only like to work with people who are coachable, open to feedback, and ready to take action quickly.

This works best for drivers who are fully committed and follow the program to the T.

Is that you?

What about your commitment level — are you genuinely ready for daily practice?
Your racing, home, and work schedule allows for proper implementation?""",
                "coaching_notes": "The coachability question is a filter AND a commitment device. By saying yes, they're pre-committing to the programme.",
                "data_keys": ["first_name", "season_goal", "mental_barrier"]
            },
            {
                "stage": 3,
                "name": "The Offer — Good News / Bad News",
                "script": """\"Perfect, just wanted to be absolutely sure.\"

\"I've got some good news and some bad news for you. Which would you like first?\"

GOOD NEWS: \"Look, I loved your application and my notes on your situation. Particularly [SPECIFIC THING 1] and [SPECIFIC THING 2].\"
*pause*
\"So I'm very keen to have you in the program and would like to offer you a spot. Congratulations, we're looking forward to working with you on this.\"

BAD NEWS: \"I'm afraid you're going to be stuck with the Camino Coaching family for the next 6 months!\"

\"How are you feeling — excited, nervous, or a bit of both?\"

(Reassure if nervous: \"That's totally normal! Investing in yourself is a big step, but you're in good hands.\")

Here's what happens next: Once we take payment you'll get instant access to the training platform, and we'll book your first kickoff call so you're not left guessing. Most drivers tell me they see changes even in the first week.""",
                "coaching_notes": "The 'bad news' joke breaks tension. Mirror their emotion — if they're excited, be excited. If nervous, be reassuring.",
                "data_keys": ["first_name"]
            },
            {
                "stage": 4,
                "name": "Close & Payment",
                "script": """\"Perfect. I'll get your member area set up and our first call booked. From my notes, you preferred [payment option], so I have that ready.\"

\"When you're ready, I'll take your card details.\"

\"You've made an excellent choice. This is exactly what will transform your racing.\"

Here's what happens next... You will get an email in the next 30 minutes.

Picture 1: Six months from now, you've mastered the mental game. You're the driver setting lap records, enjoying every session, achieving goals you didn't think possible.

Picture 2: Six months of the same struggles, same frustrations, still wondering if you'll ever breakthrough.

Which future do you prefer? Then let's make Picture 1 your reality.""",
                "coaching_notes": "Take payment on the call. Don't let them 'think about it' — that's what Call 1 was for.",
                "data_keys": ["first_name"]
            },
            {
                "stage": 5,
                "name": "Objection Handling",
                "script": """\"Do you have any questions or concerns before we get you set up?\"

If \"I still need to think about it\":
- \"What's changed between our first call and now?\"
- \"What's the real reason you are hesitating?\"
- \"What would you need to feel comfortable moving forward?\"
- \"Would it help to get started with the Starter plan and upgrade later if things go well?\"
- \"Should we set a check-in date to finalise your spot?\"

If it's the logistics of moving money around, take a £500 deposit. Explain that it's company policy.

IF it MUST go to a 3rd call, set it up so there is no way they can stall further:
- \"So it's just X that you need to figure out? There's no other reason that could prevent you joining?\"
- \"What would you like me to add to my notes to share with my team because they were expecting to be welcoming you today.\"""",
                "coaching_notes": "Most objections are smoke screens. 'I need to think about it' usually means 'I'm scared'. Address the fear, not the objection.",
                "data_keys": []
            },
        ]
    }
}


# =============================================================================
# PRE-CALL PREPARATION — Generate Script Overlay
# =============================================================================
def generate_precall_prep(candidate_data, call_number=1):
    """Generate a personalized script overlay from candidate data."""
    is_driver = _detect_discipline(candidate_data)
    call_key = "call_1" if call_number == 1 else "call_2"
    framework = CALL_FRAMEWORK[call_key]

    output_sections = []
    output_sections.append(f"# 🎯 PRE-CALL PREP: {candidate_data.get('first_name', 'Candidate')} {candidate_data.get('last_name', '')}")
    output_sections.append(f"**Call Type:** {framework['title']}")
    output_sections.append(f"**Discipline:** {'🏎️ Driver' if is_driver else '🏍️ Driver'}")
    output_sections.append(f"**Generated:** {datetime.now().strftime('%d %b %Y %H:%M')}")
    output_sections.append("---")

    # Key indicators
    output_sections.append("## 🔑 Key Indicators")
    indicators = _extract_indicators(candidate_data)
    for ind in indicators:
        output_sections.append(f"- {ind}")
    output_sections.append("---")

    # Script with overlays
    for stage_data in framework["stages"]:
        output_sections.append(f"## Stage {stage_data['stage']}: {stage_data['name']}")

        # Insert data-driven insights
        insights = _generate_insights(stage_data, candidate_data)
        if insights:
            for insight in insights:
                output_sections.append(f"> **[>> NOTE: {insight} <<]**")
            output_sections.append("")

        # Script text with variable substitution
        script = stage_data["script"]
        script = _substitute_variables(script, candidate_data)
        if is_driver:
            script = swap_terminology(script, to_driver=True)

        output_sections.append(script)
        output_sections.append("")

        # Coaching notes
        coaching = stage_data["coaching_notes"]
        if is_driver:
            coaching = swap_terminology(coaching, to_driver=True)
        output_sections.append(f"*💡 Coaching: {coaching}*")
        output_sections.append("---")

    return "\n\n".join(output_sections)


def _detect_discipline(data):
    """Detect if candidate is a driver (car) or driver (motorcycle)."""
    text = " ".join(str(v) for v in data.values()).lower()
    driver_keywords = ["car", "driver", "f1", "gt", "btcc", "touring", "formula",
                       "kart", "karting", "wec", "rally", "nascar", "indycar",
                       "porsche", "ferrari", "lambo", "aston martin", "mclaren"]
    driver_keywords = ["bike", "motorcycle", "motorbike", "driver", "motogp", "bsb",
                      "superbike", "moto2", "moto3", "wsbk", "asbk", "supersport"]

    driver_score = sum(1 for k in driver_keywords if k in text)
    driver_score = sum(1 for k in driver_keywords if k in text)

    return driver_score > driver_score


def _extract_indicators(data):
    """Extract key qualification indicators from candidate data."""
    indicators = []

    # Coachability
    commitment = data.get("commitment_level", 0)
    seriousness = data.get("seriousness", 0)
    if isinstance(commitment, str):
        try: commitment = int(commitment)
        except: commitment = 5
    if isinstance(seriousness, str):
        try: seriousness = int(seriousness)
        except: seriousness = 5

    if commitment >= 9:
        indicators.append(f"🟢 **HIGH COACHABILITY**: Commitment {commitment}/10 — 'Ready to do whatever it takes'")
    elif commitment >= 7:
        indicators.append(f"🟡 **MODERATE COACHABILITY**: Commitment {commitment}/10 — Explore what's holding them from a 10")
    else:
        indicators.append(f"🔴 **LOW COACHABILITY**: Commitment {commitment}/10 — May need more warming")

    # Financial qualification
    funding = data.get("funding_source", "")
    financial_ready = data.get("financial_ready", "")
    if "ready to invest" in str(financial_ready).lower():
        indicators.append(f"🟢 **FINANCIALLY QUALIFIED**: {financial_ready}. Funding: {funding}")
    elif "payment plan" in str(financial_ready).lower():
        indicators.append(f"🟡 **NEEDS PAYMENT PLAN**: {financial_ready}. Funding: {funding}")
    else:
        indicators.append(f"🔴 **FINANCIAL CONCERN**: {financial_ready}. Funding: {funding}")

    # Pain gap
    current = data.get("current_level", "Unknown")
    goal = data.get("season_goal", "Unknown")
    indicators.append(f"📊 **PAIN GAP**: Currently at '{current}' → Goal: '{goal}'")

    # Mental barrier
    barrier = data.get("mental_barrier", "")
    if barrier:
        indicators.append(f"🧠 **PRIMARY BARRIER**: {barrier}")

    # Age check
    age = data.get("age", "")
    if age:
        try:
            age_int = int(age)
            if age_int < 18:
                indicators.append(f"⚠️ **UNDER 18**: Age {age} — Parents must be on call")
        except:
            pass

    return indicators


def _generate_insights(stage_data, candidate_data):
    """Generate data-driven insights for a specific script stage."""
    insights = []
    keys = stage_data.get("data_keys", [])

    if "surprise_score" in keys and candidate_data.get("surprise_score"):
        insights.append(f"Candidate's most surprising IMPROVE score: '{candidate_data['surprise_score']}'. Use this as the primary pain lever.")

    if "mental_barrier" in keys and candidate_data.get("mental_barrier"):
        insights.append(f"Primary mental barrier: '{candidate_data['mental_barrier']}'. Reference this explicitly.")

    if "commitment_level" in keys:
        level = candidate_data.get("commitment_level", 5)
        if isinstance(level, str):
            try: level = int(level)
            except: level = 5
        if level < 10:
            insights.append(f"Commitment is {level}/10. Ask: 'What's keeping you from a 10?' — this reveals hidden objections.")

    if "funding_source" in keys or "financial_ready" in keys:
        funding = candidate_data.get("funding_source", "")
        if "sponsor" in str(funding).lower():
            insights.append(f"Candidate is {funding}. Mention the 'Sponsorship Mastery' pillar (Pillar 5) here.")
        if "family" in str(funding).lower():
            insights.append(f"Candidate is Family-Funded. Parents may need to be included in decision. Prepare for 'I need to ask my parents' objection.")

    if "season_goal" in keys and candidate_data.get("season_goal"):
        insights.append(f"Season goal: '{candidate_data['season_goal']}'. Use this in future pacing.")

    if "current_level" in keys and candidate_data.get("current_level"):
        insights.append(f"Current performance level: '{candidate_data['current_level']}'. Amplify the gap vs their goal.")

    return insights


def _substitute_variables(script, data):
    """Replace {variable} placeholders in script with candidate data."""
    replacements = {
        "{name}": data.get("first_name", "[NAME]"),
        "{first_name}": data.get("first_name", "[NAME]"),
        "{season_goal}": data.get("season_goal", "[THEIR GOAL]"),
        "{commitment_level}": str(data.get("commitment_level", "[X]")),
        "{mental_barrier}": data.get("mental_barrier", "[THEIR BARRIER]"),
        "{surprise_score}": data.get("surprise_score", "[THEIR LOWEST SCORE]"),
        "{current_level}": data.get("current_level", "[CURRENT LEVEL]"),
        "{championship}": data.get("championship", "[CHAMPIONSHIP]"),
    }
    result = script
    for key, value in replacements.items():
        result = result.replace(key, str(value))
    return result


# =============================================================================
# POST-CALL ANALYSIS — Score the call
# =============================================================================
ANALYSIS_CRITERIA = {
    "adherence": {
        "label": "Script Adherence",
        "description": "Did Craig follow the Championship Strategy Call Framework flow?",
        "max_score": 10,
        "checkpoints": [
            "Reverse Frame delivered (Stage 2)",
            "Detective questions asked with depth (Stage 3)",
            "Pain amplification — dug into emotional cost (Stage 4)",
            "5 Pillars presented clearly (Stage 7)",
            "Investment discussed with 'I can't offer today' frame (Stage 8)",
            "Future pacing — 2 pictures painted (Stage 9)",
            "Call 2 booked before hanging up",
        ]
    },
    "pain_amplification": {
        "label": "Pain Amplification",
        "description": "Did Craig dig deep enough into the 7 Mistakes and emotional cost?",
        "max_score": 10,
        "checkpoints": [
            "Referenced specific IMPROVE assessment scores",
            "Asked 'how long has this been holding you back?'",
            "Asked 'what's this costing you emotionally?'",
            "Asked 'what have you tried to fix this?'",
            "Asked 'if nothing changes, where will you be next season?'",
            "Used silence effectively after pain questions",
        ]
    },
    "push_pull": {
        "label": "Push/Pull Execution",
        "description": "Did Craig effectively use the Reverse Frame and the Takeaway?",
        "max_score": 10,
        "checkpoints": [
            "Reverse Frame: 'I can only work with a handful' (Stage 2)",
            "Takeaway: 'I won't be able to offer you anything today' (Stage 8)",
            "Created scarcity without being pushy",
            "Let the prospect ASK about the programme",
            "Good News / Bad News delivery (Call 2)",
        ]
    },
    "objection_handling": {
        "label": "Objection Handling",
        "description": "How effectively were doubts and objections addressed?",
        "max_score": 10,
        "checkpoints": [
            "Addressed money objections with payment plan",
            "Addressed 'need to think about it' with 'what's changed?'",
            "Used testimonials/case studies to handle doubt",
            "Kept pointing back to their stated pain and goals",
            "Maintained frame — didn't get desperate",
        ]
    },
}


def score_call_section(criteria_key, score, notes=""):
    """Create a score entry for a call analysis section."""
    return {
        "criteria": criteria_key,
        "score": score,
        "notes": notes,
        "max": ANALYSIS_CRITERIA[criteria_key]["max_score"]
    }


def generate_analysis_report(scores, call_notes=""):
    """Generate a formatted post-call analysis report."""
    total_score = sum(s["score"] for s in scores)
    max_score = sum(s["max"] for s in scores)
    pct = (total_score / max_score * 100) if max_score > 0 else 0

    report = []
    report.append(f"# 📊 Post-Call Analysis Report")
    report.append(f"**Overall Score: {total_score}/{max_score} ({pct:.0f}%)**")
    report.append(f"**Generated:** {datetime.now().strftime('%d %b %Y %H:%M')}")

    if pct >= 80:
        report.append("**Rating: 🟢 EXCELLENT** — Strong adherence to the framework")
    elif pct >= 60:
        report.append("**Rating: 🟡 GOOD** — Some missed opportunities")
    else:
        report.append("**Rating: 🔴 NEEDS WORK** — Significant deviation from framework")

    report.append("---")

    # Individual scores
    for s in scores:
        criteria = ANALYSIS_CRITERIA[s["criteria"]]
        bar = "█" * s["score"] + "░" * (s["max"] - s["score"])
        report.append(f"### {criteria['label']}: {s['score']}/{s['max']}")
        report.append(f"`{bar}`")
        report.append(f"*{criteria['description']}*")
        if s.get("notes"):
            report.append(f"\n{s['notes']}")
        report.append("")

    # Call notes
    if call_notes:
        report.append("---")
        report.append("### 📝 Additional Notes")
        report.append(call_notes)

    return "\n\n".join(report)


# =============================================================================
# GOLD STANDARD CALLS — Reference transcripts for comparison
# =============================================================================
_GOLD_DATA_PATH = "data/gold_standard_calls.json"

def load_gold_standard():
    """Load gold standard call transcripts from disk."""
    import os
    path = os.path.join(os.path.dirname(__file__), _GOLD_DATA_PATH)
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def get_gold_standard_list():
    """Return a list of available gold standard calls for the UI."""
    data = load_gold_standard()
    calls = []
    for key, entry in data.items():
        for call_key in ["call_1", "call_2"]:
            if call_key in entry:
                calls.append({
                    "id": f"{key}_{call_key}",
                    "label": f"{entry['name']} — {entry[call_key]['title']} ({entry[call_key]['duration']})",
                    "name": entry["name"],
                    "discipline": entry["discipline"],
                    "outcome": entry["outcome"],
                    "notes": entry["notes"],
                    "call_key": call_key,
                    "entry_key": key,
                })
    return calls


def get_gold_transcript(entry_key, call_key):
    """Return transcript text for a specific gold standard call."""
    data = load_gold_standard()
    entry = data.get(entry_key, {})
    call = entry.get(call_key, {})
    return call.get("transcript", "")


def compare_to_gold_standard(uploaded_transcript, gold_entry_key, gold_call_key):
    """Compare an uploaded transcript against a gold standard call.

    Returns a structured comparison with:
    - Framework stage detection in both transcripts
    - Key phrase matching
    - Technique identification
    """
    gold_text = get_gold_transcript(gold_entry_key, gold_call_key)
    if not gold_text:
        return None

    # Key phrases/techniques to look for in both transcripts
    framework_markers = {
        "Reverse Frame": [
            "don't need to worry about being sold",
            "handful of",
            "capacity to work with",
            "we'll both have a chance to go away and think",
            "won't make a decision on it today",
        ],
        "Detective Questions": [
            "what's your goal",
            "what are your struggles",
            "how long has this been holding you back",
            "what have you tried",
            "why is it important",
        ],
        "Pain Amplification": [
            "what's this costing you",
            "how does it feel",
            "if nothing changes",
            "surely you can't keep going",
            "what are you going to do if",
        ],
        "5 Pillars": [
            "pillar",
            "mindset",
            "preparation",
            "flow state",
            "feedback",
            "funding",
            "sponsorship",
        ],
        "Takeaway / Push": [
            "won't be able to offer you anything today",
            "I can't offer",
            "need to speak with the other",
            "I have some specific criteria",
        ],
        "Future Pacing": [
            "3 months from now",
            "two pictures",
            "version 1",
            "version 2",
            "no action taken",
            "invest in yourself",
        ],
        "Investment Discussion": [
            "£4,000",
            "4000",
            "investment",
            "payment plan",
            "credit card",
            "lifetime access",
        ],
        "Tiedowns": [
            "does this sound like something you'd want",
            "how does that sound",
            "scale of 1",
            "1 to 10",
            "commit to 20 minutes",
        ],
        "Good News / Bad News": [
            "good news and bad news",
            "good news and some bad",
            "offer you a spot",
            "congratulations",
        ],
        "Coachability Check": [
            "how coachable are you",
            "follow the steps exactly",
            "follow the program",
            "committed to taking action",
        ],
    }

    results = {}
    uploaded_lower = uploaded_transcript.lower()
    gold_lower = gold_text.lower()

    for technique, phrases in framework_markers.items():
        uploaded_hits = []
        gold_hits = []
        for phrase in phrases:
            if phrase.lower() in uploaded_lower:
                uploaded_hits.append(phrase)
            if phrase.lower() in gold_lower:
                gold_hits.append(phrase)

        results[technique] = {
            "uploaded_found": len(uploaded_hits),
            "uploaded_phrases": uploaded_hits,
            "gold_found": len(gold_hits),
            "gold_phrases": gold_hits,
            "total_markers": len(phrases),
            "match": "✅" if uploaded_hits else "❌",
        }

    return results


def format_comparison_report(comparison, gold_label):
    """Format a comparison result into a readable report."""
    if not comparison:
        return "No comparison data available."

    lines = []
    lines.append(f"## 🏆 Gold Standard Comparison: {gold_label}")
    lines.append("")

    total_found = 0
    total_possible = 0

    for technique, data in comparison.items():
        total_found += data["uploaded_found"]
        total_possible += data["total_markers"]

        status = data["match"]
        uploaded_pct = (data["uploaded_found"] / data["total_markers"] * 100) if data["total_markers"] > 0 else 0
        gold_pct = (data["gold_found"] / data["total_markers"] * 100) if data["total_markers"] > 0 else 0

        lines.append(f"### {status} {technique}")
        lines.append(f"- **Your call:** {data['uploaded_found']}/{data['total_markers']} markers detected ({uploaded_pct:.0f}%)")
        lines.append(f"- **Gold standard:** {data['gold_found']}/{data['total_markers']} markers ({gold_pct:.0f}%)")

        if data["uploaded_phrases"]:
            lines.append(f"  - ✅ Used: {', '.join(data['uploaded_phrases'][:3])}")

        missing = [p for p in data["gold_phrases"] if p not in data["uploaded_phrases"]]
        if missing:
            lines.append(f"  - ❌ Missing: {', '.join(missing[:3])}")

        lines.append("")

    overall_pct = (total_found / total_possible * 100) if total_possible > 0 else 0
    lines.insert(1, f"**Overall Match: {total_found}/{total_possible} ({overall_pct:.0f}%)**")
    lines.insert(2, "")

    return "\n".join(lines)
