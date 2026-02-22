#!/usr/bin/env python3
"""
Camino Coaching Funnel Manager
==============================
A comprehensive sales funnel management tool for tracking drivers through
the Podium Contenders Blueprint program.

Target: £15,000 monthly revenue
Programme Price: £4,000 (with payment plan options)

Funnel Stages:
- Phase 1: Outreach (Email, Facebook DM, Instagram DM)
- Phase 2: Registration for 3-day free training + Day 1 Assessment
- Phase 3: Day 2 Self-Assessment + Day 3 Strategy Call Booking

Author: Camino Coaching
"""

import csv
import json
import random
import threading
import pandas as pd
import os
import os
import streamlit as st
# import gsheets_loader (Removed)

from airtable_manager import AirtableManager
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from collections import defaultdict
import re



# =============================================================================
# CONFIGURATION
# =============================================================================
print("Loading Funnel Manager module...") # Debug for Streamlit Cloud logs

class FunnelConfig:
    """Central configuration for funnel targets and rates"""

    # Revenue targets
    MONTHLY_REVENUE_TARGET = 15000  # £15,000
    PROGRAMME_PRICE = 4000  # £4,000 full price

    # Average effective price (accounting for payment plans)
    # This can be adjusted based on actual payment plan mix
    AVERAGE_DEAL_VALUE = 4000

    # Working days
    WORKING_DAYS_PER_WEEK = 5
    WEEKS_PER_MONTH = 4

    # Historical conversion rates (to be calibrated with actual data)
    # These are starting estimates - the system will learn from your data
    DEFAULT_CONVERSION_RATES = {
        'outreach_to_registration': 0.08,      # 8% of outreach becomes registrations
        'registration_to_day1': 0.70,          # 70% complete Day 1 assessment
        'day1_to_day2': 0.60,                  # 60% complete Day 2 assessment
        'day2_to_strategy_call': 0.40,         # 40% book strategy call
        'strategy_call_to_sale': 0.25,         # 25% of calls convert to sale
    }

    # Rescue message timing (hours after drop-off)
    RESCUE_TIMING = {
        'registration_no_day1': 24,     # 24 hours after registration
        'day1_no_day2': 24,             # 24 hours after Day 1
        'day2_no_call': 12,             # 12 hours after Day 2 (urgency)
    }


class OutreachChannel(Enum):
    EMAIL = "email"
    FACEBOOK_DM = "facebook_dm"
    INSTAGRAM_DM = "instagram_dm"


class FunnelStage(Enum):
    CONTACT = "Contact"
    MESSAGED = "Messaged"
    REPLIED = "Replied"
    RACE_WEEKEND = "Race Weekend"
    LINK_SENT = "Link Sent"
    BLUEPRINT_STARTED = "Podium Contenders Blueprint Started"
    DAY1_COMPLETE = "Day 1 Completed"
    DAY2_COMPLETE = "Day 2 Completed"
    STRATEGY_CALL_BOOKED = "Strategy Call Booked"
    CLIENT = "Client"
    NOT_A_FIT = "Not a good fit"
    DOES_NOT_REPLY = "Does Not Reply"
    FOLLOW_UP = "Follow up"
    FLOW_PROFILE_COMPLETED = "Flow Profile Completed"
    MINDSET_QUIZ_COMPLETED = "Mindset Quiz Completed"
    SLEEP_TEST_COMPLETED = "Sleep Test Completed"
    NO_SOCIALS = "No Socials Found"
    
    # Legacy / Compatibility Aliases
    OUTREACH = "Messaged"
    REGISTERED = "Podium Contenders Blueprint Started"
    SALE_CLOSED = "Client"
    NO_SALE = "Not a good fit"
    
    # Old ones kept for CSV capability until migrated
    RACE_REVIEW_COMPLETE = "Race Weekend Review Completed"
    SEASON_REVIEW_SENT = "End of Season Review Link Sent"
    SEASON_REVIEW_COMPLETE = "End of Season Review Completed"
    BLUEPRINT_LINK_SENT = "Blueprint Link Sent"



# =============================================================================
# NICKNAME MAP — Module-level constant for identity deduplication
# Used by _get_or_create_driver, _deduplicate_drivers, and match_driver
# Maps short names ↔ full name equivalents for 4-way identity checks
# (first name, last name, nickname, championship)
# =============================================================================
NICKNAME_MAP = {
    'chris': 'christopher', 'christopher': 'chris',
    'mike': 'michael', 'michael': 'mike',
    'matt': 'matthew', 'matthew': 'matt',
    'dan': 'daniel', 'daniel': 'dan',
    'dave': 'david', 'david': 'dave',
    'rob': 'robert', 'robert': 'rob',
    'bob': 'robert',
    'bill': 'william', 'william': 'bill',
    'will': 'william',
    'jim': 'james', 'james': 'jim',
    'joe': 'joseph', 'joseph': 'joe',
    'tom': 'thomas', 'thomas': 'tom',
    'tony': 'anthony', 'anthony': 'tony',
    'ed': 'edward', 'edward': 'ed',
    'eddie': 'edward',
    'nick': 'nicholas', 'nicholas': 'nick',
    'ben': 'benjamin', 'benjamin': 'ben',
    'alex': 'alexander', 'alexander': 'alex',
    'jon': 'jonathan', 'jonathan': 'jon',
    'josh': 'joshua', 'joshua': 'josh',
    'sam': 'samuel', 'samuel': 'sam',
    'steve': 'stephen', 'stephen': 'steve',
    'andy': 'andrew', 'andrew': 'andy',
    'greg': 'gregory', 'gregory': 'greg',
    'jeff': 'jeffrey', 'jeffrey': 'jeff',
    'pat': 'patrick', 'patrick': 'pat',
    'rick': 'richard', 'richard': 'rick',
    'dick': 'richard',
    'ted': 'theodore', 'theodore': 'ted',
    'tim': 'timothy', 'timothy': 'tim',
    'pete': 'peter', 'peter': 'pete',
    'charlie': 'charles', 'charles': 'charlie',
    'larry': 'lawrence', 'lawrence': 'larry',
    'doug': 'douglas', 'douglas': 'doug',
    'ray': 'raymond', 'raymond': 'ray',
    'al': 'alan', 'alan': 'al',
    'manny': 'manuel', 'manuel': 'manny',
    'lenny': 'leonard', 'leonard': 'lenny',
    'leo': 'leonard',
}

def _names_match_via_nickname(first_a: str, first_b: str) -> bool:
    """Return True if two first names are the same person via nickname.

    Checks: exact match, one is a substring-prefix of the other (e.g.
    'Chris' is a prefix of 'Christopher'), or they map to each other
    through the NICKNAME_MAP.
    """
    a = first_a.lower().strip()
    b = first_b.lower().strip()
    if not a or not b:
        return False
    if a == b:
        return True
    # Prefix check: "Chris" startswith "Chris"topher or vice versa
    if a.startswith(b) or b.startswith(a):
        return True
    # Nickname map lookup
    if NICKNAME_MAP.get(a) == b or NICKNAME_MAP.get(b) == a:
        return True
    # Both map to the same canonical name
    if NICKNAME_MAP.get(a) and NICKNAME_MAP.get(a) == NICKNAME_MAP.get(b):
        return True
    return False


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class Driver:
    """Represents a driver in the funnel"""
    email: str
    first_name: str
    last_name: str
    phone: Optional[str] = None

    # Raw Full Name from Airtable (may differ from computed first+last)
    raw_full_name: Optional[str] = None

    # Preferred name for DMs — the name they use on social media
    # e.g. "Chris" when formal first_name is "Christopher"
    # Set automatically by nickname dedup. Used in outreach messages.
    preferred_name: Optional[str] = None

    # Funnel tracking
    outreach_channel: Optional[OutreachChannel] = None
    outreach_date: Optional[datetime] = None
    last_activity: Optional[datetime] = None # For sorting "Latest Activity"

    # Funnel progress
    current_stage: FunnelStage = FunnelStage.CONTACT
    
    # Dates
    registered_date: Optional[datetime] = None
    day1_complete_date: Optional[datetime] = None
    day2_complete_date: Optional[datetime] = None
    strategy_call_booked_date: Optional[datetime] = None
    strategy_call_complete_date: Optional[datetime] = None
    sale_closed_date: Optional[datetime] = None
    
    # Granular Outreach Tracking
    replied_date: Optional[datetime] = None
    link_sent_date: Optional[datetime] = None
    
    # Social Links
    facebook_url: Optional[str] = None
    instagram_url: Optional[str] = None
    linkedin_url: Optional[str] = None
    championship: Optional[str] = None
    
    # Review Dates & Status
    race_weekend_review_date: Optional[datetime] = None
    race_weekend_review_status: str = "pending" # pending, completed
    end_of_season_review_date: Optional[datetime] = None
    # Scores
    day1_score: Optional[float] = None
    day2_scores: Optional[Dict[str, float]] = None
    
    # Flow Profile (Lead Magnet)
    flow_profile_date: Optional[datetime] = None
    flow_profile_score: Optional[float] = None
    flow_profile_result: Optional[str] = None
    flow_profile_url: Optional[str] = None

    # Sleep Test (Lead Magnet)
    sleep_test_date: Optional[datetime] = None
    sleep_score: Optional[float] = None
    
    # Mindset Quiz (Lead Magnet)
    mindset_quiz_date: Optional[datetime] = None
    mindset_score: Optional[float] = None
    mindset_result: Optional[str] = None # e.g. "Fixed Mindset", "Growth Mindset"

    # Rescue tracking
    rescue_messages_sent: List[str] = field(default_factory=list)
    last_rescue_date: Optional[datetime] = None

    # Enhanced CRM Fields
    tags: Optional[str] = None
    championship: Optional[str] = None
    notes: Optional[str] = None
    follow_up_date: Optional[datetime] = None
    is_disqualified: bool = False
    disqualification_reason: Optional[str] = None
    sale_value: Optional[float] = None
    in_gsheet_input: bool = False # Legacy flag (kept for compatibility)



    payment_plan: bool = False
    monthly_payment: Optional[float] = None

    # Metadata
    country: Optional[str] = None
    driver_type: Optional[str] = None
    championship: Optional[str] = None
    airtable_record_id: Optional[str] = None  # Track Airtable record ID for direct updates

    @property
    def display_name(self) -> str:
        """Name to use in outreach messages — preferred (social media) name first."""
        return self.preferred_name or self.first_name or (self.full_name.split()[0] if self.full_name else "mate")

    @property
    def full_name(self) -> str:
        name = f"{self.first_name} {self.last_name}".strip()
        if name:
            return name
        # Try raw_full_name from Airtable
        if self.raw_full_name:
            return self.raw_full_name
        # Derive readable name from slug email (no_email_daryl_hutt → Daryl Hutt)
        if self.email and self.email.startswith("no_email_"):
            slug = self.email.replace("no_email_", "")
            return slug.replace("_", " ").title()
        # Derive from email prefix (john.smith@gmail.com → John Smith)
        if self.email and '@' in self.email:
            prefix = self.email.split('@')[0]
            clean = prefix.replace('.', ' ').replace('_', ' ').replace('-', ' ')
            parts = clean.split()
            if len(parts) >= 2 and all(p.isalpha() for p in parts):
                return ' '.join(p.title() for p in parts)
        return self.email or "Unknown"

    def _naive(self, dt: Optional[datetime]) -> Optional[datetime]:
        if isinstance(dt, datetime):
            if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
                return dt.astimezone().replace(tzinfo=None)
            return dt
        return None

    @property
    def days_in_current_stage(self) -> int:
        """Calculate days since entering current stage"""
        stage_dates = {
            FunnelStage.OUTREACH: self.outreach_date,
            FunnelStage.MESSAGED: self.outreach_date,
            FunnelStage.REPLIED: self.replied_date,
            FunnelStage.LINK_SENT: self.link_sent_date,
            FunnelStage.BLUEPRINT_LINK_SENT: self.link_sent_date,
            FunnelStage.RACE_WEEKEND: self.race_weekend_review_date,
            FunnelStage.RACE_REVIEW_COMPLETE: self.race_weekend_review_date,
            FunnelStage.REGISTERED: self.registered_date,
            FunnelStage.BLUEPRINT_STARTED: self.registered_date,
            FunnelStage.DAY1_COMPLETE: self.day1_complete_date,
            FunnelStage.DAY2_COMPLETE: self.day2_complete_date,
            FunnelStage.STRATEGY_CALL_BOOKED: self.strategy_call_booked_date,
            FunnelStage.CLIENT: self.sale_closed_date,
            FunnelStage.SALE_CLOSED: self.sale_closed_date,
        }

        stage_date = self._naive(stage_dates.get(self.current_stage))
        if stage_date:
            return (datetime.now() - stage_date).days
        return 0

    def needs_rescue(self, config: FunnelConfig = FunnelConfig()) -> Tuple[bool, str]:
        """Check if driver needs a rescue message"""
        now = datetime.now()

        # Registered but no Day 1
        if self.current_stage == FunnelStage.REGISTERED and self.registered_date:
            reg_date = self._naive(self.registered_date)
            if not reg_date:
                return False, ''
            hours_since = (now - reg_date).total_seconds() / 3600
            if hours_since >= config.RESCUE_TIMING['registration_no_day1']:
                if 'day1_rescue' not in self.rescue_messages_sent:
                    return True, 'day1_rescue'

        # Day 1 complete but no Day 2
        if self.current_stage == FunnelStage.DAY1_COMPLETE and self.day1_complete_date:
            day1_date = self._naive(self.day1_complete_date)
            if not day1_date:
                return False, ''
            hours_since = (now - day1_date).total_seconds() / 3600
            if hours_since >= config.RESCUE_TIMING['day1_no_day2']:
                if 'day2_rescue' not in self.rescue_messages_sent:
                    return True, 'day2_rescue'

        # Day 2 complete but no strategy call
        if self.current_stage == FunnelStage.DAY2_COMPLETE and self.day2_complete_date:
            day2_date = self._naive(self.day2_complete_date)
            if not day2_date:
                return False, ''
            hours_since = (now - day2_date).total_seconds() / 3600
            if hours_since >= config.RESCUE_TIMING['day2_no_call']:
                if 'strategy_call_rescue' not in self.rescue_messages_sent:
                    return True, 'strategy_call_rescue'

        return False, ''


@dataclass
class DailyMetrics:
    """Daily funnel metrics"""
    date: datetime

    # Outreach counts by channel
    outreach_email: int = 0
    outreach_facebook: int = 0
    outreach_instagram: int = 0

    # Funnel progression
    new_registrations: int = 0
    day1_completions: int = 0
    day2_completions: int = 0
    strategy_calls_booked: int = 0
    strategy_calls_completed: int = 0
    sales_closed: int = 0

    # Revenue
    revenue_closed: float = 0.0
    monthly_recurring_added: float = 0.0

    # Rescue activity
    rescue_messages_sent: int = 0
    rescue_conversions: int = 0

    @property
    def total_outreach(self) -> int:
        return self.outreach_email + self.outreach_facebook + self.outreach_instagram

    @property
    def outreach_to_registration_rate(self) -> float:
        if self.total_outreach == 0:
            return 0.0
        return self.new_registrations / self.total_outreach


@dataclass
class FunnelTargets:
    """Weekly and monthly targets"""

    # Monthly targets
    monthly_revenue: float
    monthly_sales: int
    monthly_strategy_calls: int
    monthly_day2_completions: int
    monthly_day1_completions: int
    monthly_registrations: int
    monthly_outreach: int

    # Weekly targets (monthly / 4)
    weekly_revenue: float = 0
    weekly_sales: int = 0
    weekly_strategy_calls: int = 0
    weekly_day2_completions: int = 0
    weekly_day1_completions: int = 0
    weekly_registrations: int = 0
    weekly_outreach: int = 0

    # Daily targets (weekly / 5 working days)
    daily_outreach: int = 0

    def __post_init__(self):
        self.weekly_revenue = self.monthly_revenue / 4
        self.weekly_sales = max(1, self.monthly_sales // 4)
        self.weekly_strategy_calls = max(1, self.monthly_strategy_calls // 4)
        self.weekly_day2_completions = max(1, self.monthly_day2_completions // 4)
        self.weekly_day1_completions = max(1, self.monthly_day1_completions // 4)
        self.weekly_registrations = max(1, self.monthly_registrations // 4)
        self.weekly_outreach = max(1, self.monthly_outreach // 4)
        self.daily_outreach = max(1, self.weekly_outreach // 5)


# =============================================================================
# FUNNEL CALCULATOR
# =============================================================================

class FunnelCalculator:
    """Calculate required activities to hit revenue targets"""

    def __init__(self, config: FunnelConfig = None):
        self.config = config or FunnelConfig()
        self.conversion_rates = dict(self.config.DEFAULT_CONVERSION_RATES)

    def update_conversion_rates(self, rates: Dict[str, float]):
        """Update conversion rates based on actual data"""
        self.conversion_rates.update(rates)

    def calculate_targets(self,
                         monthly_revenue_target: float = None,
                         average_deal_value: float = None) -> FunnelTargets:
        """
        Work backwards from revenue target to calculate required activities.

        Revenue Target → Sales Needed → Strategy Calls → Day 2 → Day 1 → Registrations → Outreach
        """
        revenue_target = monthly_revenue_target or self.config.MONTHLY_REVENUE_TARGET
        deal_value = average_deal_value or self.config.AVERAGE_DEAL_VALUE

        # Work backwards through the funnel
        sales_needed = int(revenue_target / deal_value) + 1

        strategy_calls_needed = int(
            sales_needed / self.conversion_rates['strategy_call_to_sale']
        ) + 1

        day2_needed = int(
            strategy_calls_needed / self.conversion_rates['day2_to_strategy_call']
        ) + 1

        day1_needed = int(
            day2_needed / self.conversion_rates['day1_to_day2']
        ) + 1

        registrations_needed = int(
            day1_needed / self.conversion_rates['registration_to_day1']
        ) + 1

        outreach_needed = int(
            registrations_needed / self.conversion_rates['outreach_to_registration']
        ) + 1

        return FunnelTargets(
            monthly_revenue=revenue_target,
            monthly_sales=sales_needed,
            monthly_strategy_calls=strategy_calls_needed,
            monthly_day2_completions=day2_needed,
            monthly_day1_completions=day1_needed,
            monthly_registrations=registrations_needed,
            monthly_outreach=outreach_needed
        )

    def forecast_revenue(self,
                        current_outreach: int,
                        current_registrations: int,
                        current_day1: int,
                        current_day2: int,
                        current_calls: int) -> Dict[str, float]:
        """
        Forecast expected revenue based on current funnel state.
        """
        # Project forward through funnel
        projected_registrations = current_outreach * self.conversion_rates['outreach_to_registration']
        projected_day1 = (current_registrations + projected_registrations) * self.conversion_rates['registration_to_day1']
        projected_day2 = (current_day1 + projected_day1) * self.conversion_rates['day1_to_day2']
        projected_calls = (current_day2 + projected_day2) * self.conversion_rates['day2_to_strategy_call']
        projected_sales = (current_calls + projected_calls) * self.conversion_rates['strategy_call_to_sale']
        projected_revenue = projected_sales * self.config.AVERAGE_DEAL_VALUE

        return {
            'projected_registrations': projected_registrations,
            'projected_day1': projected_day1,
            'projected_day2': projected_day2,
            'projected_calls': projected_calls,
            'projected_sales': projected_sales,
            'projected_revenue': projected_revenue
        }


# =============================================================================
# RESCUE MESSAGE SYSTEM
# =============================================================================

class RescueMessageManager:
    """Manages rescue messages for drivers who have dropped out"""

    TEMPLATES = {
        'day1_rescue': {
            'subject': "You started something amazing - let's not leave it unfinished",
            'email': """Hi {first_name},

I noticed you registered for the Podium Contenders Blueprint but haven't completed Day 1's training yet - "The 7 Biggest Mental Mistakes Costing You Lap Time".

Look, I get it. Life gets busy. Racing prep takes priority.

But here's the thing - this 20-minute assessment could be the most valuable thing you do for your racing this week.

Why? Because you can't fix what you can't see.

The drivers who've gone through this tell me they finally understand WHY they've been leaving time on the table. And that clarity? It's the first step to unlocking your real potential.

Your spot is still waiting: [LINK]

See you on the other side,
{coach_name}

P.S. The assessment reveals your score across all 7 mental mistake categories. Most drivers are shocked by what they discover about themselves.""",
            'dm': """Hey {first_name}! 👋

Noticed you signed up for the Podium Contenders Blueprint but haven't done Day 1 yet.

The 7 Biggest Mistakes assessment only takes 20 mins and drivers are telling me it's been a game-changer for understanding where they're leaving time on track.

Your link's still active - want me to resend it?

Let me know if you have any questions!"""
        },

        'day2_rescue': {
            'subject': "Day 2 unlocks your racing potential - don't stop now",
            'email': """Hi {first_name},

You crushed Day 1 of the Podium Contenders Blueprint. Your 7 Biggest Mistakes assessment revealed some powerful insights about your mental game.

But here's the thing - Day 1 shows you the PROBLEM. Day 2 shows you the SOLUTION.

The 5-Pillar Self-Assessment takes what you learned yesterday and maps out exactly where to focus your energy for maximum improvement.

Without it, you've got half the picture.

Don't leave your breakthrough incomplete: [LINK]

This won't take long, and the clarity you'll get is worth every minute.

Talk soon,
{coach_name}

P.S. Drivers who complete both assessments before their Strategy Call see 3x better results in their first month of training. Just saying... 🏁""",
            'dm': """Hey {first_name}!

Loved seeing your Day 1 results - some really interesting patterns there.

Day 2's 5-Pillar Assessment is where it all comes together though. It shows you exactly which areas will give you the biggest gains.

Takes about 15 mins - you ready to dive in?

Here's your link: [LINK]"""
        },

        'strategy_call_rescue': {
            'subject': "Your Strategy Call spot is waiting (but not for long)",
            'email': """Hi {first_name},

You've done the work. You completed both assessments. You KNOW where your mental game needs attention.

But knowledge without action? That's just entertainment.

The Strategy Call is where we turn your insights into a real plan. Where we look at your specific situation, your goals, and map out exactly how to get there.

I've got a few spots open this week: [BOOKING LINK]

This isn't a sales pitch. It's a genuine conversation about your racing and whether we're a good fit to work together.

If we are? Great. If not? You'll still walk away with actionable insights you can use immediately.

But you've got to book the call to find out.

Ready when you are,
{coach_name}

P.S. These spots fill up fast. If you're serious about transforming your racing this season, don't wait.""",
            'dm': """Hey {first_name}!

You've done Day 1 AND Day 2 - that's awesome! You're clearly serious about this.

The next step is a Strategy Call where we look at your results together and figure out the best path forward for you.

No pressure, no hard sell - just a real conversation about your racing goals.

I've got some spots open - shall I send the booking link?"""
        }
    }

    def __init__(self):
        self.coach_name = "Camino"  # Configure this

    def get_rescue_message(self,
                          rescue_type: str,
                          driver: Driver,
                          channel: str = 'email') -> Dict[str, str]:
        """Generate a personalized rescue message"""
        template = self.TEMPLATES.get(rescue_type, {})

        if channel == 'email':
            return {
                'subject': template.get('subject', '').format(
                    first_name=driver.first_name
                ),
                'body': template.get('email', '').format(
                    first_name=driver.first_name,
                    coach_name=self.coach_name
                )
            }
        else:
            return {
                'body': template.get('dm', '').format(
                    first_name=driver.first_name,
                    coach_name=self.coach_name
                )
            }

    def get_drivers_needing_rescue(self, drivers: List[Driver]) -> Dict[str, List[Driver]]:
        """Identify all drivers needing rescue messages"""
        rescue_needed = {
            'day1_rescue': [],
            'day2_rescue': [],
            'strategy_call_rescue': []
        }

        for driver in drivers:
            needs_rescue, rescue_type = driver.needs_rescue()
            if needs_rescue:
                rescue_needed[rescue_type].append(driver)

        return rescue_needed


class FollowUpMessageManager:
    """Generates next-step messages based on funnel state"""

    def __init__(self):
        self.coach_name = "Camino"

    def get_message(self, driver: Driver) -> Optional[Dict[str, str]]:
        stage = driver.current_stage
        
        # 1. Registered -> Day 1 (Nudge)
        if stage == FunnelStage.REGISTERED:
            return {
                "subject": "Ready to fix the #1 mistake?",
                "body": (
                    f"Hey {driver.first_name},\n\n"
                    "Saw you registered for the blueprint but haven't started Day 1 yet.\n\n"
                    "The first video covers the biggest mistake most drivers make with their prep "
                    "(and why they plateau). Takes about 15 mins.\n\n"
                    "Here's the link to jump in: [LINK]\n\n"
                    "Let me know if you have any questions before you start."
                )
            }
            
        # 2. Day 1 -> Day 2
        elif stage == FunnelStage.DAY1_COMPLETE:
            return {
                "subject": "Your Day 1 assessment results",
                "body": (
                    f"Hey {driver.first_name},\n\n"
                    f"Just reviewed your Day 1 assessment. You scored {driver.day1_score}/100.\n\n"
                    "That's a solid starting point, but definitely some low-hanging fruit to improve performance.\n\n"
                    "Day 2 is about building your Self-Assessment profile. That's where we get specific on exactly "
                    "WHICH mental blockers are slowing you down.\n\n"
                    "Ready for the next step? [LINK]"
                )
            }

        # 3. Flow Profile / Post Season Logic (Special Cases)
        
        # TEMPLATE: SEQUENCE 3 (Post-Season Review Follow-up)
        if driver.end_of_season_review_date and stage != FunnelStage.STRATEGY_CALL_BOOKED:
             return {
                 "subject": "Your 2025 Season Review",
                 "body": (
                     f"{driver.first_name} - saw you completed the post-season review.\n\n"
                     "That's actually the #1 thing that separates mid-pack drivers from championship contenders. "
                     "Most drivers ignore it all off-season and then wonder why 2026 feels like 2025 all over again.\n\n"
                     "Want me to show you how to fix the gaps you identified?"
                 )
             }

        # TEMPLATE: SEQUENCE 2 (Off-Season Reflection -> Call)
        if stage == FunnelStage.OUTREACH:
            return {
                "subject": "2026 Season Prep",
                "body": (
                    f"{driver.first_name} - now that the 2025 season's wrapped, what's the one thing you wish you'd worked on before it started?\n\n"
                    "Most drivers say 'mental game' in December and then show up to testing in February with the same issues.\n\n"
                    "If you're serious about making 2026 different, I'm doing some 1-on-1 calls to build proper off-season game plans. Up for a chat?"
                )
            }
            
        # 4. Old Flow Profile Logic (Fallback)
        if driver.flow_profile_result:
             result_type = driver.flow_profile_result
             return {
                "subject": f"Your Flow Profile: {result_type}",
                "body": (
                    f"Hey {driver.first_name},\n\n"
                    f"I saw you got '{result_type}' on your Flow Profile.\n\n"
                    "This usually means you have natural speed but struggle with consistency (or vice versa).\n\n"
                    "We have a specific protocol for this profile. Want me to send over the details?"
                )
             }

        return None


# =============================================================================
# MANUAL DAILY STATS
# =============================================================================

@dataclass
class DailyManualStats:
    """Manual daily inputs for metrics not tracked automatically"""
    date: datetime.date
    fb_messages_sent: int = 0
    ig_messages_sent: int = 0
    links_sent: int = 0

class DailyStatsManager:
    """Manages manual daily statistics"""
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.filename = "daily_stats.csv"
        self.stats: Dict[datetime.date, DailyManualStats] = {}
        self._load_stats()
        
    def _load_stats(self):
        """Load stats from CSV"""
        filepath = os.path.join(self.data_dir, self.filename)
        if not os.path.exists(filepath):
            return
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    date_str = row.get('date')
                    if not date_str:
                        continue
                    dt = datetime.strptime(date_str, '%Y-%m-%d').date()
                    
                    self.stats[dt] = DailyManualStats(
                        date=dt,
                        fb_messages_sent=int(row.get('fb_messages_sent', 0)),
                        ig_messages_sent=int(row.get('ig_messages_sent', 0)),
                        links_sent=int(row.get('links_sent', 0))
                    )
        except Exception as e:
            print(f"Error loading daily stats: {e}")

    def save_stats(self, date: datetime.date, fb: int, ig: int, links: int):
        """Save stats for a specific date"""
        self.stats[date] = DailyManualStats(
            date=date,
            fb_messages_sent=fb,
            ig_messages_sent=ig,
            links_sent=links
        )
        
        # Rewrite file
        filepath = os.path.join(self.data_dir, self.filename)
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['date', 'fb_messages_sent', 'ig_messages_sent', 'links_sent'])
            writer.writeheader()
            
            # Sort by date
            for dt in sorted(self.stats.keys()):
                s = self.stats[dt]
                writer.writerow({
                    'date': dt.strftime('%Y-%m-%d'),
                    'fb_messages_sent': s.fb_messages_sent,
                    'ig_messages_sent': s.ig_messages_sent,
                    'links_sent': s.links_sent
                })

    def get_stats_for_date(self, date: datetime.date) -> DailyManualStats:
        return self.stats.get(date, DailyManualStats(date=date))

    def get_mtd_stats(self, year: int, month: int) -> Dict[str, int]:
        """Get Month-To-Date totals"""
        total_fb = 0
        total_ig = 0
        total_links = 0
        
        for dt, s in self.stats.items():
            if dt.year == year and dt.month == month:
                total_fb += s.fb_messages_sent
                total_ig += s.ig_messages_sent
                total_links += s.links_sent
                
        return {
            'fb_messages_sent': total_fb,
            'ig_messages_sent': total_ig,
            'links_sent': total_links
        }

    def update_stats(self, date: datetime.date, **kwargs):
        """Update specific stats for a date"""
        current = self.get_stats_for_date(date)
        
        # Current values
        fb = current.fb_messages_sent
        ig = current.ig_messages_sent
        links = current.links_sent
        
        # Update from kwargs (additive)
        if 'fb_messages_sent' in kwargs:
            fb += kwargs['fb_messages_sent']
        if 'ig_messages_sent' in kwargs:
            ig += kwargs['ig_messages_sent']
        if 'links_sent' in kwargs:
            links += kwargs['links_sent']
            
        self.save_stats(date, fb, ig, links)

    def increment_fb(self):
        """Add +1 to Today's FB Messages"""
        self.update_stats(datetime.now().date(), fb_messages_sent=1)
        
    def increment_ig(self):
        """Add +1 to Today's IG Messages"""
        self.update_stats(datetime.now().date(), ig_messages_sent=1)
        
    def increment_link(self):
        """Add +1 to Today's Links Sent"""
        self.update_stats(datetime.now().date(), links_sent=1)

    def get_mtd_total(self, stat_name: str) -> int:
        """Calculate Month-To-Date total for a manual stat"""
        now = datetime.now()
        stats = self.get_mtd_stats(now.year, now.month)
        return stats.get(stat_name, 0)


# =============================================================================
# DATA LOADER
# =============================================================================

class DataLoader:
    """Load and process data from CSV files"""

    def __init__(self, data_dir: str, overrides: Optional[Dict[str, Any]] = None):
        self.data_dir = data_dir
        self.drivers: Dict[str, Driver] = {}
        self.load_report = {'total': 0, 'loaded': 0, 'skipped': 0, 'reasons': {}}
        self.overrides = overrides or {}
        
        # Initialize Airtable Manager
        self.airtable = None
        if "airtable" in st.secrets:
             try:
                 self.airtable = AirtableManager(
                     api_key=st.secrets["airtable"]["api_key"],
                     base_id=st.secrets["airtable"]["base_id"],
                     table_name=st.secrets["airtable"].get("table_name", "Drivers")
                 )
                 print("Airtable Manager Initialized")
             except Exception as e:
                 print(f"Failed to init Airtable: {e}")
                 self.airtable = None
        else:
             print("Airtable secrets not found.")

    def load_all_data(self) -> Dict[str, Driver]:
        """Load data from all CSV files and merge into driver records"""

        # 1. Load Master Data from Airtable FIRST (Source of Truth)
        # CRITICAL: Do not comment this out. Required for matching 7700+ drivers.
        self._load_from_airtable()

        # Load each data source
        self._load_strategy_call_applications()
        self._load_blueprint_registrations()
        self._load_day1_assessments()
        self._load_day2_assessments()
        self._load_xperiencify_csv()
        self._load_flow_profile_results()
        self._load_sleep_test()
        self._load_mindset_quiz()
        self._load_race_reviews()

        # LOCAL CSV LOADING REMOVED — Airtable is the sole source of truth.
        # manual_updates.csv, revenue_log.csv, driver_details.csv were overwriting
        # Airtable stages on every reload, causing pipeline to not reflect
        # messaging activity. All writes now go directly to Airtable.
        # self._load_manual_updates()   # DISABLED
        # self._load_revenue_log()      # DISABLED
        # self._load_driver_details()    # DISABLED
        
        # Scan for reviews/socials (flexible CSVs)
        self._scan_for_social_and_reviews()

        # --- AUTO-SYNC: Google Sheets → Airtable ---
        # When Google Sheets data (ScoreApp exports) updates a driver's stage or dates
        # beyond what Airtable had, push those changes back to Airtable immediately.
        # Without this, Sheet data only lives in-memory and is lost on next reload.
        self._auto_sync_sheets_to_airtable()

        # --- Post-load deduplication sweep ---
        # Catch any remaining duplicates where the same person has two records
        # (e.g. a real email from Airtable AND a no_email_ key from a CSV source).
        self._deduplicate_drivers()

        # Update Race Manager if exists
        # This fixes the "New Prospect" bug where RaceManager holds stale data/index
        if hasattr(self, 'race_manager'):
            self.race_manager.refresh_data()

        return self.drivers

    def _auto_sync_sheets_to_airtable(self):
        """Auto-sync: push Google Sheets-sourced data back to Airtable.

        Compares each driver's current state (after Sheets loaded) against
        the Airtable snapshot taken during _load_from_airtable.  If any
        dates or stage moved forward, push that change to Airtable so it
        persists across reloads.
        """
        if not self.airtable:
            return

        _snapshot = getattr(self, '_airtable_snapshot', {})
        if not _snapshot:
            return

        def _fmt(dt):
            return dt.strftime('%Y-%m-%d') if dt else None

        # Stage ordering for "advance" check
        STAGE_ORDER = {
            FunnelStage.CONTACT: 0, FunnelStage.OUTREACH: 1,
            FunnelStage.MESSAGED: 2, FunnelStage.REPLIED: 3,
            FunnelStage.LINK_SENT: 4, FunnelStage.BLUEPRINT_LINK_SENT: 5,
            FunnelStage.RACE_WEEKEND: 6, FunnelStage.RACE_REVIEW_COMPLETE: 7,
            FunnelStage.BLUEPRINT_STARTED: 8, FunnelStage.REGISTERED: 8,
            FunnelStage.SLEEP_TEST_COMPLETED: 3, FunnelStage.MINDSET_QUIZ_COMPLETED: 3,
            FunnelStage.FLOW_PROFILE_COMPLETED: 3,
            FunnelStage.DAY1_COMPLETE: 9, FunnelStage.DAY2_COMPLETE: 10,
            FunnelStage.STRATEGY_CALL_BOOKED: 11,
            FunnelStage.CLIENT: 12, FunnelStage.SALE_CLOSED: 12,
        }

        synced = 0
        errors = 0

        for email, driver in self.drivers.items():
            rec_id = getattr(driver, 'airtable_record_id', None)
            if not rec_id:
                continue

            snap = _snapshot.get(rec_id, {})
            snap_stage = snap.get('stage', FunnelStage.CONTACT)
            cur_stage = driver.current_stage

            cur_order = STAGE_ORDER.get(cur_stage, -1)
            snap_order = STAGE_ORDER.get(snap_stage, -1)

            # Build data to sync: dates that are new (not in snapshot)
            data = {}

            date_map = [
                ('day1_complete_date', 'Date Day 1 Assessment', 'snap_day1'),
                ('day2_complete_date', 'Date Day 2 Assessment', 'snap_day2'),
                ('registered_date', 'Date Blueprint Started', 'snap_reg'),
                ('sleep_test_date', 'Date Sleep Test', 'snap_sleep'),
                ('mindset_quiz_date', 'Date Mindset Quiz', 'snap_mindset'),
                ('flow_profile_date', 'Date Flow Profile', 'snap_flow'),
                ('race_weekend_review_date', 'Date Race Review', 'snap_race'),
                ('strategy_call_booked_date', 'Date Strategy Call', 'snap_strat'),
            ]

            for attr, field, snap_key in date_map:
                current_val = getattr(driver, attr, None)
                snap_val = snap.get(snap_key)
                if current_val and not snap_val:
                    data[field] = _fmt(current_val)

            # If stage advanced, include the new stage
            if cur_order > snap_order:
                data["Stage"] = cur_stage.value
                if driver.last_activity:
                    data["Last Activity"] = _fmt(driver.last_activity)

            if data:
                try:
                    from sync_manager import sync_save
                    name = driver.full_name or email
                    success = sync_save(
                        self.airtable, data,
                        record_id=rec_id,
                        description=f"Auto-sync Sheets→AT: {name}"
                    )
                    if success:
                        synced += 1
                    else:
                        errors += 1
                except Exception as e:
                    print(f"  ⚠️ Auto-sync failed for {email}: {e}")
                    errors += 1

        if synced > 0:
            print(f"  ✅ Auto-synced {synced} driver(s) from Google Sheets → Airtable")
        if errors > 0:
            print(f"  ⚠️ {errors} auto-sync error(s)")


    def _get_data_iter(self, filename: str):
        """Yields rows (dicts) from Google Sheets overrides.
        Local-only CSVs (revenue_log, manual_updates, driver_details) use
        their own csv.DictReader calls, not this method.
        Normalizes all keys to lowercase."""
        raw_iter = None

        # Google Sheets data (loaded as DataFrame or list of dicts)
        if filename in self.overrides:
            data = self.overrides[filename]
            # If it's a DataFrame (has 'to_dict'), convert it
            if hasattr(data, 'to_dict'):
                # orient='records' -> list of dicts. Fix duplicates warning.
                if hasattr(data, 'loc') and hasattr(data, 'columns'):
                    data = data.loc[:, ~data.columns.duplicated()]
                raw_iter = data.to_dict(orient='records')
            elif isinstance(data, list):
                raw_iter = data
        else:
            # No Google Sheet data for this file — skip silently
            print(f"  ⏭️ {filename}: no Google Sheet override found, skipping (CSV fallback disabled)")
            return

        if raw_iter:
            for row in raw_iter:
                # Normalize keys: Lowercase and strip
                clean_row = {str(k).lower().strip(): v for k, v in row.items() if k}
                yield clean_row

    def _first_available_data_file(self, filenames):
        """Find first filename with Google Sheets data available."""
        for filename in filenames:
            if filename in self.overrides:
                return filename
        return None

    @staticmethod
    def _looks_like_datetime(val: str) -> bool:
        """Return True if val looks like an ISO datetime rather than a phone number.

        Catches strings like '2026-02-13T17:21:30.328402+00:00' that end up in
        phone fields when a webhook maps a timestamp column incorrectly.
        """
        import re
        v = val.strip()
        # ISO 8601 datetime patterns
        if re.match(r'^\d{4}-\d{2}-\d{2}[T ]', v):
            return True
        # Also catch bare timestamps with timezone offsets
        if re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            return True
        return False

    @staticmethod
    def _resolve_email(row):
        """Extract identity from a Google Sheet row.

        Full Name is the PRIMARY identifier.  Email is optional (most contacts
        come via social media with no email).

        Two-pass approach:
          Pass 1: Header-based extraction (standard column names)
          Pass 2: Content-based fallback — scan ALL values to find emails
                  and names when headers don't match expected patterns.

        Returns (email, first_name, last_name) tuple.
        The caller passes all three to _get_or_create_driver() which uses
        full name as the primary dict key.
        """
        import re as _re

        # --- PASS 1: Header-based extraction ---
        email = (row.get('email', '') or row.get('email address', '') or
                 row.get('email_address', '')).strip()

        # Also check for common Google Forms / webhook variations
        if not email:
            for k, v in row.items():
                kl = str(k).lower()
                if 'email' in kl and v:
                    val = str(v).strip()
                    if '@' in val:
                        email = val
                        break

        first_name = (row.get('first_name', '') or row.get('first name', '') or
                     row.get('firstname', '')).strip()
        last_name = (row.get('last_name', '') or row.get('last name', '') or
                    row.get('lastname', '') or row.get('surname', '')).strip()

        # DEFENSIVE: If an email address ended up in a name field, move it
        if '@' in first_name:
            if not email: email = first_name
            first_name = ''
        if '@' in last_name:
            if not email: email = last_name
            last_name = ''

        # DEFENSIVE: If a datetime/timestamp ended up in a name field, discard it
        if first_name and _re.match(r'^\d{4}-\d{2}-\d{2}[T ]', first_name):
            first_name = ''
        if last_name and _re.match(r'^\d{4}-\d{2}-\d{2}[T ]', last_name):
            last_name = ''

        # CHECK 'full name' column — PREFER it when it contains a multi-word
        # name and first/last look incomplete (e.g. webhook put surname in
        # first_name and left last_name empty).
        full_name_col = (row.get('full name', '') or row.get('full_name', '') or '').strip()
        # Avoid using 'name' column here — too generic, often has other data
        if not full_name_col:
            full_name_col = (row.get('name', '') or '').strip()

        if full_name_col:
            if '@' in full_name_col:
                if not email: email = full_name_col
                full_name_col = ''
            elif _re.match(r'^\d{4}-\d{2}-\d{2}', full_name_col):
                full_name_col = ''  # Discard datetime

        if full_name_col:
            fn_parts = full_name_col.split()
            if len(fn_parts) >= 2:
                # Full name has at least first + last — ALWAYS prefer it
                # over potentially-wrong first_name / last_name fields
                first_name = fn_parts[0]
                last_name = ' '.join(fn_parts[1:])
            elif not first_name and not last_name:
                # Only a single word in full name, and no other name info
                first_name = fn_parts[0] if fn_parts else ''

        # --- PASS 2: Content-based fallback ---
        # If we STILL have no email, scan all values for an @ address
        if not email:
            for k, v in row.items():
                val = str(v).strip()
                if '@' in val and '.' in val and not _re.match(r'^\d{4}-\d{2}-\d{2}', val):
                    # Looks like an email address
                    if _re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', val):
                        email = val
                        break

        # If we have email but no name, try to derive name from email prefix
        if email and not first_name and not last_name:
            prefix = email.split('@')[0]
            # Common patterns: first.last, first_last, firstlast
            prefix_clean = prefix.replace('.', ' ').replace('_', ' ').replace('-', ' ')
            parts = prefix_clean.split()
            if len(parts) >= 2 and all(p.isalpha() for p in parts):
                first_name = parts[0].title()
                last_name = ' '.join(p.title() for p in parts[1:])

        # Must have at least a name or email to be a valid row
        if not email and not first_name and not last_name:
            return '', '', ''

        return email, first_name, last_name

    def _load_from_airtable(self):
        """Load Master Records from Airtable"""
        if not self.airtable: return
        
        # Check cache or fetch
        if self.airtable.drivers_cache:
            records = self.airtable.drivers_cache
        else:
            records = self.airtable.fetch_all_drivers()
            
        print(f"Loaded {len(records)} drivers from Airtable")

        # Store debug info for UI display
        self.airtable_debug = {
            'total_records': len(records),
            'column_names': list(records[0].keys()) if records else [],
            'skipped': []
        }

        skipped_count = 0
        for r in records:
            # Identity — Full Name is the PRIMARY identifier
            email = (r.get('Email') or '').strip()
            full_name = r.get('Full Name') or r.get('Name') or r.get('full_name') or r.get('name')
            first = (r.get('First Name') or r.get('first_name') or '').strip()
            last = (r.get('Last Name') or r.get('last_name') or '').strip()

            # DEFENSIVE: If an email address ended up in a name field, move it
            if '@' in first:
                if not email: email = first
                first = ''
            if '@' in last:
                if not email: email = last
                last = ''
            if full_name and '@' in full_name:
                if not email: email = full_name
                full_name = ''

            # Build full name from parts if Full Name column is empty
            if not full_name and (first or last):
                full_name = f"{first} {last}".strip()

            # If we have Full Name but missing first OR last, try to split it
            if full_name and (not first or not last):
                parts = full_name.strip().split()
                if len(parts) >= 2:
                    if not first: first = parts[0]
                    if not last: last = " ".join(parts[1:])
                elif len(parts) == 1 and not first:
                    first = parts[0]

            # If we have email but no name, try to derive from email prefix
            if email and not first and not last and not full_name:
                prefix = email.split('@')[0]
                prefix_clean = prefix.replace('.', ' ').replace('_', ' ').replace('-', ' ')
                parts = prefix_clean.split()
                if len(parts) >= 2 and all(p.isalpha() for p in parts):
                    first = parts[0].title()
                    last = ' '.join(p.title() for p in parts[1:])

            # Must have either a name or email to create a driver
            if not full_name and not email:
                skipped_count += 1
                if skipped_count <= 10:
                    self.airtable_debug['skipped'].append(str(r)[:200])
                continue

            driver = self._get_or_create_driver(email, first or "", last or "")

            # Store Airtable record ID for direct updates (no search needed)
            if r.get('id'):
                driver.airtable_record_id = r['id']

            # ALWAYS update names from Airtable (Source of Truth)
            # This fixes the bug where names were empty after reload
            if first:
                driver.first_name = first.strip()
            if last:
                driver.last_name = last.strip()

            # Store raw Full Name from Airtable for better matching
            if full_name:
                driver.raw_full_name = full_name.strip()

            # Map Fields (Robust)
            _phone = str(r.get('Phone Number', '')).strip()
            if _phone and not self._looks_like_datetime(_phone):
                driver.phone = _phone
            
            # Robust key search for Socials
            fb_val = r.get('FB URL') or r.get('Facebook URL') or r.get('fb_url') or r.get('facebook')
            if fb_val: driver.facebook_url = fb_val
            
            ig_val = r.get('IG URL') or r.get('Instagram URL') or r.get('ig_url') or r.get('instagram')
            if ig_val: driver.instagram_url = ig_val
            
            web_val = r.get('Website URL') or r.get('Website')
            if web_val: driver.magic_link = web_val
            
            # Tags (Airtable sends list of strings)
            tags = r.get('Tags')
            if tags and isinstance(tags, list):
                driver.tags = ",".join(tags) 
            
            # Scores (Day 1)
            if r.get('Overall Score'): driver.day1_score = float(r.get('Overall Score'))
            if r.get('Biggest Mistake'): driver.biggest_mistake = r.get('Biggest Mistake')
            
            # Dates — load ALL date columns that we sync back to Airtable
            if r.get('Date Blueprint Started'):
                driver.registered_date = self._parse_date(r.get('Date Blueprint Started'))
                if not driver.outreach_date:
                    driver.outreach_date = driver.registered_date
            if r.get('Date Messaged'): driver.outreach_date = self._parse_date(r.get('Date Messaged'))
            if r.get('Date Replied'): driver.replied_date = self._parse_date(r.get('Date Replied'))
            if r.get('Date Link Sent'): driver.link_sent_date = self._parse_date(r.get('Date Link Sent'))
            if r.get('Date Race Review'): driver.race_weekend_review_date = self._parse_date(r.get('Date Race Review'))
            if r.get('Date Sleep Test'): driver.sleep_test_date = self._parse_date(r.get('Date Sleep Test'))
            if r.get('Date Mindset Quiz'): driver.mindset_quiz_date = self._parse_date(r.get('Date Mindset Quiz'))
            if r.get('Date Flow Profile'): driver.flow_profile_date = self._parse_date(r.get('Date Flow Profile'))
            if r.get('Date Day 1 Assessment'): driver.day1_complete_date = self._parse_date(r.get('Date Day 1 Assessment'))
            if r.get('Date Day 2 Assessment'): driver.day2_complete_date = self._parse_date(r.get('Date Day 2 Assessment'))
            if r.get('Date Strategy Call'): driver.strategy_call_booked_date = self._parse_date(r.get('Date Strategy Call'))
            if r.get('Date Sale Closed'): driver.sale_closed_date = self._parse_date(r.get('Date Sale Closed'))

            # Race Results — load from dedicated Airtable field into notes [RESULTS] block
            _at_race_results = r.get('Race Results', '')
            if _at_race_results and '[RESULTS]' not in (driver.notes or ''):
                import json as _rrj
                try:
                    _parsed = _rrj.loads(_at_race_results) if isinstance(_at_race_results, str) else _at_race_results
                    if isinstance(_parsed, list) and _parsed:
                        _results_block = f"[RESULTS]\n{_rrj.dumps(_parsed)}\n[/RESULTS]"
                        driver.notes = f"{_results_block}\n{driver.notes or ''}" if driver.notes else _results_block
                except Exception:
                    pass
            
            # Preferred name (from social media / Also Known As field)
            aka = r.get('Also Known As', '')
            if aka:
                # Use the first name from the AKA field (e.g. "Chris Reynolds" → "Chris")
                first_aka = aka.split(',')[0].strip().split()[0] if aka.strip() else ''
                if first_aka and first_aka.lower() != (driver.first_name or '').lower():
                    driver.preferred_name = first_aka

            # Stage Mapping
            stage_str = r.get('Stage')
            if stage_str:
                s_clean = stage_str.strip().lower()
                found_stage = None
                for stage in FunnelStage:
                    if stage.value.lower() == s_clean:
                        found_stage = stage
                        break
                    # Aliases
                    if s_clean in ['messaged', 'outreach']: found_stage = FunnelStage.MESSAGED
                    if s_clean in ['client', 'won']: found_stage = FunnelStage.CLIENT
                    if s_clean in ['lost', 'not a fit']: found_stage = FunnelStage.NOT_A_FIT
                    if s_clean in ['registered', 'blueprint started']: found_stage = FunnelStage.BLUEPRINT_STARTED
                    if s_clean in ['strategy call', 'call booked', 'strategy call booked']: found_stage = FunnelStage.STRATEGY_CALL_BOOKED
                    # Stage-like values with slight differences
                    if s_clean in ['day 1', 'day 1 complete']: found_stage = FunnelStage.DAY1_COMPLETE
                    if s_clean in ['day 2', 'day 2 complete']: found_stage = FunnelStage.DAY2_COMPLETE

                # LEGACY: Comma-separated automation tags (e.g. "driver_lead, added to blueprint, ...")
                # Parse keywords to infer highest pipeline stage
                if not found_stage and ',' in s_clean:
                    tags = [t.strip() for t in s_clean.split(',')]
                    # Check tags from HIGHEST stage to LOWEST
                    if any('day 3 completed' in t or 'day 2 completed' in t or 'completed blueprint 2' in t for t in tags):
                        found_stage = FunnelStage.DAY2_COMPLETE
                    elif any('day 1 completed' in t or 'watched day 1' in t or 'completed blueprint 1' in t for t in tags):
                        found_stage = FunnelStage.DAY1_COMPLETE
                    elif any('started podium contenders' in t or 'blueprint started' in t or 'mission accepted' in t for t in tags):
                        found_stage = FunnelStage.BLUEPRINT_STARTED
                    elif any('season review completed' in t for t in tags):
                        found_stage = FunnelStage.RACE_REVIEW_COMPLETE
                    elif any('season review started' in t for t in tags):
                        found_stage = FunnelStage.RACE_WEEKEND
                    elif any('added to blueprint' in t or 'in blueprint' in t for t in tags):
                        found_stage = FunnelStage.BLUEPRINT_LINK_SENT

                if found_stage:
                    driver.current_stage = found_stage

            # SAFETY NET: If driver is still at CONTACT but has date evidence
            # of progress, infer the stage from the most advanced date field.
            # Only checks fields that actually exist in the Airtable schema.
            if driver.current_stage == FunnelStage.CONTACT:
                if r.get('Date Strategy Call'):
                    driver.current_stage = FunnelStage.STRATEGY_CALL_BOOKED
                elif r.get('Date Blueprint Started'):
                    driver.current_stage = FunnelStage.BLUEPRINT_STARTED
                elif r.get('Date Race Review'):
                    driver.current_stage = FunnelStage.RACE_REVIEW_COMPLETE

            # CRM Fields (The "State" we need to persist)
            if r.get('Notes'): driver.notes = r.get('Notes')
            if r.get('Championship'): driver.championship = r.get('Championship')
            if r.get('Follow Up Date'): driver.follow_up_date = self._parse_date(r.get('Follow Up Date'))
            
            # Revenue (Financials)
            if r.get('Revenue'): 
                try:
                    driver.sale_value = float(r.get('Revenue'))
                except: pass

            # Last Activity (from Airtable column)
            if r.get('Last Activity'):
                driver.last_activity = self._parse_date(r.get('Last Activity'))

            # Fallback Date (If no specific Funnel Date, use Record Creation Time)
            _created_dt = None
            if r.get('createdTime'):
                try:
                    dt_str = r.get('createdTime').replace('Z', '+00:00')
                    _created_dt = datetime.fromisoformat(dt_str).replace(tzinfo=None)
                except: pass

            # DATE RECOVERY SAFETY NET: If a driver has a pipeline stage but
            # the stage-specific date column was EMPTY in Airtable (due to
            # the sync bug where dates weren't being written), use createdTime
            # as the fallback. Prefer createdTime over Last Activity because
            # Last Activity gets updated on every stage sync and would show
            # wrong "days at stage" counts (e.g., showing 1d instead of 7d).
            #
            # CRITICAL: Only fill in the date if the driver attribute is ALSO
            # empty — don't overwrite a date that was set from another source
            # (e.g., 'Date Blueprint Started' also sets outreach_date).
            _fallback_date = _created_dt or driver.last_activity
            if _fallback_date:
                _used_fallback = False
                if driver.current_stage in [FunnelStage.MESSAGED, FunnelStage.OUTREACH] and not r.get('Date Messaged'):
                    if not driver.outreach_date:
                        driver.outreach_date = _fallback_date
                        _used_fallback = True
                elif driver.current_stage == FunnelStage.REPLIED and not r.get('Date Replied'):
                    if not driver.replied_date:
                        driver.replied_date = _fallback_date
                        _used_fallback = True
                    if not driver.outreach_date: driver.outreach_date = _fallback_date
                elif driver.current_stage in [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT] and not r.get('Date Link Sent'):
                    if not driver.link_sent_date:
                        driver.link_sent_date = _fallback_date
                        _used_fallback = True
                elif driver.current_stage in [FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE] and not r.get('Date Race Review'):
                    if not driver.race_weekend_review_date:
                        driver.race_weekend_review_date = _fallback_date
                        _used_fallback = True
                elif driver.current_stage in [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED] and not r.get('Date Blueprint Started'):
                    if not driver.registered_date:
                        driver.registered_date = _fallback_date
                        _used_fallback = True
                elif driver.current_stage == FunnelStage.DAY1_COMPLETE and not r.get('Date Day 1 Assessment'):
                    if not driver.day1_complete_date:
                        driver.day1_complete_date = _fallback_date
                        _used_fallback = True
                elif driver.current_stage == FunnelStage.DAY2_COMPLETE and not r.get('Date Day 2 Assessment'):
                    if not driver.day2_complete_date:
                        driver.day2_complete_date = _fallback_date
                        _used_fallback = True
                elif driver.current_stage == FunnelStage.STRATEGY_CALL_BOOKED and not r.get('Date Strategy Call'):
                    if not driver.strategy_call_booked_date:
                        driver.strategy_call_booked_date = _fallback_date
                        _used_fallback = True
                # Flag drivers whose stage date is inferred, not from Airtable
                if _used_fallback:
                    driver._date_is_fallback = True

            # Last resort: if outreach_date is still empty, use createdTime
            if not driver.outreach_date and _created_dt:
                driver.outreach_date = _created_dt

            # Ensure last_activity is always populated for sorting —
            # take the most recent date from any field on this driver.
            if not driver.last_activity:
                candidate_dates = [
                    driver.outreach_date, driver.replied_date,
                    getattr(driver, 'link_sent_date', None),
                    driver.race_weekend_review_date,
                    getattr(driver, 'sleep_test_date', None),
                    getattr(driver, 'mindset_quiz_date', None),
                    getattr(driver, 'flow_profile_date', None),
                    driver.registered_date,
                    getattr(driver, 'day1_complete_date', None),
                    getattr(driver, 'day2_complete_date', None),
                    driver.strategy_call_booked_date,
                    driver.sale_closed_date,
                ]
                valid = [d for d in candidate_dates if isinstance(d, datetime)]
                if valid:
                    driver.last_activity = max(valid)

        # --- SNAPSHOT: capture Airtable state before Google Sheets overlay ---
        # Used by _auto_sync_sheets_to_airtable to detect what Sheets changed
        self._airtable_snapshot = {}
        for email, driver in self.drivers.items():
            rec_id = getattr(driver, 'airtable_record_id', None)
            if rec_id:
                self._airtable_snapshot[rec_id] = {
                    'stage': driver.current_stage,
                    'snap_day1': driver.day1_complete_date,
                    'snap_day2': driver.day2_complete_date,
                    'snap_reg': driver.registered_date,
                    'snap_sleep': driver.sleep_test_date,
                    'snap_mindset': driver.mindset_quiz_date,
                    'snap_flow': driver.flow_profile_date,
                    'snap_race': driver.race_weekend_review_date,
                    'snap_strat': driver.strategy_call_booked_date,
                }

        # Summary of loading
        self.airtable_debug['skipped_count'] = skipped_count
        if skipped_count > 0:
            print(f"DEBUG: Skipped {skipped_count} records (no email and no name)")

    def sync_database_to_airtable(self) -> int:
        """
        Manually sync ALL loaded drivers to Airtable.
        Returns the number of drivers successfully synced.
        """
        if not self.airtable:
            print("No Airtable connection.")
            return 0
            
        count = 0
        total = len(self.drivers)
        print(f"Starting bulk sync for {total} drivers...")
        
        def _fmt(dt):
            return dt.strftime('%Y-%m-%d') if dt else None

        for email, driver in self.drivers.items():
            try:
                # Basic Mapping
                data = {
                    "Email": driver.email,
                    "First Name": driver.first_name,
                    "Last Name": driver.last_name,
                    "FB URL": driver.facebook_url,
                    "IG URL": driver.instagram_url,
                    "Stage": driver.current_stage.value if driver.current_stage else "Contact",
                    "Phone Number": driver.phone,
                    "Championship": driver.championship,
                }

                # Optional fields
                if driver.sale_value: data["Revenue"] = driver.sale_value

                # --- DATES SYNC (Comprehensive - matches migrate_driver_to_airtable) ---
                # Outreach / Pipeline dates
                if driver.outreach_date: data["Date Messaged"] = _fmt(driver.outreach_date)
                if driver.replied_date: data["Date Replied"] = _fmt(driver.replied_date)
                if driver.link_sent_date: data["Date Link Sent"] = _fmt(driver.link_sent_date)

                # Follow Up Date: always sync (including None to clear)
                data["Follow Up Date"] = _fmt(driver.follow_up_date)

                # Activity / Funnel Dates
                if driver.last_activity: data["Last Activity"] = _fmt(driver.last_activity)
                if driver.registered_date: data["Date Blueprint Started"] = _fmt(driver.registered_date)
                if driver.day1_complete_date: data["Date Day 1 Assessment"] = _fmt(driver.day1_complete_date)
                if driver.day2_complete_date: data["Date Day 2 Assessment"] = _fmt(driver.day2_complete_date)
                if driver.strategy_call_booked_date: data["Date Strategy Call"] = _fmt(driver.strategy_call_booked_date)

                # Lead Magnet Dates (from Google Sheets)
                if driver.race_weekend_review_date: data["Date Race Review"] = _fmt(driver.race_weekend_review_date)
                if driver.sleep_test_date: data["Date Sleep Test"] = _fmt(driver.sleep_test_date)
                if driver.mindset_quiz_date: data["Date Mindset Quiz"] = _fmt(driver.mindset_quiz_date)
                if driver.flow_profile_date: data["Date Flow Profile"] = _fmt(driver.flow_profile_date)

                # clean empty
                clean_data = {k: v for k, v in data.items() if v is not None}

                # Race Results — save structured JSON to dedicated Airtable field
                _race_json = parse_race_results(driver.notes or "")
                if _race_json:
                    import json as _rj
                    clean_data["Race Results"] = _rj.dumps(_race_json)
                
                success = self.airtable.upsert_driver(clean_data)
                if success:
                    count += 1
                    if count % 50 == 0:
                        print(f"  Synced {count}/{total} drivers...")
            except Exception as e:
                print(f"Failed to sync {email}: {e}")
                
        return count

    def add_new_driver_to_db(self, email: str, first_name: str, last_name: str, fb_url: str, ig_url: str = "", championship: str = "", **kwargs) -> bool:
        """Add a new driver to Airtable and in-memory database."""
        full_name = f"{first_name} {last_name}".strip()

        try:
            # --- AIRTABLE SYNC (Synchronous — verified save) ---
            if self.airtable:
                at_data = {
                    "Email": email,
                    "First Name": first_name,
                    "Last Name": last_name,
                    "FB URL": fb_url,
                    "IG URL": ig_url,
                    "Championship": championship,
                    "Phone Number": kwargs.get('phone', ''),
                }
                # Optional fields
                if kwargs.get('notes'):
                    at_data['Notes'] = kwargs['notes']
                    # Also sync structured race results to dedicated field
                    _rr = parse_race_results(kwargs['notes'])
                    if _rr:
                        import json as _rrj2
                        at_data['Race Results'] = _rrj2.dumps(_rr)
                if kwargs.get('follow_up_date'):
                    at_data['Follow Up Date'] = kwargs['follow_up_date'].strftime('%Y-%m-%d')
                # Include stage if provided (avoids a second Airtable call)
                if kwargs.get('stage'):
                    stage_val = kwargs['stage']
                    if hasattr(stage_val, 'value'):
                        stage_val = stage_val.value
                    at_data['Stage'] = stage_val
                    at_data['Last Activity'] = datetime.now().strftime('%Y-%m-%d')
                # Sync Last Activity so outreach date shows in Airtable
                existing_driver = self._find_driver_by_key(email)
                if existing_driver and existing_driver.last_activity:
                    at_data['Last Activity'] = existing_driver.last_activity.strftime('%Y-%m-%d')

                # Pass Airtable record ID if available (for direct update, no search needed)
                existing_driver = self._find_driver_by_key(email)
                record_id = getattr(existing_driver, 'airtable_record_id', None) if existing_driver else None

                from sync_manager import sync_save
                sync_save(self.airtable, at_data, record_id=record_id,
                          description=f"Add/update {first_name} {last_name}")

            # Update In-Memory
            driver = self._get_or_create_driver(email, first_name, last_name)
            
            # Explicitly update properties (including clearing them if empty)
            driver.facebook_url = fb_url
            driver.instagram_url = ig_url
            driver.championship = championship
            
            if 'notes' in kwargs: driver.notes = kwargs['notes']
            if 'phone' in kwargs: driver.phone = kwargs['phone']
            if 'follow_up_date' in kwargs: driver.follow_up_date = kwargs['follow_up_date']
            
            print(f"DEBUG_UPDATE: Updated {email} in memory. FB={fb_url}")
                
            return True
        except Exception as e:
            print(f"Error adding driver: {e}")
            return False

            # (Duplicate unreachable sync block removed)
            return True

        
    def _scan_for_social_and_reviews(self):
        """Scan all CSVs in dir AND overrides (Google Sheets) for Social Media columns and Review dates"""

        race_review_count = 0
        season_review_count = 0

        # PRIORITY 1: Process Google Sheets overrides first (fresh data)
        print(f"\n=== SCANNING OVERRIDES FOR REVIEWS ===")
        print(f"Total overrides to process: {len(self.overrides)}")

        for filename, df in self.overrides.items():
            if not filename.endswith(".csv"):
                continue

            try:
                if df is None or (hasattr(df, 'empty') and df.empty):
                    print(f"  ⚠ {filename}: Empty or None")
                    continue

                # Deduplicate columns to prevent Series-in-boolean errors
                if hasattr(df, 'columns') and df.columns.duplicated().any():
                    df = df.loc[:, ~df.columns.duplicated()]

                # Get headers from DataFrame
                headers = [h.lower() for h in df.columns]

                has_email = 'email' in headers
                has_name = any(h in headers for h in ['full name', 'full_name', 'first name', 'first_name', 'name'])
                if not has_email and not has_name:
                    print(f"  ⚠ {filename}: No email or name column")
                    continue

                # 1. Check for specific Review Types by unique columns
                # Race Weekend Review (export 15) has "what circuit did you race at this weekend?"
                is_race_review = any("what circuit did you race at" in h for h in headers) or 'race weekend' in filename.lower()

                # End of Season Review (export 16) has "what championship did you race in?" (and explicitly mentions season)
                is_season_review = any("what championship did you race in" in h for h in headers) or 'end of season' in filename.lower()

                if is_race_review:
                    print(f"  ✓ {filename}: RACE REVIEW detected ({len(df)} rows)")
                elif is_season_review:
                    print(f"  ✓ {filename}: SEASON REVIEW detected ({len(df)} rows)")
                else:
                    # Debug: Why wasn't it detected?
                    if len(df) > 0 and 'export' in filename:
                        print(f"  ? {filename} NOT detected as review. Headers sample: {headers[:10]}")

                # 2. Check for Social Links (Generic)
                has_fb = any('facebook' in h for h in headers)
                has_ig = any('instagram' in h for h in headers)
                has_li = any('linked' in h for h in headers)

                if not (has_fb or has_ig or has_li or is_race_review or is_season_review):
                    continue

                # Process DataFrame rows — use to_dict for safe scalar access
                for row_dict in df.to_dict(orient='records'):
                    # Get email (case-insensitive)
                    email = None
                    for k, v in row_dict.items():
                        if str(k).lower() == 'email':
                            email = str(v).strip() if pd.notna(v) else ''
                            break

                    if not email or '@' not in email:
                        continue

                    driver = self._get_or_create_driver(email)

                    # Extract Socials
                    for k, v in row_dict.items():
                        c_low = str(k).lower()
                        val = str(v).strip() if pd.notna(v) else ''
                        if not val:
                            continue

                        if 'facebook' in c_low and 'url' in c_low:
                            driver.facebook_url = val
                        elif 'instagram' in c_low and 'url' in c_low:
                            driver.instagram_url = val
                        elif 'linked' in c_low and 'url' in c_low:
                            driver.linkedin_url = val

                    # Extract Name if missing
                    for k, v in row_dict.items():
                        kl = str(k).lower()
                        if kl == 'first_name' and not driver.first_name and pd.notna(v):
                            driver.first_name = str(v).strip()
                        elif kl == 'last_name' and not driver.last_name and pd.notna(v):
                            driver.last_name = str(v).strip()

                    # Extract Dates (ScoreApp standard: 'scorecard_finished_at' or 'submit date (utc)')
                    date_str = None
                    for k, v in row_dict.items():
                        if str(k).lower() in ['scorecard_finished_at', 'submit_date_utc', 'submit date (utc)']:
                            if pd.notna(v) and str(v).strip():
                                date_str = str(v).strip()
                                break

                    submit_date = self._parse_date(date_str) if date_str else None

                    if is_race_review and submit_date:
                        if not driver.race_weekend_review_date or submit_date > driver.race_weekend_review_date:
                            driver.race_weekend_review_date = submit_date
                            driver.race_weekend_review_status = "completed"
                            race_review_count += 1

                            # Auto-update status to RACE_REVIEW_COMPLETE ONLY if they haven't progressed further
                            if driver.current_stage in [FunnelStage.CONTACT, FunnelStage.MESSAGED, FunnelStage.RACE_WEEKEND, FunnelStage.REPLIED]:
                                driver.current_stage = FunnelStage.RACE_REVIEW_COMPLETE
                                print(f"    → Updated {email[:30]} to RACE_REVIEW_COMPLETE")

                    if is_season_review and submit_date:
                        if not driver.end_of_season_review_date or submit_date > driver.end_of_season_review_date:
                            driver.end_of_season_review_date = submit_date
                            season_review_count += 1

            except Exception as e:
                print(f"Error processing override {filename}: {e}")
                continue

        print(f"\n=== OVERRIDE SCAN COMPLETE ===")
        print(f"  Race Reviews processed: {race_review_count}")
        print(f"  Season Reviews processed: {season_review_count}")
        # CSV fallback removed — all pipeline data comes from Google Sheets

    def save_revenue(self, email: str, amount: float):
        """Save revenue entry — Airtable only (no local CSV)."""
        # Update in-memory (alias-aware — driver may be stored under name key)
        driver = self._find_driver_by_key(email)
        if driver:
            driver.sale_value = amount

        # Sync to Airtable (synchronous — verified save)
        if self.airtable:
            data = {"Email": email, "Revenue": amount}
            rec_id = getattr(driver, 'airtable_record_id', None) if driver else None
            from sync_manager import sync_save
            sync_save(self.airtable, data, record_id=rec_id,
                      description=f"Revenue {amount} for {email}")

    def _load_revenue_log(self):
        """Load revenue_log.csv"""
        filepath = os.path.join(self.data_dir, 'revenue_log.csv')
        if not os.path.exists(filepath):
            return
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    email = row.get('email', '').strip().lower()
                    try:
                        amount = float(row.get('amount', 0))
                    except ValueError:
                        continue
                        
                    if email and amount > 0:
                        driver = self._get_or_create_driver(email)
                        driver.sale_value = amount
                        # Parse date if available
                        date_val = (row.get('date', '') or row.get('timestamp', '') or
                                   row.get('submitted at', '') or row.get('created at', ''))
                        sale_date = self._parse_date(date_val) if date_val else None
                        if sale_date:
                            driver.sale_closed_date = sale_date
                            driver.last_activity = sale_date
                        elif not driver.sale_closed_date:
                            # No date in CSV — use now as fallback so filtering works
                            driver.sale_closed_date = datetime.now()
                            driver.last_activity = driver.sale_closed_date
                        # Assume sale closed if revenue present
                        if driver.current_stage != FunnelStage.SALE_CLOSED:
                             driver.current_stage = FunnelStage.SALE_CLOSED
        except Exception:
            pass

    def save_manual_update(self, email: str, stage):
        """Save a manual stage update — in-memory only.
        Airtable sync is handled by FunnelDashboard.update_driver_stage().
        Local CSV writing removed — Airtable is the sole source of truth."""
        stage_val = stage.value if isinstance(stage, FunnelStage) else str(stage)

        # Update in-memory (alias-aware — driver may be stored under name key)
        driver = self._find_driver_by_key(email)
        if driver:
            matched_stage = None
            for s in FunnelStage:
                if s.value == stage_val:
                    matched_stage = s
                    break

            if matched_stage:
                driver.current_stage = matched_stage
                driver.last_activity = datetime.now()

                # Update date in memory for immediate UI feedback
                if matched_stage == FunnelStage.MESSAGED:
                    driver.outreach_date = datetime.now()

        return True

            
    def save_driver_details(self, email: str, **kwargs):
        """Save custom CRM fields (notes, follow_up, championship, etc.)
        directly to Airtable. Local CSV writing removed."""
        # 1. Update in-memory Driver immediately
        driver = self._get_or_create_driver(email)
        for k, v in kwargs.items():
            if hasattr(driver, k):
                setattr(driver, k, v)

        # --- GSHEET SYNC REMOVED ---


        # --- AIRTABLE SYNC (Synchronous — verified save) ---
        if self.airtable:
            # Prepare Payload
            data = {"Email": email}

            # Map Kwargs to Airtable Columns
            if 'notes' in kwargs: data['Notes'] = kwargs['notes']
            if 'championship' in kwargs: data['Championship'] = kwargs['championship']
            if 'follow_up_date' in kwargs:
                d = kwargs['follow_up_date']
                data['Follow Up Date'] = d.strftime('%Y-%m-%d') if d else None
            if 'phone' in kwargs: data['Phone Number'] = kwargs['phone']
            if 'sale_value' in kwargs: data['Revenue'] = kwargs['sale_value']

            # Upsert — pass record ID for direct update if available
            driver = self._find_driver_by_key(email)
            rec_id = getattr(driver, 'airtable_record_id', None) if driver else None

            from sync_manager import sync_save
            sync_save(self.airtable, data, record_id=rec_id,
                      description=f"Details for {email}")

    def delete_driver_from_db(self, email: str):
        """Permanently delete a driver — Airtable deletion handled by
        FunnelDashboard.delete_driver(). Local CSV cleanup removed."""
        email = email.lower().strip()
        print(f"[Delete] {email} — local CSV cleanup skipped (Airtable-only mode)")

    def _load_driver_details(self):
        """Load driver_details.csv and apply to Drivers"""
        filepath = os.path.join(self.data_dir, 'driver_details.csv')
        if not os.path.exists(filepath):
            return

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    email = row.get('email', '').strip().lower()
                    field_name = row.get('field')
                    value_str = row.get('value')
                    
                    if not email or not field_name: continue
                    
                    driver = self._get_or_create_driver(email)
                    
                    # Type Conversion
                    if field_name == 'follow_up_date':
                        driver.follow_up_date = self._parse_date(value_str)
                    elif field_name == 'is_disqualified':
                        driver.is_disqualified = (value_str == 'True')
                    elif field_name == 'sale_value':
                        try: driver.sale_value = float(value_str)
                        except: pass
                    elif hasattr(driver, field_name):
                        # Generic string fields: notes, championship, disqualification_reason
                        setattr(driver, field_name, value_str)
                        
        except Exception:
            pass # resilient loading

    def _load_manual_updates(self):
        """Load manual_updates.csv"""
        filepath = os.path.join(self.data_dir, 'manual_updates.csv')
        
        if not os.path.exists(filepath):
            return
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    email = row.get('email', '').strip().lower()
                    stage_val = row.get('stage', '')
                    timestamp_str = row.get('timestamp', '')
                    
                    if not email or not stage_val:
                        continue
                        
                    # Handle Legacy Enum Repr (e.g. "FunnelStage.REPLIED")
                    if "FunnelStage." in stage_val:
                        # Extract name "REPLIED"
                        stage_val_clean = stage_val.split('.')[-1]
                    else:
                        stage_val_clean = stage_val

                    # Find matching enum
                    matched_stage = None
                    for stage in FunnelStage:
                        # Match Value ("Replied") OR Name ("REPLIED")
                        if stage.value == stage_val_clean or stage.name == stage_val_clean:
                            matched_stage = stage
                            break
                        if stage.value == stage_val:
                            matched_stage = stage
                            break
                    
                    if matched_stage:
                         driver = self._get_or_create_driver(email)
                         driver.current_stage = matched_stage
                         
                         if timestamp_str:
                             ts = self._parse_date(timestamp_str)
                             if ts:
                                 driver.last_activity = ts
                                 
                                 # Set the CORRECT stage-specific date so pipeline
                                 # columns sort by when the driver entered that stage
                                 if matched_stage in [FunnelStage.MESSAGED, FunnelStage.OUTREACH]:
                                     driver.outreach_date = ts
                                 elif matched_stage == FunnelStage.REPLIED:
                                     driver.replied_date = ts
                                     if not driver.outreach_date: driver.outreach_date = ts
                                 elif matched_stage in [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT]:
                                     driver.link_sent_date = ts
                                     if not driver.outreach_date: driver.outreach_date = ts
                                 elif matched_stage in [FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE]:
                                     driver.race_weekend_review_date = ts
                                 elif matched_stage in [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED]:
                                     driver.registered_date = ts
                                 elif matched_stage == FunnelStage.DAY1_COMPLETE:
                                     driver.day1_complete_date = ts
                                 elif matched_stage == FunnelStage.DAY2_COMPLETE:
                                     driver.day2_complete_date = ts
                                 elif matched_stage == FunnelStage.STRATEGY_CALL_BOOKED:
                                     driver.strategy_call_booked_date = ts
                                 elif matched_stage in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED]:
                                     driver.sale_closed_date = ts

        except Exception:
            pass # Ignore corrupt manual file

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats"""
        if not date_str:
            return None

        formats = [
            '%d/%m/%Y %H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y',
            '%Y-%m-%dT%H:%M:%S.%f',  # ISO with microseconds (from datetime.isoformat())
            '%Y-%m-%dT%H:%M:%S',     # ISO without microseconds
            '%Y-%m-%dT%H:%M:%S.%fZ', # ISO with Z
            '%Y-%m-%dT%H:%M:%SZ',
        ]

        # Try native ISO parsing first (fastest/most robust for logs)
        try:
            dt = datetime.fromisoformat(date_str)
            # Always strip timezone info to keep all dates naive (prevents
            # "can't subtract offset-naive and offset-aware datetimes" errors)
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except ValueError:
            pass

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None

    def _load_xperiencify_csv(self):
        """Load Xperiencify.csv — Flow Performance Programme Starters (paid clients).

        Drivers in this sheet have started the paid programme = CLIENT stage.
        Handles both snake_case and Google Forms headers.
        Note: _get_data_iter lowercases all keys automatically.
        """
        filename = 'Xperiencify.csv'

        for row in self._get_data_iter(filename):
            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name: continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            # Map Fields
            if not driver.phone:
                _phone = str(row.get('phone', '') or row.get('phone number', '') or row.get('mobile', '')).strip()
                if _phone and not self._looks_like_datetime(_phone):
                    driver.phone = _phone

            # Date — when they started the paid programme
            date_val = (row.get('scorecard_finished_at', '') or
                       row.get('submitted_at', '') or
                       row.get('date', '') or
                       row.get('date_joined', '') or
                       row.get('timestamp', '') or
                       row.get('submitted at', '') or
                       row.get('start date', '') or
                       row.get('date started', '') or
                       row.get('created at', ''))
            start_date = self._parse_date(date_val)

            if start_date:
                driver.sale_closed_date = start_date
                driver.last_activity = start_date

            # These drivers are paid programme starters = CLIENT
            driver.current_stage = FunnelStage.CLIENT

            # Airtable sync removed — data already loaded from Airtable




    def _find_driver_by_key(self, key: str) -> Optional[Driver]:
        """Alias-aware driver lookup WITHOUT creating new drivers.

        Drivers may be stored under full-name key ('daryl hutt') or email
        key ('no_email_daryl_hutt').  This resolves aliases so callers
        don't have to know which key was used at creation time.
        """
        k = key.lower().strip() if key else ''
        if not k:
            return None
        # 1. Direct dict key
        if k in self.drivers:
            return self.drivers[k]
        # 2. Alias resolution
        if hasattr(self, '_key_aliases'):
            resolved = self._key_aliases.get(k)
            if resolved and resolved in self.drivers:
                return self.drivers[resolved]
        # 3. Name-based fallback — extract name from slug email
        #    e.g. 'no_email_daryl_hutt' → search for 'daryl hutt'
        if k.startswith('no_email_'):
            name_guess = k.replace('no_email_', '').replace('_', ' ')
            if name_guess in self.drivers:
                return self.drivers[name_guess]
            # Scan by full_name property
            for driver in self.drivers.values():
                if driver.full_name.lower().strip() == name_guess:
                    # Cache alias for next time
                    if hasattr(self, '_key_aliases'):
                        self._key_aliases[k] = next(
                            (rk for rk, rv in self.drivers.items() if rv is driver), k
                        )
                    return driver
        return None

    def _get_or_create_driver(self, email: str, first_name: str = '', last_name: str = '') -> Driver:
        """Get existing driver or create new one.

        FULL NAME is the PRIMARY key for driver identity.  Most contacts come
        via social media with no email, so name is the only reliable ID.

        Lookup order:
        1. Full Name key (primary)
        2. Email / alias key (fallback for legacy data)
        3. Create new driver keyed by full name (or email if no name)
        """
        first_name = first_name.strip() if first_name else ''
        last_name = last_name.strip() if last_name else ''
        email_clean = email.lower().strip() if email else ''

        full_name = f"{first_name} {last_name}".strip()
        name_key = full_name.lower() if full_name else ''

        # AUTO-GENERATE slug email for social media leads with no email
        # This ensures every driver has a usable identifier for UI actions
        if not email_clean and full_name:
            slug = name_key.replace(' ', '_')
            slug = "".join([c for c in slug if c.isalnum() or c == '_'])
            email_clean = f"no_email_{slug}"

        # Initialise alias dict on first call
        if not hasattr(self, '_key_aliases'):
            self._key_aliases = {}
        # Migrate old _email_aliases if present
        if hasattr(self, '_email_aliases') and not hasattr(self, '_aliases_migrated'):
            self._key_aliases.update(self._email_aliases)
            self._aliases_migrated = True

        # --- 1. Lookup by full name (primary) ---
        if name_key and len(name_key) > 1:
            # Check aliases
            resolved = self._key_aliases.get(name_key, name_key)
            if resolved in self.drivers:
                driver = self.drivers[resolved]
                # Fill in email if we now have one and driver doesn't
                if email_clean and not email_clean.startswith('no_email_'):
                    if not driver.email or driver.email.startswith('no_email_'):
                        driver.email = email_clean
                # Ensure driver always has SOME email for UI actions
                if not driver.email and email_clean:
                    driver.email = email_clean
                # ALWAYS store email alias so _find_driver_by_key can resolve it
                if email_clean and email_clean != resolved:
                    self._key_aliases[email_clean] = resolved
                return driver

            # Check all existing drivers by name match
            for existing_key, existing_driver in self.drivers.items():
                if existing_driver.full_name.lower().strip() == name_key:
                    self._key_aliases[name_key] = existing_key
                    if email_clean:
                        self._key_aliases[email_clean] = existing_key
                    # Fill in email if better
                    if email_clean and not email_clean.startswith('no_email_'):
                        if not existing_driver.email or existing_driver.email.startswith('no_email_'):
                            existing_driver.email = email_clean
                    # Ensure driver always has SOME email for UI actions
                    if not existing_driver.email and email_clean:
                        existing_driver.email = email_clean
                    return existing_driver

        # --- 1b. Nickname + Last Name match ---
        # "Chris Binker" should match "Christopher Binker" when the last
        # name is identical and the first names are nickname-equivalent.
        # This is the 4-way check: first name, last name, nickname, championship.
        # Championship acts as a tiebreaker — there will never be 2 people
        # in the same championship with the same name.
        if name_key and ' ' in name_key and first_name and last_name:
            fn_lower = first_name.lower().strip()
            ln_lower = last_name.lower().strip()
            nickname_matches = []
            for ek, er in self.drivers.items():
                er_ln = (er.last_name or '').lower().strip()
                er_fn = (er.first_name or '').lower().strip()
                if er_ln == ln_lower and er_fn != fn_lower:
                    # Same last name, different first name — check nickname
                    if _names_match_via_nickname(fn_lower, er_fn):
                        nickname_matches.append((ek, er))
            if len(nickname_matches) == 1:
                existing_key, existing_driver = nickname_matches[0]
                self._key_aliases[name_key] = existing_key
                print(f"[Identity] Nickname match: '{full_name}' → '{existing_driver.full_name}'")
                # Store the shorter / social-media name as preferred for DMs
                incoming_fn = first_name.strip()
                existing_fn = (existing_driver.first_name or '').strip()
                shorter = incoming_fn if len(incoming_fn) < len(existing_fn) else existing_fn
                if shorter and shorter.lower() != existing_fn.lower():
                    existing_driver.preferred_name = shorter
                elif not existing_driver.preferred_name:
                    existing_driver.preferred_name = shorter or incoming_fn
                # Merge email
                if email_clean and not email_clean.startswith('no_email_'):
                    if not existing_driver.email or existing_driver.email.startswith('no_email_'):
                        existing_driver.email = email_clean
                if email_clean and email_clean != existing_key:
                    self._key_aliases[email_clean] = existing_key
                return existing_driver

        # --- 1c. Partial name match (single-word name) ---
        # When only a first OR last name is supplied (e.g. webhook puts
        # surname "Reynolds" in first_name field), try to find the one
        # existing driver whose first_name or last_name matches.
        if name_key and ' ' not in name_key and len(name_key) > 1:
            partial_matches = []
            for ek, er in self.drivers.items():
                if (er.last_name and er.last_name.lower().strip() == name_key) or \
                   (er.first_name and er.first_name.lower().strip() == name_key):
                    partial_matches.append((ek, er))
            if len(partial_matches) == 1:
                existing_key, existing_driver = partial_matches[0]
                self._key_aliases[name_key] = existing_key
                # Merge email
                if email_clean and not email_clean.startswith('no_email_'):
                    if not existing_driver.email or existing_driver.email.startswith('no_email_'):
                        existing_driver.email = email_clean
                if email_clean and email_clean != existing_key:
                    self._key_aliases[email_clean] = existing_key
                return existing_driver

        # --- 1c. Email-derived name match ---
        # If we have an email like john.reynolds@gmail.com, derive
        # "john reynolds" and try to find that driver.
        if email_clean and '@' in email_clean:
            prefix = email_clean.split('@')[0]
            derived = prefix.replace('.', ' ').replace('_', ' ').replace('-', ' ').strip()
            parts = derived.split()
            if len(parts) >= 2 and all(p.isalpha() for p in parts):
                derived_key = ' '.join(parts).lower()
                # Direct dict lookup
                resolved = self._key_aliases.get(derived_key, derived_key)
                if resolved in self.drivers:
                    driver = self.drivers[resolved]
                    # Merge email
                    if email_clean and not email_clean.startswith('no_email_'):
                        if not driver.email or driver.email.startswith('no_email_'):
                            driver.email = email_clean
                    # Update names if we have better info
                    if first_name and not driver.first_name:
                        driver.first_name = first_name
                    if last_name and not driver.last_name:
                        driver.last_name = last_name
                    # Store aliases
                    if name_key and len(name_key) > 1:
                        self._key_aliases[name_key] = resolved
                    if email_clean != resolved:
                        self._key_aliases[email_clean] = resolved
                    return driver
                # Scan by full_name property
                for ek, er in self.drivers.items():
                    if er.full_name.lower().strip() == derived_key:
                        self._key_aliases[derived_key] = ek
                        if email_clean:
                            self._key_aliases[email_clean] = ek
                        if name_key and len(name_key) > 1:
                            self._key_aliases[name_key] = ek
                        if email_clean and not email_clean.startswith('no_email_'):
                            if not er.email or er.email.startswith('no_email_'):
                                er.email = email_clean
                        return er

        # --- 2. Fallback: lookup by email (for legacy/internal CSVs) ---
        if email_clean:
            resolved = self._key_aliases.get(email_clean, email_clean)
            if resolved in self.drivers:
                driver = self.drivers[resolved]
                if first_name and not driver.first_name:
                    driver.first_name = first_name
                if last_name and not driver.last_name:
                    driver.last_name = last_name
                # Store name alias for future lookups
                if name_key and len(name_key) > 1:
                    self._key_aliases[name_key] = resolved
                return driver

        # --- 3. Create new driver ---
        # Key by full name if available, otherwise by email
        if name_key and len(name_key) > 1:
            primary_key = name_key
        elif email_clean:
            primary_key = email_clean
        else:
            # No identity — create a placeholder so callers don't get None
            primary_key = f"_unknown_{len(self.drivers)}"

        self.drivers[primary_key] = Driver(
            email=email_clean,
            first_name=first_name,
            last_name=last_name,
        )

        # Store cross-references
        if name_key and email_clean and name_key != primary_key:
            self._key_aliases[name_key] = primary_key
        if email_clean and email_clean != primary_key:
            self._key_aliases[email_clean] = primary_key

        return self.drivers[primary_key]

    # ------------------------------------------------------------------
    # Stage-ordering helper used by dedup to pick the "furthest" stage
    # ------------------------------------------------------------------
    _STAGE_PRIORITY = [
        FunnelStage.CONTACT,
        FunnelStage.MESSAGED,
        FunnelStage.REPLIED,
        FunnelStage.LINK_SENT,
        FunnelStage.BLUEPRINT_LINK_SENT,
        FunnelStage.RACE_WEEKEND,
        FunnelStage.RACE_REVIEW_COMPLETE,
        FunnelStage.SLEEP_TEST_COMPLETED,
        FunnelStage.MINDSET_QUIZ_COMPLETED,
        FunnelStage.FLOW_PROFILE_COMPLETED,
        FunnelStage.REGISTERED,
        FunnelStage.DAY1_COMPLETE,
        FunnelStage.DAY2_COMPLETE,
        FunnelStage.STRATEGY_CALL_BOOKED,
        FunnelStage.CLIENT,
    ]

    def _stage_rank(self, stage):
        try:
            return self._STAGE_PRIORITY.index(stage)
        except ValueError:
            return -1

    def _merge_driver_into(self, keep, discard):
        """Merge all data from `discard` driver into `keep` driver.

        Takes the furthest pipeline stage, copies non-None dates,
        merges social URLs, notes, and other CRM fields.
        """
        # Take the furthest stage
        if self._stage_rank(discard.current_stage) > self._stage_rank(keep.current_stage):
            keep.current_stage = discard.current_stage

        # Merge dates: copy any non-None dates from discard → keep
        date_attrs = [
            'outreach_date', 'replied_date', 'link_sent_date',
            'race_weekend_review_date', 'sleep_test_date',
            'mindset_quiz_date', 'flow_profile_date',
            'registered_date', 'day1_complete_date',
            'day2_complete_date', 'strategy_call_booked_date',
            'sale_closed_date', 'last_activity', 'follow_up_date',
        ]
        for attr in date_attrs:
            src = getattr(discard, attr, None)
            dst = getattr(keep, attr, None)
            if src and not dst:
                setattr(keep, attr, src)

        # Merge last_activity: keep the most recent
        if discard.last_activity and keep.last_activity:
            if discard.last_activity > keep.last_activity:
                keep.last_activity = discard.last_activity

        # Copy name if keep is missing it
        if not keep.first_name and discard.first_name:
            keep.first_name = discard.first_name
        if not keep.last_name and discard.last_name:
            keep.last_name = discard.last_name

        # Merge social URLs
        for url_attr in ['facebook_url', 'instagram_url', 'linkedin_url']:
            if getattr(discard, url_attr, None) and not getattr(keep, url_attr, None):
                setattr(keep, url_attr, getattr(discard, url_attr))

        # Merge CRM fields
        if not keep.championship and discard.championship:
            keep.championship = discard.championship
        if not keep.notes and discard.notes:
            keep.notes = discard.notes
        if not keep.phone and discard.phone:
            keep.phone = discard.phone
        if not keep.airtable_record_id and discard.airtable_record_id:
            keep.airtable_record_id = discard.airtable_record_id
        if not keep.email or keep.email.startswith('no_email_'):
            if discard.email and not discard.email.startswith('no_email_'):
                keep.email = discard.email
        # Preserve preferred_name (the shorter / social media name)
        if discard.preferred_name and not keep.preferred_name:
            keep.preferred_name = discard.preferred_name

    def _deduplicate_drivers(self):
        """Remove duplicate driver records — 4-way identity check.

        Pass 1: Exact full-name match (original logic).
        Pass 2: Nickname + Last Name + Championship match.
                 e.g. 'Chris Binker' and 'Christopher Binker' in the
                 same championship are merged because there will never
                 be two people with the same name in the same championship.

        The 4 identity dimensions:
          1. First Name
          2. Last Name (must match exactly)
          3. Nickname (first names must be nickname-equivalent)
          4. Championship (must match when both are set — acts as tiebreaker)
        """
        # ── Pass 1: Exact full-name dedup (original) ──
        seen_names = {}   # lowercase name → primary email_key
        to_remove = []

        for email_key, driver in list(self.drivers.items()):
            name = driver.full_name.lower().strip()
            if not name or len(name) <= 3 or name == email_key:
                continue  # skip unnamed / email-as-name drivers

            if name in seen_names:
                primary_key = seen_names[name]
                primary = self.drivers.get(primary_key)
                if primary is None:
                    seen_names[name] = email_key
                    continue

                # Decide which record to keep (prefer real email)
                if primary_key.startswith('no_email_') and not email_key.startswith('no_email_'):
                    keep, discard, discard_key = driver, primary, primary_key
                    seen_names[name] = email_key
                else:
                    keep, discard, discard_key = primary, driver, email_key

                self._merge_driver_into(keep, discard)
                to_remove.append(discard_key)
            else:
                seen_names[name] = email_key

        for key in to_remove:
            self.drivers.pop(key, None)

        exact_merges = len(to_remove)
        if exact_merges:
            print(f"[Dedup] Pass 1 — Merged {exact_merges} exact-name duplicate(s)")

        # ── Pass 2: Nickname + Last Name + Championship dedup ──
        # Group drivers by last name for efficient comparison
        from collections import defaultdict
        by_last_name = defaultdict(list)  # lowercase last_name → [(key, driver)]
        for key, driver in self.drivers.items():
            ln = (driver.last_name or '').lower().strip()
            if ln and len(ln) > 1:
                by_last_name[ln].append((key, driver))

        nickname_removes = []
        already_merged = set()

        for ln, group in by_last_name.items():
            if len(group) < 2:
                continue  # No duplicates possible

            # Compare all pairs within the same last-name group
            for i in range(len(group)):
                key_a, driver_a = group[i]
                if key_a in already_merged:
                    continue
                fn_a = (driver_a.first_name or '').lower().strip()
                champ_a = (driver_a.championship or '').lower().strip()

                for j in range(i + 1, len(group)):
                    key_b, driver_b = group[j]
                    if key_b in already_merged:
                        continue
                    fn_b = (driver_b.first_name or '').lower().strip()
                    champ_b = (driver_b.championship or '').lower().strip()

                    # Skip if first names are identical (already handled in Pass 1)
                    if fn_a == fn_b:
                        continue

                    # Check nickname equivalence
                    if not _names_match_via_nickname(fn_a, fn_b):
                        continue

                    # Championship check: if BOTH have a championship set,
                    # they must match. If one or both are empty, allow the merge
                    # (timing sheet data often doesn't have championship yet).
                    if champ_a and champ_b and champ_a != champ_b:
                        continue  # Different championships — different people

                    # ✓ Same last name + nickname-matching first names +
                    #   compatible championship → SAME PERSON
                    # Keep the one with the longer first name (more formal)
                    if len(fn_a) >= len(fn_b):
                        keep, keep_key = driver_a, key_a
                        discard, discard_key = driver_b, key_b
                    else:
                        keep, keep_key = driver_b, key_b
                        discard, discard_key = driver_a, key_a

                    print(f"[Dedup] Pass 2 — Nickname merge: "
                          f"'{discard.full_name}' → '{keep.full_name}' "
                          f"(championship: {champ_a or champ_b or 'n/a'})")

                    # Store the shorter / social-media name as preferred for DMs
                    shorter_fn = discard.first_name if len(fn_b) < len(fn_a) else discard.first_name
                    if len(fn_a) < len(fn_b):
                        shorter_fn = driver_a.first_name
                    else:
                        shorter_fn = driver_b.first_name
                    if shorter_fn and not keep.preferred_name:
                        keep.preferred_name = shorter_fn

                    self._merge_driver_into(keep, discard)

                    # Store alias so future lookups find the kept driver
                    if hasattr(self, '_key_aliases'):
                        self._key_aliases[discard_key] = keep_key
                        # Also alias the discard's name
                        discard_name = discard.full_name.lower().strip()
                        if discard_name:
                            self._key_aliases[discard_name] = keep_key

                    nickname_removes.append(discard_key)
                    already_merged.add(discard_key)

        for key in nickname_removes:
            self.drivers.pop(key, None)

        if nickname_removes:
            print(f"[Dedup] Pass 2 — Merged {len(nickname_removes)} nickname duplicate(s)")

        total = exact_merges + len(nickname_removes)
        if total:
            print(f"[Dedup] Total merged: {total} duplicate driver(s)")

    def _load_strategy_call_applications(self):
        """Load Strategy Call Application.csv

        Handles both snake_case CSV headers AND Google Forms headers:
          Google Forms: 'email address', 'first name', 'last name', 'timestamp'
          Snake case:   'email', 'first_name', 'last_name', 'submit_date_utc'
        Note: _get_data_iter lowercases all keys automatically.
        """
        filename = 'Strategy Call Application.csv'

        _sc_first = True
        for row in self._get_data_iter(filename):
            if _sc_first:
                print(f"  Strategy Call columns: {list(row.keys())}")
                _sc_first = False
            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name:
                continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            # Update stage to strategy call booked (only if not already further along)
            if driver.current_stage not in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED,
                                           FunnelStage.NOT_A_FIT, FunnelStage.FOLLOW_UP]:
                driver.current_stage = FunnelStage.STRATEGY_CALL_BOOKED

            # Robust Date Parsing — try all common header variants
            date_val = (row.get('scorecard_finished_at', '') or
                       row.get('submitted_at', '') or
                       row.get('submit_date_utc', '') or
                       row.get('stage_date_utc', '') or
                       row.get('date', '') or
                       row.get('timestamp', '') or
                       row.get('submitted at', '') or
                       row.get('created at', '') or
                       row.get('submit date', '') or
                       row.get('date submitted', '') or
                       row.get('submission date', ''))

            driver.strategy_call_booked_date = self._parse_date(date_val)

            # Update last_activity for sorting
            if driver.strategy_call_booked_date:
                driver.last_activity = driver.strategy_call_booked_date

            # Additional data — flexible header matching
            _phone = str(row.get('phone', '') or row.get('phone number', '') or row.get('mobile', '')).strip()
            if _phone and not self._looks_like_datetime(_phone):
                driver.phone = _phone
            driver.country = (row.get('country', '') or row.get('location', ''))
            driver.driver_type = (row.get('driver_type', '') or row.get('driver type', '') or row.get('type of driver', ''))
            driver.championship = (row.get('championship_racing_in', '') or
                                 row.get('championship racing in', '') or
                                 row.get('championship', '') or
                                 row.get('what championship are you racing in?', ''))

            # Airtable sync removed — data already loaded from Airtable

    def _load_blueprint_registrations(self):
        """Load Podium Contenders Blueprint Registered.csv

        Handles both snake_case CSV headers AND Google Forms headers:
          Google Forms: 'first name', 'last name', 'email', 'phone number', 'date'
          Snake case:   'first_name', 'last_name', 'email', 'phone', 'date'
        Note: _get_data_iter lowercases all keys automatically.
        """
        filename = 'Podium Contenders Blueprint Registered.csv'

        _bp_first_row = True
        for row in self._get_data_iter(filename):
            # Debug: log actual column headers on first row
            if _bp_first_row:
                print(f"  Blueprint columns: {list(row.keys())}")
                _bp_first_row = False

            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name:
                continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            # Only update if not already further in funnel
            if driver.current_stage in [FunnelStage.CONTACT, FunnelStage.OUTREACH, FunnelStage.MESSAGED, FunnelStage.REPLIED, FunnelStage.LINK_SENT]:
                driver.current_stage = FunnelStage.REGISTERED

            # Robust Date Parsing — try all common header variants
            date_val = (row.get('scorecard_finished_at', '') or
                       row.get('submitted_at', '') or
                       row.get('submit_date_utc', '') or
                       row.get('stage_date_utc', '') or
                       row.get('date', '') or
                       row.get('timestamp', '') or
                       row.get('submitted at', '') or
                       row.get('created at', '') or
                       row.get('submit date', '') or
                       row.get('date submitted', '') or
                       row.get('submission date', ''))

            driver.registered_date = self._parse_date(date_val)

            # Update last_activity for sorting
            if driver.registered_date:
                driver.last_activity = driver.registered_date

            # Additional data — flexible header matching
            if not driver.phone:
                phone_val = str(row.get('phone', '') or row.get('phone number', '') or row.get('mobile', '')).strip()
                # DEFENSIVE: Reject datetime strings that ended up in phone field
                # (happens when webhook maps a timestamp column to phone)
                if phone_val and not self._looks_like_datetime(phone_val):
                    driver.phone = phone_val
            if not driver.country:
                driver.country = (row.get('country', '') or row.get('location', ''))
            if not driver.driver_type:
                driver.driver_type = (row.get('driver_type', '') or row.get('driver type', '') or row.get('type of driver', ''))

            # Airtable sync removed — data already loaded from Airtable

    def _load_day1_assessments(self):
        """Load 7 Biggest Mistakes Assessment.csv"""
        filename = self._first_available_data_file([
            '7 Biggest Mistakes Assessment.csv',
            'Day 1 Assessment.csv',
        ])
        
        print(f"\n=== LOADING DAY 1 ASSESSMENT ===")
        if not filename:
            print("  ⚠ No Day 1 file found")
            return

        print(f"  ✓ Found file: {filename}")
        
        count_processed = 0
        count_completed = 0
        count_updated = 0

        for row in self._get_data_iter(filename):
            count_processed += 1
            if count_processed == 1:
                print(f"  DEBUG: First row keys: {list(row.keys())}")

            # --- ScoreApp header fix ---
            # ScoreApp overwrites A1:B1 with score values, so the first
            # column header becomes a number like '79' instead of 'full_name'.
            # Column A actually holds the full name.  Detect this and inject
            # a 'full name' key so _resolve_email can find it.
            if 'full name' not in row and 'full_name' not in row and 'name' not in row:
                keys = list(row.keys())
                if keys:
                    first_key = keys[0]
                    first_val = str(row.get(first_key, '')).strip()
                    # If the header is numeric/UUID and the value looks like a name
                    # (contains a space, no @, not a date), treat it as full name
                    header_is_odd = (first_key.replace('.', '').replace('-', '').isdigit()
                                    or first_key.startswith('#')
                                    or len(first_key) > 30)  # UUID-length
                    if header_is_odd and first_val and ' ' in first_val and '@' not in first_val:
                        row['full name'] = first_val

            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name:
                continue

            # Check if completed (fallback to date if column missing)
            completed_val = row.get('completed', '')
            finished_date = (row.get('scorecard_finished_at', '') or
                            row.get('submitted_at', '') or
                            row.get('submitted at', ''))

            # If 'completed' column exists, respect it. If missing, assume completed if 'finished_at' date exists.
            # Use str() to handle non-string values from Google Sheets (booleans, NaN, etc.)
            is_completed_explicit = str(completed_val).strip().lower() in ('yes', 'true', '1')
            is_completed_implied = (not completed_val and finished_date)

            if not (is_completed_explicit or is_completed_implied):
                # Debug why not completed if valid email
                if count_processed <= 5:
                     print(f"  DEBUG: Row {count_processed} skipped (completed='{completed_val}', finished='{finished_date}')")
                continue

            count_completed += 1

            driver = self._get_or_create_driver(email, first_name, last_name)

            # Update stage if not already further (include all stages before Day 1 in the funnel)
            if driver.current_stage in [FunnelStage.CONTACT, FunnelStage.OUTREACH, FunnelStage.MESSAGED,
                                       FunnelStage.REPLIED, FunnelStage.LINK_SENT, FunnelStage.REGISTERED,
                                       FunnelStage.BLUEPRINT_LINK_SENT, FunnelStage.BLUEPRINT_STARTED,
                                       FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE,
                                       FunnelStage.SLEEP_TEST_COMPLETED, FunnelStage.MINDSET_QUIZ_COMPLETED,
                                       FunnelStage.FLOW_PROFILE_COMPLETED]:
                driver.current_stage = FunnelStage.DAY1_COMPLETE
                count_updated += 1
                if count_updated <= 3:
                     print(f"  → Updated {email} to DAY1_COMPLETE")
        
            # Robust Date Parsing
            date_val = (row.get('scorecard_finished_at', '') or
                       row.get('submitted_at', '') or
                       row.get('submitted at', '') or
                       row.get('submit_date_utc', '') or
                       row.get('submit date (utc)', '') or
                       row.get('date', '') or
                       row.get('timestamp', '') or
                       row.get('created at', '') or
                       row.get('submit date', ''))
            
            if not date_val and not driver.day1_complete_date:
                # Debug missing dates
                 print(f"DEBUG: Day 1 - No date found for {email}. Keys: {list(row.keys())[:5]}...")
                       
            driver.day1_complete_date = self._parse_date(date_val)

            # Update last_activity for sorting
            if driver.day1_complete_date:
                driver.last_activity = driver.day1_complete_date

            # Extract overall score — handle multiple column-name formats:
            #   ScoreApp export: 'Overall Score - Actual'
            #   Google Sheet:    'score', 'overall score', or numeric headers
            try:
                score_str = (row.get('overall score - actual', '') or
                            row.get('overall score', '') or
                            row.get('score', ''))
                # If standard headers fail, check for 'actual' in any key
                if not score_str:
                    for k, v in row.items():
                        kl = str(k).lower()
                        if 'actual' in kl or 'score' in kl:
                            score_str = str(v).strip()
                            break
                if score_str:
                    driver.day1_score = float(score_str)
            except (ValueError, TypeError):
                pass

            # Airtable sync removed — data already loaded from Airtable

        print(f"  ✓ Processed {count_processed} rows. {count_completed} completed. {count_updated} stage updates.")

    def _load_day2_assessments(self):
        """Load Day 2 Self Assessment.csv"""
        filename = 'Day 2 Self Assessment.csv'

        _d2_first = True
        for row in self._get_data_iter(filename):
            if _d2_first:
                print(f"  Day 2 columns: {list(row.keys())}")
                _d2_first = False

            # ScoreApp header fix: detect numeric/UUID/# header in column A
            if 'full name' not in row and 'full_name' not in row and 'name' not in row:
                keys = list(row.keys())
                if keys:
                    first_key = keys[0]
                    first_val = str(row.get(first_key, '')).strip()
                    header_is_odd = (first_key.replace('.', '').replace('-', '').isdigit()
                                    or first_key.startswith('#')
                                    or len(first_key) > 30)
                    if header_is_odd and first_val and ' ' in first_val and '@' not in first_val:
                        row['full name'] = first_val

            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name:
                continue

            # Skip only if explicitly marked as not completed
            completed_val = str(row.get('completed', '')).strip().lower()
            if completed_val in ('no', 'false', '0'):
                continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            # Update stage if not already further (only advance from Day 1 or Registered)
            if driver.current_stage in [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED,
                                       FunnelStage.DAY1_COMPLETE]:
                driver.current_stage = FunnelStage.DAY2_COMPLETE

            # Robust Date Parsing (ScoreApp sheets use scorecard_finished_at / submitted_at)
            date_val = (row.get('scorecard_finished_at', '') or
                       row.get('submitted_at', '') or
                       row.get('submit_date_utc', '') or
                       row.get('stage_date_utc', '') or
                       row.get('date', '') or
                       row.get('timestamp', '') or
                       row.get('submitted at', '') or
                       row.get('created at', '') or
                       row.get('submit date', '') or
                       row.get('date submitted', ''))

            driver.day2_complete_date = self._parse_date(date_val)

            # Update last_activity for sorting
            if driver.day2_complete_date:
                driver.last_activity = driver.day2_complete_date

            # Extract pillar scores
            driver.day2_scores = {}
            pillar_keys = [
                ('Pillar 1', 'mindset'),
                ('Pillar 2', 'preparation'),
                ('Pillar 3', 'flow'),
                ('Pillar 4', 'feedback'),
                ('Pillar 5', 'sponsorship'),
            ]

            for csv_key, score_key in pillar_keys:
                # First pass: look for column with pillar name AND 'rate'
                found = False
                for col in row.keys():
                    cl = col.lower()
                    if csv_key.lower() in cl and 'rate' in cl:
                        try:
                            driver.day2_scores[score_key] = float(row[col])
                            found = True
                        except (ValueError, TypeError):
                            pass
                        break
                # Second pass: just pillar name (Pillar 2 and 5 don't have 'rate')
                if not found:
                    for col in row.keys():
                        cl = col.lower()
                        if csv_key.lower() in cl:
                            try:
                                val = float(row[col])
                                driver.day2_scores[score_key] = val
                            except (ValueError, TypeError):
                                pass
                            break

            # Airtable sync removed — data already loaded from Airtable

    def _load_sleep_test(self):
        """Load Sleep Test.csv"""
        filename = 'Sleep Test.csv'

        _sl_first = True
        for row in self._get_data_iter(filename):
            if _sl_first:
                print(f"  Sleep Test columns: {list(row.keys())}")
                _sl_first = False
            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name:
                continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            driver.sleep_test_date = self._parse_date(
                row.get('scorecard_finished_at', '') or row.get('submitted_at', '') or
                row.get('submit_date_utc', '') or row.get('stage_date_utc', '') or
                row.get('date', '') or row.get('timestamp', '') or
                row.get('submitted at', '') or row.get('created at', '')
            )

            # Update last_activity for sorting
            if driver.sleep_test_date:
                driver.last_activity = driver.sleep_test_date

            try:
                score = row.get('overall score - actual', '') or row.get('score', '')
                if score:
                    driver.sleep_score = float(score)
            except ValueError:
                pass

            # --- UPDATE STAGE ---
            if driver.current_stage in [FunnelStage.CONTACT, FunnelStage.OUTREACH]:
                driver.current_stage = FunnelStage.SLEEP_TEST_COMPLETED

            # Airtable sync removed — data already loaded from Airtable

    def _load_mindset_quiz(self):
        """Load Mindset Quiz.csv"""
        filename = 'Mindset Quiz.csv'

        _mq_first = True
        for row in self._get_data_iter(filename):
            if _mq_first:
                print(f"  Mindset Quiz columns: {list(row.keys())}")
                _mq_first = False
            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name:
                continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            driver.mindset_quiz_date = self._parse_date(
                row.get('scorecard_finished_at', '') or row.get('submitted_at', '') or
                row.get('submit_date_utc', '') or row.get('stage_date_utc', '') or
                row.get('date', '') or row.get('timestamp', '') or
                row.get('submitted at', '') or row.get('created at', '')
            )

            # Update last_activity for sorting
            if driver.mindset_quiz_date:
                driver.last_activity = driver.mindset_quiz_date

            # Result extraction (Type/Score)
            try:
                score = row.get('overall score - actual', '') or row.get('score', '')
                if score:
                    driver.mindset_score = float(score)
            except ValueError:
                pass

            outcome = row.get('outcome', '') or row.get('your mindset', '') or row.get('result', '')
            if outcome:
                driver.mindset_result = outcome.strip()

            # --- UPDATE STAGE ---
            if driver.current_stage in [FunnelStage.CONTACT, FunnelStage.OUTREACH]:
                driver.current_stage = FunnelStage.MINDSET_QUIZ_COMPLETED

            # Airtable sync removed — data already loaded from Airtable

    def _load_flow_profile_results(self):
        """Load Flow Profile.csv"""
        filename = 'Flow Profile.csv'

        _fp_first = True
        for row in self._get_data_iter(filename):
            if _fp_first:
                print(f"  Flow Profile columns: {list(row.keys())}")
                _fp_first = False
            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name:
                continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            # Map fields — Submit Date (UTC) -> flow_profile_date
            driver.flow_profile_date = self._parse_date(
                row.get('scorecard_finished_at', '') or row.get('submitted_at', '') or
                row.get('submit_date_utc', '') or row.get('submit date (utc)', '') or
                row.get('date', '') or row.get('timestamp', '') or
                row.get('submitted at', '') or row.get('created at', '')
            )

            # Update last_activity for sorting
            if driver.flow_profile_date:
                driver.last_activity = driver.flow_profile_date

            # Score -> flow_profile_score
            try:
                driver.flow_profile_score = float(row.get('score', '0'))
            except (ValueError, TypeError):
                pass

            # Ending -> flow_profile_url and result
            # Try standard 'ending' column first, then check for URL-as-header
            # columns (ScoreApp exports URLs as column headers with 'true'/value)
            ending_url = row.get('ending', '') or row.get('ending_displayed_id', '')
            if not ending_url:
                # ScoreApp format: column header IS the URL, value is truthy
                for k, v in row.items():
                    k_str = str(k).strip()
                    if k_str.startswith('http') and 'caminocoaching' in k_str:
                        val = str(v).strip().lower()
                        if val and val not in ('', '0', 'false', 'none', 'nan'):
                            ending_url = k_str
                            break

            driver.flow_profile_url = ending_url

            # --- UPDATE STAGE ---
            if driver.current_stage in [FunnelStage.CONTACT, FunnelStage.OUTREACH]:
                driver.current_stage = FunnelStage.FLOW_PROFILE_COMPLETED

            # Derive result from URL if possible
            if ending_url:
                ending_lower = ending_url.lower()
                if 'gogetter' in ending_lower or 'go-getter' in ending_lower:
                    driver.flow_profile_result = "Go Getter"
                elif 'deepthinker' in ending_lower or 'deep-thinker' in ending_lower:
                    driver.flow_profile_result = "Deep Thinker"
                elif 'novice' in ending_lower:
                    driver.flow_profile_result = "Novice"
                else:
                    driver.flow_profile_result = "Completed"

            # Airtable sync removed — data already loaded from Airtable

    def _load_race_reviews(self):
        """Load export (15).csv (Race Reviews)"""
        filename = self._first_available_data_file([
            'export (15).csv',
            'Race Weekend Review.csv',
        ])
        if not filename:
            return

        _rr_first = True
        for row in self._get_data_iter(filename):
            if _rr_first:
                print(f"  Race Review columns: {list(row.keys())}")
                _rr_first = False

            # ScoreApp header fix: detect numeric/UUID/# header in column A
            # and treat the value as full name
            if 'full name' not in row and 'full_name' not in row and 'name' not in row:
                keys = list(row.keys())
                if keys:
                    first_key = keys[0]
                    first_val = str(row.get(first_key, '')).strip()
                    header_is_odd = (first_key.replace('.', '').replace('-', '').isdigit()
                                    or first_key.startswith('#')
                                    or len(first_key) > 30)  # UUID-length
                    if header_is_odd and first_val and ' ' in first_val and '@' not in first_val:
                        row['full name'] = first_val

            email, first_name, last_name = self._resolve_email(row)
            if not email and not first_name and not last_name: continue

            driver = self._get_or_create_driver(email, first_name, last_name)

            # Parse Date
            date_val = (row.get('scorecard_finished_at') or row.get('submitted_at') or
                       row.get('submit_date_utc') or
                       row.get('submit date (utc)') or row.get('submit date') or
                       row.get('date', '') or row.get('timestamp', '') or
                       row.get('submitted at', ''))
            
            if not date_val:
                pass  # Silenced: was printing 80+ column keys per driver, causing log bloat
            
            submit_date = self._parse_date(date_val)
            
            if submit_date:
                # Update if new
                if not driver.race_weekend_review_date or submit_date > driver.race_weekend_review_date:
                    driver.race_weekend_review_date = submit_date
                    driver.race_weekend_review_status = "completed"
                    driver.last_activity = submit_date

                    # Auto-update stage to RACE_REVIEW_COMPLETE if driver hasn't progressed further
                    if driver.current_stage in [FunnelStage.CONTACT, FunnelStage.MESSAGED,
                                               FunnelStage.RACE_WEEKEND, FunnelStage.REPLIED,
                                               FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT]:
                        driver.current_stage = FunnelStage.RACE_REVIEW_COMPLETE

                    # Airtable sync removed — data already loaded from Airtable


# =============================================================================
# FUNNEL DASHBOARD
# =============================================================================

class FunnelDashboard:
    """Main dashboard for funnel management"""

    def __init__(self, data_dir: str, overrides: Optional[Dict[str, Any]] = None):
        self.data_dir = data_dir
        self.data_loader = DataLoader(data_dir, overrides=overrides)
        self.calculator = FunnelCalculator()
        self.rescue_manager = RescueMessageManager()
        self.followup_manager = FollowUpMessageManager()
        self.daily_stats = DailyStatsManager(data_dir) # Init Manual Stats
        # Initialize Race Manager after data load
        self.drivers: Dict[str, Driver] = {}
        
        # Load data first
        self.reload_data()
        
        # Now init race manager with populated loader
        self.race_manager = RaceResultManager(self.data_loader)

    @property
    def airtable(self):
        return self.data_loader.airtable

    def _find_driver(self, email: str) -> Optional['Driver']:
        """Alias-aware driver lookup (without creating new drivers).

        Delegates to DataLoader._find_driver_by_key which resolves aliases
        so callers don't need to know if a driver is stored under their
        full-name key or their email/slug key.
        """
        return self.data_loader._find_driver_by_key(email)

    def reload_data(self):
        """Reload all data from CSV files"""
        self.drivers = self.data_loader.load_all_data()
        self._calculate_conversion_rates()
        # Reload manual stats
        self.daily_stats = DailyStatsManager(self.data_dir)
        # Re-sync race manager if needed (though it shares reference)
        if hasattr(self, 'race_manager'):
            self.race_manager.refresh_data()
            
    # Proxy methods for Race Results
    def process_race_results(self, raw_names: List[str], event_name: str, championship: str = "") -> List[Dict]:
        return self.race_manager.process_race_results(raw_names, event_name, championship=championship)
        



    def cleanup_duplicates(self) -> int:
        """Deduplicate drivers in memory (post-load sweep already handles most cases)."""
        # The _deduplicate_drivers() method runs at load time.
        # This manual trigger re-runs it and returns count removed.
        before = len(self.drivers)
        self.data_loader._deduplicate_drivers()
        removed = before - len(self.drivers)
        return removed

    def generate_outreach_message(self, result: Dict, event_name: str) -> str:
        return self.race_manager.generate_outreach_message(result, event_name)




    def delete_driver(self, email: str):
        """Permanently delete a driver from Airtable, local DB, and memory.
        Airtable deletion is synchronous (with timeout) to prevent the driver
        being reloaded from Airtable when the dashboard cache refreshes."""
        email = email.lower().strip()
        driver = self._find_driver(email)
        driver_name = ""

        # 1. DELETE FROM AIRTABLE — synchronous with timeout so cache refresh
        #    doesn't resurrect the driver before the delete completes.
        if driver and self.data_loader.airtable:
            record_id = getattr(driver, 'airtable_record_id', None)
            driver_name = driver.full_name or f"{driver.first_name or ''} {driver.last_name or ''}".strip()

            def _bg_delete(at, rec_id, driver_name_inner, driver_email):
                try:
                    if rec_id:
                        at.delete_record(rec_id)
                        print(f"✅ Deleted Airtable record {rec_id} for {driver_email}")
                    elif driver_name_inner:
                        records = at.table.all(
                            formula=f'FIND(LOWER("{driver_name_inner}"), LOWER({{Full Name}}))',
                            max_records=3
                        )
                        for r in records:
                            if r['fields'].get('Full Name', '').lower().strip() == driver_name_inner.lower().strip():
                                at.delete_record(r['id'])
                                print(f"✅ Deleted Airtable record {r['id']} for {driver_name_inner}")
                                break
                except Exception as e:
                    print(f"❌ Airtable delete failed for {driver_email}: {e}")

            t = threading.Thread(
                target=_bg_delete,
                args=(self.data_loader.airtable, record_id, driver_name, email),
                daemon=True
            )
            t.start()
            t.join(timeout=5)  # Wait up to 5s for Airtable delete
            if t.is_alive():
                print(f"⚠️ Airtable delete still running for {email} (continuing anyway)")

            # Clear Airtable cache so reload doesn't resurrect the driver
            if hasattr(self.data_loader.airtable, 'drivers_cache'):
                self.data_loader.airtable.drivers_cache = None

        # 2. Local DB Delete
        self.data_loader.delete_driver_from_db(email)

        # 3. Memory Delete — remove ALL possible keys for this driver
        aliases = getattr(self.data_loader, '_key_aliases', {})
        resolved = aliases.get(email, email)

        # Also build name-based keys to catch all variants
        keys_to_remove = {email, resolved}
        if driver_name:
            keys_to_remove.add(driver_name.lower().strip())
            slug = driver_name.lower().strip().replace(' ', '_')
            slug = "".join([c for c in slug if c.isalnum() or c == '_'])
            keys_to_remove.add(f"no_email_{slug}")

        for key in keys_to_remove:
            if key in self.drivers:
                del self.drivers[key]
            # Also clean up aliases pointing to deleted keys
            stale_aliases = [a for a, v in aliases.items() if v == key]
            for a in stale_aliases:
                del aliases[a]

        print(f"[Delete] Removed {email} from memory (keys checked: {keys_to_remove})")

    def add_new_driver(self, email: str, first_name: str, last_name: str, fb_url: str, ig_url: str = "", championship: str = "", notes: str = None, follow_up_date: datetime = None) -> bool:
        """Add a new driver to the database"""
        # Save to Local CSV first (Redundancy)
        success = self.data_loader.add_new_driver_to_db(email, first_name, last_name, fb_url, ig_url=ig_url, championship=championship, notes=notes, follow_up_date=follow_up_date)
        
        if success:
            # Update In-Memory - pass first_name and last_name to ensure they're set
            self.drivers[email.lower()] = self.data_loader._get_or_create_driver(email, first_name, last_name)
            driver = self.drivers[email.lower()]

            # Explicitly set names (ensure they're always applied, even if empty check fails)
            driver.first_name = first_name.strip() if first_name else driver.first_name
            driver.last_name = last_name.strip() if last_name else driver.last_name
            if fb_url: driver.facebook_url = fb_url
            if ig_url: driver.instagram_url = ig_url
            
            if notes: driver.notes = notes
            if championship: driver.championship = championship
            if follow_up_date: driver.follow_up_date = follow_up_date

            # NOTE: Airtable sync already handled inside add_new_driver_to_db()
            # Removed duplicate migrate_driver_to_airtable() call that was causing
            # 3 extra blocking API round-trips on every new contact save.

        return success

    def update_driver_stage(self, email: str, new_stage: FunnelStage, sale_value: Optional[float] = None):
        """Manually update a driver's stage"""
        # Defensive: accept both FunnelStage enum and string values
        if isinstance(new_stage, str):
            for s in FunnelStage:
                if s.value == new_stage or s.name == new_stage:
                    new_stage = s
                    break
            else:
                print(f"WARNING: Unknown stage string '{new_stage}' — skipping update for {email}")
                return
        # Save to CSV and update in-memory
        self.data_loader.save_manual_update(email, new_stage.value)
        
        driver = self._find_driver(email)
        if driver:
            old_stage_str = driver.current_stage.value if driver.current_stage else 'Contact'
            driver.current_stage = new_stage
            driver.last_activity = datetime.now() # Update sort key
            
            # Auto follow-up timing matrix (hours until follow-up needed)
            _AUTO_FOLLOWUP = {
                FunnelStage.MESSAGED: 72,       # 3 days — first check-in
                FunnelStage.OUTREACH: 72,
                FunnelStage.REPLIED: 48,        # 2 days — keep momentum
                FunnelStage.LINK_SENT: 24,      # 1 day — results waiting
                FunnelStage.BLUEPRINT_LINK_SENT: 24,
                FunnelStage.RACE_WEEKEND: 48,   # 2 days — assessment nudge
                FunnelStage.REGISTERED: 24,
                FunnelStage.BLUEPRINT_STARTED: 24,
                FunnelStage.DAY1_COMPLETE: 24,
                FunnelStage.DAY2_COMPLETE: 24,
            }
            # Terminal stages — clear follow-up
            _CLEAR_FOLLOWUP = [
                FunnelStage.STRATEGY_CALL_BOOKED, FunnelStage.CLIENT,
                FunnelStage.SALE_CLOSED, FunnelStage.NOT_A_FIT,
                FunnelStage.NO_SOCIALS, FunnelStage.NO_SALE,
            ]

            # Logic: If moving to OUTREACH/MESSAGED, set outreach_date = now
            if new_stage in [FunnelStage.MESSAGED, FunnelStage.OUTREACH]:
                driver.outreach_date = datetime.now()

            elif new_stage == FunnelStage.REPLIED:
                driver.replied_date = datetime.now()
                if not driver.outreach_date: driver.outreach_date = datetime.now() # Fallback

            elif new_stage in [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT]:
                driver.link_sent_date = datetime.now()
                if not driver.outreach_date: driver.outreach_date = datetime.now()

            elif new_stage in [FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE]:
                driver.race_weekend_review_date = datetime.now()

            elif new_stage in [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED]:
                driver.registered_date = datetime.now()
                if not driver.outreach_date: driver.outreach_date = datetime.now()

            elif new_stage == FunnelStage.DAY1_COMPLETE:
                driver.day1_complete_date = datetime.now()
                if not driver.registered_date: driver.registered_date = datetime.now()

            elif new_stage == FunnelStage.DAY2_COMPLETE:
                driver.day2_complete_date = datetime.now()
                if not driver.day1_complete_date: driver.day1_complete_date = datetime.now()

            elif new_stage == FunnelStage.STRATEGY_CALL_BOOKED:
                driver.strategy_call_booked_date = datetime.now()

            elif new_stage == FunnelStage.SALE_CLOSED:
                driver.sale_closed_date = datetime.now()
                if sale_value is not None:
                     driver.sale_value = sale_value

                # Save revenue if applicable
                if sale_value:
                    self.data_loader.save_revenue(email, sale_value)

            # AUTO-SCHEDULE FOLLOW-UP based on timing matrix
            if new_stage in _AUTO_FOLLOWUP:
                driver.follow_up_date = datetime.now() + timedelta(hours=_AUTO_FOLLOWUP[new_stage])
            elif new_stage in _CLEAR_FOLLOWUP:
                driver.follow_up_date = None

            # Lightweight Airtable sync — Stage + relevant date + name
            # Fire-and-forget in background thread so UI doesn't freeze
            if self.airtable:
                sync_data = {
                    "Email": driver.email,
                    "First Name": driver.first_name,
                    "Last Name": driver.last_name,
                    "Stage": new_stage.value,
                    "Last Activity": datetime.now().strftime('%Y-%m-%d'),
                }
                # Include socials if available (ensures Airtable has full profile)
                if driver.facebook_url: sync_data["FB URL"] = driver.facebook_url
                if driver.instagram_url: sync_data["IG URL"] = driver.instagram_url
                # Include the relevant date field for ALL pipeline stages
                _now_str = datetime.now().strftime('%Y-%m-%d')
                if new_stage in [FunnelStage.MESSAGED, FunnelStage.OUTREACH]:
                    sync_data["Date Messaged"] = _now_str
                elif new_stage == FunnelStage.REPLIED:
                    sync_data["Date Replied"] = _now_str
                elif new_stage in [FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT]:
                    sync_data["Date Link Sent"] = _now_str
                elif new_stage in [FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE]:
                    sync_data["Date Race Review"] = _now_str
                elif new_stage in [FunnelStage.REGISTERED, FunnelStage.BLUEPRINT_STARTED]:
                    sync_data["Date Blueprint Started"] = _now_str
                elif new_stage == FunnelStage.DAY1_COMPLETE:
                    sync_data["Date Day 1 Assessment"] = _now_str
                elif new_stage == FunnelStage.DAY2_COMPLETE:
                    sync_data["Date Day 2 Assessment"] = _now_str
                elif new_stage == FunnelStage.STRATEGY_CALL_BOOKED:
                    sync_data["Date Strategy Call"] = _now_str
                elif new_stage in [FunnelStage.CLIENT, FunnelStage.SALE_CLOSED]:
                    sync_data["Date Sale Closed"] = _now_str

                _driver_name = f"{driver.first_name} {driver.last_name}".strip()
                _rec_id = getattr(driver, 'airtable_record_id', None)
                _desc = f"{_driver_name}: {old_stage_str} → {new_stage.value}"

                from sync_manager import sync_save
                _saved = sync_save(self.airtable, sync_data, record_id=_rec_id, description=_desc)

                # Audit log (non-critical — fire-and-forget is OK for logging)
                if _saved:
                    def _audit(at, rn, os, ns):
                        try:
                            at.table.api.table(
                                at.table.base_id if hasattr(at.table, 'base_id') else 'appOK1dNqufKg0bEd',
                                'Activity Log'
                            ).create({
                                'Driver': rn, 'Action': 'Stage Change',
                                'Old Stage': os, 'New Stage': ns,
                                'Source': 'streamlit',
                                'Timestamp': datetime.now().isoformat()
                            }, typecast=True)
                        except Exception:
                            pass
                    threading.Thread(target=_audit, args=(self.airtable, _driver_name, old_stage_str, new_stage.value), daemon=True).start()
                
            # --- AUTO-INCREMENT DAILY STATS ---
            # Ensure the dashboard metrics reflect this action immediately
            if self.daily_stats:
                if new_stage == FunnelStage.MESSAGED:
                    # Guess channel based on URLs (or default to FB if both or neither, usually FB is primary)
                    if driver.instagram_url and not driver.facebook_url:
                        self.daily_stats.increment_ig()
                    else:
                        self.daily_stats.increment_fb()
                        
                elif new_stage == FunnelStage.LINK_SENT:
                    self.daily_stats.increment_link()
        
        # Recalculate conversion rates as data changed
        self._calculate_conversion_rates()


    # ==========================================
    # DAILY DASHBOARD METRICS


    # ==========================================
    # DAILY DASHBOARD METRICS
    # ==========================================
    
    def get_daily_metrics(self, target_date: Optional[datetime.date] = None) -> Dict:
        """Get counts of activities for a specific date (default Today)"""
        if not target_date:
            target_date = datetime.now().date()
            
        # 1. Get Manual Stats
        manual = self.daily_stats.get_stats_for_date(target_date)
            
        metrics = {
            'outreach_sent': 0, # Legacy total
            # Manual Metrics
            'fb_sent': manual.fb_messages_sent,
            'ig_sent': manual.ig_messages_sent,
            'links_sent': manual.links_sent,
            
            # Automated Metrics
            'new_registered': 0,
            'day1_completed': 0,
            'day2_completed': 0,
            'calls_booked': 0,
            'sales_closed': 0
        }
        
        for r in self.drivers.values():
            # Check each date field
            if r.outreach_date and r.outreach_date.date() == target_date:
                metrics['outreach_sent'] += 1
            if r.registered_date and r.registered_date.date() == target_date:
                metrics['new_registered'] += 1
            if r.day1_complete_date and r.day1_complete_date.date() == target_date:
                metrics['day1_completed'] += 1
            if r.day2_complete_date and r.day2_complete_date.date() == target_date:
                metrics['day2_completed'] += 1
            if r.strategy_call_booked_date and r.strategy_call_booked_date.date() == target_date:
                metrics['calls_booked'] += 1
            if r.sale_closed_date and r.sale_closed_date.date() == target_date:
                metrics['sales_closed'] += 1
                
        return metrics

    def get_stalled_drivers(self, days_threshold: int = 1) -> Dict[str, List[Dict]]:
        """Identify drivers stuck in a stage longer than threshold"""
        stalled = {
            'registered_no_start': [], # Reg -> Day 1 stuck
            'day1_no_day2': [],        # Day 1 -> Day 2 stuck
            'day2_no_call': [],        # Day 2 -> Call stuck
            'outreach_no_reply': []    # Outreach -> stuck (maybe too noisy?)
        }
        
        for r in self.drivers.values():
            days_in = r.days_in_current_stage
            if days_in is None or days_in < days_threshold:
                continue
                
            info = {
                'name': r.full_name or r.email,
                'email': r.email,
                'days': days_in,
                'stage': r.current_stage.value,
                'fb': r.facebook_url
            }

            if r.current_stage == FunnelStage.REGISTERED:
                stalled['registered_no_start'].append(info)
            elif r.current_stage == FunnelStage.DAY1_COMPLETE:
                stalled['day1_no_day2'].append(info)
            elif r.current_stage == FunnelStage.DAY2_COMPLETE:
                stalled['day2_no_call'].append(info)
            elif r.current_stage == FunnelStage.OUTREACH:
                stalled['outreach_no_reply'].append(info)
                
        return stalled

    def get_revenue_metrics(self) -> Dict:
        """Calculate revenue progress"""
        target = float(self.calculator.config.MONTHLY_REVENUE_TARGET)
        actual = 0.0
        pipeline_value = 0.0
        
        program_cost = 4000.0
        
        for r in self.drivers.values():
            if r.current_stage == FunnelStage.SALE_CLOSED:
                actual += r.sale_value if r.sale_value else program_cost
            elif r.current_stage == FunnelStage.STRATEGY_CALL_BOOKED:
                pipeline_value += program_cost * 0.25 
                
        return {
            'target': target,
            'actual': actual,
            'pipeline': pipeline_value,
            'progress_pct': (actual / target) * 100 if target > 0 else 0
        }

    def _calculate_conversion_rates(self):
        """Calculate actual conversion rates from data"""
        stage_counts = self.get_stage_counts()

        # Only update if we have meaningful data
        if stage_counts['registered'] > 10:
            rates = {}

            if stage_counts['registered'] > 0:
                rates['registration_to_day1'] = (
                    stage_counts['day1_complete'] / stage_counts['registered']
                )

            if stage_counts['day1_complete'] > 0:
                rates['day1_to_day2'] = (
                    stage_counts['day2_complete'] / stage_counts['day1_complete']
                )

            if stage_counts['day2_complete'] > 0:
                rates['day2_to_strategy_call'] = (
                    stage_counts['strategy_call_booked'] / stage_counts['day2_complete']
                )

            self.calculator.update_conversion_rates(rates)

    def get_stage_counts(self) -> Dict[str, int]:
        """Get count of drivers at each stage"""
        counts = defaultdict(int)

        for driver in self.drivers.values():
            stage_name = driver.current_stage.value
            counts[stage_name] += 1

            # Also count total who reached each stage (not just current)
            if driver.registered_date:
                counts['total_registered'] += 1
            if driver.day1_complete_date:
                counts['total_day1'] += 1
            if driver.day2_complete_date:
                counts['total_day2'] += 1
            if driver.strategy_call_booked_date:
                counts['total_calls_booked'] += 1

        # Map to simpler names
        return {
            'registered': counts['total_registered'],
            'day1_complete': counts['total_day1'],
            'day2_complete': counts['total_day2'],
            'strategy_call_booked': counts['total_calls_booked'],
            'current_registered': counts['registered'],
            'current_day1': counts['day1_complete'],
            'current_day2': counts['day2_complete'],
            'current_calls': counts['strategy_call_booked'],
        }

    def get_stage_counts_by_month(self, year: int, month: int) -> Dict[str, int]:
        """Get counts for a specific month (MTD Actuals)"""
        counts = defaultdict(int)
        
        for driver in self.drivers.values():
            # Helper to check date match
            def is_in_month(dt):
                return dt and dt.year == year and dt.month == month

            if is_in_month(driver.registered_date):
                counts['registered'] += 1
            if is_in_month(driver.day1_complete_date):
                counts['day1_complete'] += 1
            if is_in_month(driver.day2_complete_date):
                counts['day2_complete'] += 1
            if is_in_month(driver.strategy_call_booked_date):
                counts['strategy_call_booked'] += 1
                
        return counts

    def get_funnel_summary(self) -> str:
        """Generate a text summary of the funnel"""
        counts = self.get_stage_counts()
        targets = self.calculator.calculate_targets()

        lines = [
            "=" * 60,
            "CAMINO COACHING - FUNNEL DASHBOARD",
            f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "=" * 60,
            "",
            "📊 MONTHLY TARGETS",
            "-" * 40,
            f"Revenue Target:      £{targets.monthly_revenue:,.0f}",
            f"Sales Needed:        {targets.monthly_sales}",
            f"Strategy Calls:      {targets.monthly_strategy_calls}",
            f"Day 2 Completions:   {targets.monthly_day2_completions}",
            f"Day 1 Completions:   {targets.monthly_day1_completions}",
            f"Registrations:       {targets.monthly_registrations}",
            f"Total Outreach:      {targets.monthly_outreach}",
            "",
            "📈 CURRENT FUNNEL STATE",
            "-" * 40,
            f"Total Drivers in System:   {len(self.drivers)}",
            f"Registered:               {counts['registered']}",
            f"Day 1 Complete:           {counts['day1_complete']}",
            f"Day 2 Complete:           {counts['day2_complete']}",
            f"Strategy Calls Booked:    {counts['strategy_call_booked']}",
            "",
            "📉 CONVERSION RATES (Actual)",
            "-" * 40,
        ]

        for rate_name, rate_value in self.calculator.conversion_rates.items():
            lines.append(f"{rate_name}: {rate_value*100:.1f}%")

        # Add rescue needed section
        rescue_needed = self.rescue_manager.get_drivers_needing_rescue(list(self.drivers.values()))

        lines.extend([
            "",
            "🆘 DRIVERS NEEDING RESCUE",
            "-" * 40,
            f"Day 1 Rescue Needed:          {len(rescue_needed['day1_rescue'])}",
            f"Day 2 Rescue Needed:          {len(rescue_needed['day2_rescue'])}",
            f"Strategy Call Rescue Needed:  {len(rescue_needed['strategy_call_rescue'])}",
        ])

        # Add daily action items
        lines.extend([
            "",
            "📋 DAILY TARGETS",
            "-" * 40,
            f"Outreach Messages:   {targets.daily_outreach} per day",
            f"  - Email:           {targets.daily_outreach // 3}",
            f"  - Facebook DM:     {targets.daily_outreach // 3}",
            f"  - Instagram DM:    {targets.daily_outreach // 3}",
            "",
            "=" * 60,
        ])

        return "\n".join(lines)

    def get_rescue_actions(self) -> str:
        """Get list of rescue actions needed today"""
        rescue_needed = self.rescue_manager.get_drivers_needing_rescue(list(self.drivers.values()))

        lines = [
            "=" * 60,
            "🆘 RESCUE ACTIONS NEEDED",
            "=" * 60,
        ]

        for rescue_type, drivers in rescue_needed.items():
            if not drivers:
                continue

            lines.append(f"\n{rescue_type.upper().replace('_', ' ')} ({len(drivers)} drivers)")
            lines.append("-" * 40)

            for driver in drivers[:10]:  # Show top 10
                days = driver.days_in_current_stage
                lines.append(f"  • {driver.full_name} ({driver.email})")
                lines.append(f"    Stuck for: {days} days")

                # Get message preview
                msg = self.rescue_manager.get_rescue_message(rescue_type, driver, 'dm')
                preview = msg['body'][:100] + "..." if len(msg['body']) > 100 else msg['body']
                lines.append(f"    Message: {preview}")
                lines.append("")

            if len(drivers) > 10:
                lines.append(f"  ... and {len(drivers) - 10} more")

        return "\n".join(lines)

    def export_daily_report(self, filename: str = None) -> str:
        """Export a daily report to file"""
        if not filename:
            filename = f"daily_report_{datetime.now().strftime('%Y%m%d')}.txt"

        filepath = os.path.join(self.data_dir, filename)

        report = self.get_funnel_summary() + "\n\n" + self.get_rescue_actions()

        with open(filepath, 'w') as f:
            f.write(report)

        return filepath








# =============================================================================
# RACE RESULT MANAGER (Restored)
# =============================================================================
class SocialFinder:
    """Find social media profiles and generate Deep DM Links"""
    
    def find_socials(self, name: str, context: str = "") -> Dict[str, str]:
        """
        Search for social media profiles using multi-level strategy.
        Returns dict of {platform: url}
        """
        # Level 1: Core Racing Search
        # Level 2: Social Specific
        queries = [
            f'"{name}" site:instagram.com ("racing" OR "racer" OR "motorsport")',
            f'"{name}" site:facebook.com ("motorcycle" OR "racing")',
            f'"{name}" {context} racing social media',
            f'"{name}" AND ("competitor" OR "race results")'
        ]
        
        found = {}
        
        try:
            from googlesearch import search
            
            # Use the most specific one first
            base_query = queries[0] 
            
            # Search top 15 results
            results = list(search(base_query, num_results=15, advanced=True))
            
            for result in results:
                url = result.url
                lower_url = url.lower()
                
                if "facebook.com" in lower_url and "public" not in lower_url and "posts" not in lower_url:
                    if "facebook_url" not in found:
                        found['facebook_url'] = url
                        
                elif "instagram.com" in lower_url:
                    if "instagram_url" not in found:
                        # Clean out some junk params if needed
                        found['instagram_url'] = url
                        
                elif "linkedin.com/in" in lower_url:
                    if "linkedin_url" not in found:
                        found['linkedin_url'] = url
                        
        except ImportError:
            print("googlesearch-python not installed")
        except Exception as e:
            print(f"Search error: {e}")
            
        return found

    def clean_social_url(self, url: str) -> Optional[str]:
        """Extract username/handle from a raw URL"""
        if not url: return None
        
        # Basic cleanup
        clean = url.strip().rstrip('/')
        
        # Remove query params
        if '?' in clean:
            clean = clean.split('?')[0]
            
        return clean

    def generate_deep_dm_link(self, platform: str, url: str, message: str = "") -> Optional[str]:
        """
        Generate a direct 'Mobile First' deep link for DMs.
        - Facebook: m.me/{username}
        - Instagram: ig.me/m/{username}
        """
        import urllib.parse
        
        if not url: return None
        
        clean_url = self.clean_social_url(url)
        username = clean_url.split('/')[-1]
        
        # Safety check: if username is 'profile.php', we might need ID parsing (skip for now)
        if 'profile.php' in username:
            return None 
            
        encoded_msg = urllib.parse.quote(message)
        
        if platform == 'facebook':
            # https://m.me/<USERNAME>?text=<MESSAGE>
            return f"https://m.me/{username}?text={encoded_msg}"
            
        elif platform == 'instagram':
            # https://ig.me/m/<USERNAME>?text=<MESSAGE>
            return f"https://ig.me/m/{username}?text={encoded_msg}"
            
        return None

    def generate_deep_search_links(self, name: str, event_name: str = "") -> Dict[str, str]:
        """
        Generate Google Dork URLs for manual Deep Search.
        Based on User's Level 1-4 Operators.
        """
        import urllib.parse
        
        def make_link(query):
            return f"https://www.google.com/search?q={urllib.parse.quote(query)}"
            
        links = {}
        
        # Level 1: Core Discovery
        links['🔍 Core Discovery'] = make_link(f'"{name}" AND ("racing" OR "racer" OR "motorsport" OR "car")')
        
        # Level 2: Socials (Direct Platform Search preferred by User)
        # Facebook: Direct search (shows mutuals)
        links['👥 Facebook Direct'] = f"https://www.facebook.com/search/people/?q={urllib.parse.quote(name)}"
        
        # Instagram: No reliable web search URL, so we link to Home for pasting
        links['📸 Instagram Direct'] = "https://www.instagram.com/"
        
        # Keep Google backups just in case
        links['(Backup) IG Google'] = make_link(f'"{name}" site:instagram.com ("racing" OR "track day" OR "moto")')
        
        # Level 3: Context / Event
        if event_name:
            links['🏁 Event check'] = make_link(f'"{name}" AND "{event_name}" ("race results" OR "competitor")')
            
        # Level 4: Validation (Associations)
        links['📋 Racing Org Check'] = make_link(f'"{name}" AND ("CVMA" OR "WERA" OR "ASMA" OR "CRA")')
        links['⏱️ Lap Times'] = make_link(f'"{name}" AND ("lap times" OR "race monitor" OR "mylaps")')
        
        return links

    def generate_search_link(self, name: str) -> str:
        # Legacy fallback
        return self.generate_deep_search_links(name)['🔍 Core Discovery']


class RaceResultManager:
    """Manages race result analysis and outreach generation"""

    def __init__(self, data_loader: DataLoader):
        self.data_loader = data_loader
        self.drivers = data_loader.drivers
        self.name_index = {} # O(1) Lookup
        self._build_index()
        self.social_finder = SocialFinder()
        self.circuit_file = os.path.join(data_loader.data_dir, "race_circuits.json")
        self.circuits = self._load_circuits()

    def _normalize(self, text: str) -> str:
        """Remove accents and lowercase"""
        if not text: return ""
        import unicodedata
        # NFD decomposition splits characters and accents
        # Mn checks for non-spacing mark
        normalized = unicodedata.normalize('NFD', text)
        return "".join(c for c in normalized if unicodedata.category(c) != 'Mn').lower().strip()

    def _build_index(self):
        """Build O(1) lookup map for names (Strict + Normalized + Raw Airtable)"""
        self.name_index = {}
        for r in self.drivers.values():
            # Index by computed full_name (first + last)
            if r.full_name:
                # 1. Strict Lowercase
                self.name_index[r.full_name.lower().strip()] = r

                # 2. Normalized (No Accents)
                # This solves 'Víctor' vs 'Victor'
                norm_name = self._normalize(r.full_name)
                if norm_name:
                    self.name_index[norm_name] = r

            # 3. ALSO index by raw_full_name from Airtable (if different)
            # This catches cases where Airtable "Full Name" differs from First+Last
            if r.raw_full_name:
                raw_key = r.raw_full_name.lower().strip()
                if raw_key and raw_key not in self.name_index:
                    self.name_index[raw_key] = r

                # Also normalize the raw name
                norm_raw = self._normalize(r.raw_full_name)
                if norm_raw and norm_raw not in self.name_index:
                    self.name_index[norm_raw] = r

            # 4. Nickname variant entries — so "Christopher Binker" finds "Chris Binker"
            # and vice versa. Only add if the variant isn't already another driver.
            fn = (r.first_name or '').lower().strip()
            ln = (r.last_name or '').lower().strip()
            if fn and ln and fn in NICKNAME_MAP:
                alt_fn = NICKNAME_MAP[fn]
                alt_full = f"{alt_fn} {ln}"
                if alt_full not in self.name_index:
                    self.name_index[alt_full] = r
                alt_norm = self._normalize(alt_full)
                if alt_norm and alt_norm not in self.name_index:
                    self.name_index[alt_norm] = r

    def refresh_data(self):
        """Refresh references and rebuild index"""
        self.drivers = self.data_loader.drivers
        self._build_index()

    def _load_circuits(self) -> List[str]:
        if os.path.exists(self.circuit_file):
            try:
                with open(self.circuit_file, 'r') as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def save_circuit(self, name: str):
        if not name: return
        name = name.strip()
        if name not in self.circuits:
            self.circuits.append(name)
            self.circuits.sort()
            with open(self.circuit_file, 'w') as f:
                json.dump(self.circuits, f)
                
    def get_all_circuits(self) -> List[str]:
        return self.circuits

    def match_driver(self, raw_name: str, championship: str = "") -> Optional[Driver]:
        """Attempt to match a raw name from results to a database driver.
        Triple-check: First Name + Last Name + Championship."""
        if not raw_name:
            return None
            
        # 1. Normalization
        clean_raw = raw_name.lower().strip()
        norm_raw = self._normalize(raw_name)
        
        # 2. Exact Match (O(1) Lookup) - Checks strict and normalized via index
        if clean_raw in self.name_index:
            return self.name_index[clean_raw]
        if norm_raw in self.name_index:
            return self.name_index[norm_raw]
                
        # 3. Try "Last, First" swap -> "First Last"
        if ',' in clean_raw:
            parts = clean_raw.split(',')
            if len(parts) >= 2:
                swapped = f"{parts[1].strip()} {parts[0].strip()}"
                norm_swapped = self._normalize(swapped)
                
                if swapped in self.name_index: return self.name_index[swapped]
                if norm_swapped in self.name_index: return self.name_index[norm_swapped]
                       
        # 4. Token-Based Match (Stricter + Normalized)
        # Handle "Victor Perez de Leon" vs "Victor Pérez de León"
        
        # Nickname map: use module-level NICKNAME_MAP constant
        
        def _expand_tokens(tokens):
            """Expand tokens with nickname equivalents."""
            expanded = set(tokens)
            for t in tokens:
                if t in NICKNAME_MAP:
                    expanded.add(NICKNAME_MAP[t])
            return expanded
        
        raw_tokens = set(norm_raw.split())
        raw_expanded = _expand_tokens(raw_tokens)
        
        for email, driver in self.drivers.items():
            # SANITY CHECK: Ignore corrupt drivers with massive names
            if len(driver.full_name) > 60:
                continue

            # Compare Normalized Tokens
            db_norm = self._normalize(driver.full_name)
            db_tokens = set(db_norm.split())
            
            if not db_tokens: continue
            
            db_expanded = _expand_tokens(db_tokens)
            common = raw_expanded.intersection(db_expanded)
            
            # CRITERIA:
            # If DB Name is longer than 1 word, require at least 2 matches (First + Last)
            if len(db_tokens) >= 2:
                if len(common) >= 2:
                    return driver
            
        # FUZZY MATCHING DISABLED - Was causing false positives
        # "Chris Denley" was matching "Chris Exley" (wrong person!)
        # Only EXACT name matches are allowed now

        # 5. TRIPLE CHECK: First Name + Last Name (starts-with) + Championship
        # Catches: "Kensei Matsudaira" matching "Kensei Matsudaira #74" in CVMA
        # Also catches abbreviated last names like "D" matching "Dabalos"
        raw_parts = clean_raw.split()
        if len(raw_parts) >= 2:
            search_first = raw_parts[0]
            search_last = ' '.join(raw_parts[1:])  # Could be multi-word
            search_last_clean = search_last.replace('.', '').strip()
            search_champ = championship.lower().strip() if championship else ''

            for email, driver in self.drivers.items():
                if len(driver.full_name) > 60: continue
                r_first = (driver.first_name or '').lower().strip()
                r_last = (driver.last_name or '').lower().strip()
                r_champ = (driver.championship or '').lower().strip()

                # First name must match (or be a nickname)
                first_ok = (r_first == search_first or
                           r_first.startswith(search_first) or
                           search_first.startswith(r_first))
                if not first_ok:
                    if search_first in NICKNAME_MAP:
                        first_ok = NICKNAME_MAP[search_first] == r_first or r_first.startswith(NICKNAME_MAP[search_first])
                    if not first_ok and r_first in NICKNAME_MAP:
                        first_ok = NICKNAME_MAP[r_first] == search_first or search_first.startswith(NICKNAME_MAP[r_first])
                if not first_ok:
                    continue

                # Last name: either starts-with match or contained-in
                last_ok = False
                if search_last_clean and r_last:
                    # "Matsudaira" matches "Matsudaira #74" (r_last starts with search)
                    # "D" matches "Dabalos" (r_last starts with search initial)
                    last_ok = (r_last.startswith(search_last_clean) or
                              search_last_clean.startswith(r_last.split()[0]) or
                              r_last.split()[0] == search_last_clean)
                if not last_ok:
                    continue

                # Championship: if provided, must match
                if search_champ and r_champ:
                    champ_parts = [c.strip() for c in r_champ.split(',')]
                    if not any(search_champ in p or p in search_champ for p in champ_parts):
                        continue

                return driver
        # If you need fuzzy matching back, the user must manually link contacts
             
        # 6. Slug / ID Fallback
        # If the input name generates a slug that matches a driver ID/Email, it's a match.
        # e.g. "Brian Hull" -> "brian_hull". If "brian_hull" is a key in drivers, match it.
        try:
            slug_try = clean_raw.replace(' ', '_')
            slug_clean = "".join([c for c in slug_try if c.isalnum() or c == '_'])
            if slug_clean in self.drivers:
                return self.drivers[slug_clean]
        except: pass
             
        return None

    def process_race_results(self, raw_names: List[str], event_name: str, championship: str = "") -> List[Dict]:
        """Process a list of names and return match status.
        If championship is provided, cross-championship name matches are
        treated as new prospects (e.g. 'Phil Smith' in CVMA ≠ 'Phil Smith' in BSB)."""
        import re
        results = []
        for name in raw_names:
            if not name.strip():
                continue

            # Remove trailing position numbers or gaps if simple split
            # Often PDF extracts might be "Name 1:23.456"
            # We assume the input is relatively clean list of names

            # --- SANITIZATION ---
            # Remove artifacts if user copy-pasted from the app output (e.g. "🆕 Name [NEW PROSPECT]")
            clean_name = re.sub(r'[🆕\u200b]', '', name) # Remove logic emoji
            clean_name = re.sub(r'\[.*?\]', '', clean_name) # Remove [TAGS]
            clean_name = re.sub(r'\s+', ' ', clean_name).strip() # Collapse whitespace

            match = self.match_driver(clean_name, championship=championship)

            # Championship guard: if we're doing outreach for a specific championship
            # and the matched driver belongs to a DIFFERENT championship, treat as new prospect.
            # BUT: never un-match drivers who've been contacted (Messaged, Replied, Link Sent etc.)
            # — they are clearly the same person regardless of championship field.
            if match and championship:
                driver_champ = (match.championship or "").strip().lower()
                search_champ = championship.strip().lower()
                # Contacted stages — these drivers are definitely real matches, don't un-match
                _contacted = {FunnelStage.MESSAGED, FunnelStage.OUTREACH, FunnelStage.REPLIED,
                              FunnelStage.LINK_SENT, FunnelStage.BLUEPRINT_LINK_SENT,
                              FunnelStage.RACE_WEEKEND, FunnelStage.RACE_REVIEW_COMPLETE,
                              FunnelStage.BLUEPRINT_STARTED, FunnelStage.DAY1_COMPLETE,
                              FunnelStage.DAY2_COMPLETE, FunnelStage.STRATEGY_CALL_BOOKED,
                              FunnelStage.CLIENT, FunnelStage.SALE_CLOSED}
                if driver_champ and search_champ and match.current_stage not in _contacted:
                    # Check if ANY part of the driver's championship contains the search term
                    # (handles comma-separated: "Chuckwalla, CVMA")
                    champ_parts = [c.strip().lower() for c in driver_champ.split(',')]
                    if not any(search_champ in part or part in search_champ for part in champ_parts):
                        match = None
            
            status = "match_found" if match else "new_prospect"
            
            # DEBUG DIAGNOSTICS FOR FAILED MATCHES
            debug_note = ""
            if not match:
                # Re-run fuzzy logic to find "What was the closest?"
                import difflib
                norm_raw = self._normalize(clean_name)
                best_s = 0.0
                best_n = ""
                for email, r in self.drivers.items():
                    if len(r.full_name) > 60: continue
                    d_norm = self._normalize(r.full_name)
                    ratio = difflib.SequenceMatcher(None, norm_raw, d_norm).ratio()
                    if ratio > best_s:
                        best_s = ratio
                        best_n = r.full_name
                
                # Check for "Exact String" failure (e.g. whitespace)
                # If Score is High but match failed, it means threshold not met logic error?
                debug_note = f"(Closest: '{best_n}' {best_s:.2f})"
                
                # Check Drivers Count
                if len(self.drivers) < 10:
                    debug_note += " [DB EMPTY WARN]"

            # Determine appropriate stage/context
            current_stage = match.current_stage.value if match else "New"
            
            results.append({
                "original_name": clean_name,
                "match_status": status,
                "match": match, # Internal object
                "matched_email": match.email if match else None,
                "facebook_url": match.facebook_url if match else None,
                "current_stage": current_stage,
                "debug_note": debug_note
            })
        return results

    def generate_outreach_message(self, result: Dict, event_name: str) -> str:
        """Generate a context-aware message based on User Templates"""
        name = result['original_name']
        match = result['match']
        
        # Split first name
        first_name = name.split(' ')[0].title()
        if match and match.first_name:
             first_name = match.first_name
             
        # TEMPLATE: SEQUENCE 1 (Qualifying Struggle -> Free Training)
        # Context: saw them race, maybe qualified well but finished lower, or just general outreach
        # We will adapt the "Opening" message from the PDF
        
        if result['match_status'] == 'match_found' and match:
            # Context: Existing Contact
            if match.race_weekend_review_status == 'completed':
                return f"Hey {first_name}, great to see you out at {event_name}! Saw you already did your review - how are you feeling about the progress since then?"
            
            # BLUEPRINT SPECIFIC
            elif match.registered_date:
                 return f"Hey {first_name}, great to see you out at {event_name} the other week! How was it, did anything from the Podium Contenders Blueprint show up for you?"

            else:
                return self._random_cold_outreach(first_name, event_name)
        else:
             # Context: Cold / New
             return self._random_cold_outreach(first_name, event_name)

    def _random_cold_outreach(self, first_name, event_name):
        """Generate a randomized cold outreach message to avoid automated detection."""
        import random
        greetings = ["Hey", "Hi", "Hello", "Hiya"]
        middles = [
            f"I see you were out at {event_name} at the weekend.",
            f"I noticed you were at {event_name} at the weekend.",
            f"saw you were out at {event_name} at the weekend.",
            f"I see you were racing at {event_name} at the weekend.",
        ]
        closings = [
            "How was it for you?",
            "How did it go?",
            "How was the weekend?",
            "How did you get on?",
            "How was it?",
            "How did it go for you?",
        ]
        return f"{random.choice(greetings)} {first_name}, {random.choice(middles)} {random.choice(closings)}"

    def find_socials_for_prospect(self, name: str, context: str) -> Dict[str, str]:
        """Find socials for a prospect"""
        return self.social_finder.find_socials(name, context)
    
    def get_manual_search_link(self, name: str) -> str:
        return self.social_finder.generate_search_link(name)


# ═══════════════════════════════════════════════════════════════════════════
# RACE RESULTS STORAGE — stored as structured [RESULTS]...[/RESULTS] block
# in driver notes. Each entry is a JSON dict with session results.
# ═══════════════════════════════════════════════════════════════════════════

def parse_race_results(notes: str) -> list:
    """Extract stored race results from driver notes.
    Returns list of result dicts sorted by date (newest first).
    """
    if not notes:
        return []
    import re, json
    match = re.search(r'\[RESULTS\](.*?)\[/RESULTS\]', notes, re.DOTALL)
    if not match:
        return []
    try:
        results = json.loads(match.group(1).strip())
        if isinstance(results, list):
            return sorted(results, key=lambda x: x.get('date', ''), reverse=True)
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def save_race_result(driver, circuit: str, championship: str, session_results: list,
                     event_date: str = "") -> str:
    """Append race results for a driver. Returns updated notes string.

    Args:
        driver: Driver object (needs .notes)
        circuit: Track name
        championship: Series name
        session_results: List of dicts from Speedhive, each with:
            session_name, session_type, position, position_in_class,
            best_lap, total_time, laps, best_speed, result_class, status
        event_date: ISO date string for the event
    """
    import re, json

    if not event_date:
        event_date = datetime.now().strftime("%Y-%m-%d")

    existing = parse_race_results(driver.notes or "")

    # Add new results (one entry per session)
    for sr in session_results:
        entry = {
            "date": event_date,
            "circuit": circuit,
            "championship": championship,
            "session": sr.get("session_group") or sr.get("session_name", ""),
            "type": sr.get("session_type", "race"),
            "pos": sr.get("position"),
            "pos_class": sr.get("position_in_class"),
            "best_lap": sr.get("best_lap", ""),
            "total_time": sr.get("total_time", ""),
            "laps": sr.get("laps", 0),
            "speed": sr.get("best_speed", 0),
            "class": sr.get("result_class", ""),
            "status": sr.get("status", "Normal"),
        }
        # Avoid duplicates (same date + session)
        _dup = any(
            e.get("date") == entry["date"] and e.get("session") == entry["session"]
            for e in existing
        )
        if not _dup:
            existing.append(entry)

    # Sort by date descending
    existing.sort(key=lambda x: x.get("date", ""), reverse=True)

    # Build the results block
    results_json = json.dumps(existing)
    results_block = f"[RESULTS]\n{results_json}\n[/RESULTS]"

    # Replace or insert in notes
    notes = driver.notes or ""
    if "[RESULTS]" in notes:
        notes = re.sub(
            r'\[RESULTS\].*?\[/RESULTS\]',
            results_block,
            notes,
            flags=re.DOTALL
        )
    else:
        notes = f"{results_block}\n{notes}" if notes else results_block

    return notes


def get_results_summary(notes: str) -> dict:
    """Get a quick summary of driver's race history.

    Returns dict with:
        count: number of recorded results
        best_pos: best overall position
        best_lap: fastest lap time string
        best_circuit: circuit where best position was achieved
        latest: most recent result dict
        trend: "improving", "declining", "stable", or "new"
    """
    results = parse_race_results(notes)
    if not results:
        return {"count": 0, "trend": "new"}

    # Filter out DNS/DNF for position stats
    valid = [r for r in results if r.get("status") == "Normal" and r.get("pos")]
    races = [r for r in valid if r.get("type") == "race"]

    summary = {
        "count": len(results),
        "latest": results[0] if results else None,
    }

    if races:
        best = min(races, key=lambda r: r["pos"])
        summary["best_pos"] = best["pos"]
        summary["best_circuit"] = best.get("circuit", "")

    # Best lap across all sessions
    lap_times = [r["best_lap"] for r in valid if r.get("best_lap") and r["best_lap"] != "00.000"]
    if lap_times:
        summary["best_lap"] = min(lap_times)  # Lexicographic works for MM:SS.mmm format

    # Trend: compare last 2 race weekends
    race_dates = sorted(set(r["date"] for r in races), reverse=True) if races else []
    if len(race_dates) >= 2:
        latest_races = [r for r in races if r["date"] == race_dates[0]]
        prev_races = [r for r in races if r["date"] == race_dates[1]]
        if latest_races and prev_races:
            avg_latest = sum(r["pos"] for r in latest_races) / len(latest_races)
            avg_prev = sum(r["pos"] for r in prev_races) / len(prev_races)
            if avg_latest < avg_prev - 0.5:
                summary["trend"] = "improving"
            elif avg_latest > avg_prev + 0.5:
                summary["trend"] = "declining"
            else:
                summary["trend"] = "stable"
    else:
        summary["trend"] = "new"

    return summary
