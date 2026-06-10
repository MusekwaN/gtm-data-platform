# ai_agents/lead_scoring/intent_agent.py
"""
Intent Agent — detects buying signals and
calculates an AI-powered intent score.
"""

import json
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from ai_agents.lead_scoring.base_agent import BaseAgent


class IntentAgent(BaseAgent):

    def __init__(self):
        super().__init__(name="IntentAgent")

    def run(self, lead: dict) -> dict:
        """
        Analyze lead signals and return intent score + reasoning.
        """
        self.logger.info(
            f"Detecting intent: {lead.get('full_name')}"
        )

        system_prompt = """You are a B2B sales intent analyst.
Analyze the lead signals and determine buying intent.
Always respond with valid JSON only. No explanation, no markdown.
JSON must have exactly these keys:
- intent_score: integer 0-100
- intent_level: one of [cold, cool, warm, hot]
- intent_signals: list of 3 observed signals that indicate intent
- urgency: one of [low, medium, high, critical]
- deal_size_estimate: one of [smb, mid_market, enterprise]
- next_best_action: specific action the sales rep should take now
- ai_reasoning: 1-2 sentences explaining the intent assessment"""

        user_prompt = f"""Analyze this lead for buying intent:

Name:             {lead.get('full_name', 'Unknown')}
Title:            {lead.get('job_title', 'Unknown')}
Seniority:        {lead.get('seniority', 'Unknown')}
Company:          {lead.get('company_name', 'Unknown')}
Industry:         {lead.get('industry', 'Unknown')}
Employee Count:   {lead.get('employee_count', 'Unknown')}
Lead Status:      {lead.get('lead_status', 'Unknown')}
Company Stage:    {lead.get('company_stage', 'Unknown')}
Buying Readiness: {lead.get('buying_readiness', 'Unknown')}
Pain Points:      {lead.get('pain_points', [])}
Data Sources:     {lead.get('sources', 'unknown')}
Record Count:     {lead.get('record_count', 1)}

Assess buying intent as JSON."""

        response = self.call_llm(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.2,
            max_tokens=400
        )

        try:
            intent = json.loads(response)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse intent JSON")
            intent = {
                "intent_score":       50,
                "intent_level":       "cool",
                "intent_signals":     [],
                "urgency":            "medium",
                "deal_size_estimate": "smb",
                "next_best_action":   "Send introductory email",
                "ai_reasoning":       "Unable to assess intent",
            }

        return {**lead, **intent}