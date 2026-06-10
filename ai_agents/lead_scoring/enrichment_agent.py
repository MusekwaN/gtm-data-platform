# ai_agents/lead_scoring/enrichment_agent.py
"""
Enrichment Agent — researches a company and person,
adds context that raw data doesn't have.
"""

import json
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from ai_agents.lead_scoring.base_agent import BaseAgent


class EnrichmentAgent(BaseAgent):

    def __init__(self):
        super().__init__(name="EnrichmentAgent")

    def run(self, lead: dict) -> dict:
        """
        Given a lead record, generate company and person insights.
        Returns enriched fields to add to the lead.
        """
        self.logger.info(
            f"Enriching: {lead.get('full_name')} @ {lead.get('company_name')}"
        )

        system_prompt = """You are a B2B sales intelligence analyst.
Given a lead's basic information, provide enrichment insights.
Always respond with valid JSON only. No explanation, no markdown.
JSON must have exactly these keys:
- company_summary: 1 sentence about the company
- company_stage: one of [startup, growth, enterprise, public]
- likely_tech_stack: list of 3 technologies they likely use
- pain_points: list of 2 likely business pain points
- buying_readiness: one of [low, medium, high]
- best_contact_channel: one of [email, linkedin, phone, event]
- enrichment_confidence: integer 0-100"""

        user_prompt = f"""Lead information:
Name:           {lead.get('full_name', 'Unknown')}
Job Title:      {lead.get('job_title', 'Unknown')}
Seniority:      {lead.get('seniority', 'Unknown')}
Company:        {lead.get('company_name', 'Unknown')}
Industry:       {lead.get('industry', 'Unknown')}
Employee Count: {lead.get('employee_count', 'Unknown')}
Email Domain:   {lead.get('email_domain', 'Unknown')}

Provide enrichment insights as JSON."""

        response = self.call_llm(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.2,
            max_tokens=400
        )

        try:
            enrichment = json.loads(response)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse enrichment JSON")
            enrichment = {
                "company_summary":      "Unable to enrich",
                "company_stage":        "unknown",
                "likely_tech_stack":    [],
                "pain_points":          [],
                "buying_readiness":     "unknown",
                "best_contact_channel": "email",
                "enrichment_confidence": 0,
            }

        return {**lead, **enrichment}