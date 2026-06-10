# ai_agents/lead_scoring/outreach_agent.py
"""
Outreach Agent — crafts personalized outreach (subject + body)
"""

import json
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from ai_agents.lead_scoring.base_agent import BaseAgent


class OutreachAgent(BaseAgent):

    def __init__(self):
        super().__init__(name="OutreachAgent")

    def run(self, lead: dict) -> dict:
        """
        Produce an email subject and short email body tailored to the lead.
        Returns the lead augmented with `email_subject`, `email_body`, and `cadence`.
        """
        self.logger.info(f"Generating outreach for {lead.get('full_name')}")

        system_prompt = """You are an expert B2B sales copywriter.
Given lead/context produce a short personalized email subject and body.
Always respond with valid JSON only. No explanation. JSON keys:
- email_subject: short subject line
- email_body: 3-5 sentence email body, personalized
- cadence: list of follow-up steps (each a short string)
"""

        user_prompt = f"""Lead info:
Name: {lead.get('full_name', 'Unknown')}
Title: {lead.get('job_title', 'Unknown')}
Company: {lead.get('company_name', 'Unknown')}
Pain Points: {lead.get('pain_points', [])}
Best Contact Channel: {lead.get('best_contact_channel', 'email')}
Provide the JSON output described."""

        response = self.call_llm(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.3,
            max_tokens=400
        )

        try:
            payload = json.loads(response)
        except Exception:
            self.logger.warning("Failed to parse outreach JSON from LLM; using defaults")
            payload = {
                "email_subject": f"Quick question about {lead.get('company_name', 'your team')}",
                "email_body": (
                    f"Hi {lead.get('full_name', 'there')},\n\n"
                    "I saw you're working on initiatives in your area. We'd love to share a brief idea that has helped similar teams."
                    "\n\nBest,\nSales Team"
                ),
                "cadence": ["Email day 0", "Follow-up day 3", "Final attempt day 7"],
            }

        return {**lead, **payload}
