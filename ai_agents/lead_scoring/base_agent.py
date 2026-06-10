# ai_agents/lead_scoring/base_agent.py
"""
Base AI agent — all agents inherit from this.
Handles OpenAI connection, retries, and response parsing.
"""

import os
import time
import logging
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


class BaseAgent:

    def __init__(self, name: str, model: str = "gpt-3.5-turbo"):
        self.name   = name
        self.model  = model
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            self.client = OpenAI(api_key=api_key)
        else:
            self.client = None
            logging.getLogger(name).warning(
                "OPENAI_API_KEY not set; agents will use local fallbacks"
            )
        self.logger = logging.getLogger(name)

    def call_llm(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 500,
        retries: int = 3
    ) -> str:
        """
        Call OpenAI with retry logic.
        Returns the response text.
        """
        for attempt in range(retries):
            try:
                if not self.client:
                    # No OpenAI client available; return empty to trigger agent fallbacks
                    self.logger.info("No LLM client available; returning empty response")
                    return ""

                response = self.client.chat.completions.create(
                    model=self.model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ]
                )
                return response.choices[0].message.content.strip()

            except Exception as e:
                self.logger.warning(
                    f"Attempt {attempt + 1} failed: {e}"
                )
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)   # exponential backoff
                else:
                    self.logger.error("All retries exhausted")
                    return ""

    def run(self, lead: dict) -> dict:
        raise NotImplementedError("Each agent must implement run()")