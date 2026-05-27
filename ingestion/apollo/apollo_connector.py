# ingestion/apollo/apollo_connector.py

import requests
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from base_connector import BaseConnector

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv()
load_dotenv(os.path.join(ROOT_DIR, 'env'))

class ApolloConnector(BaseConnector):
    """
    Pulls company and lead data from Apollo.io API
    and streams records into the apollo_leads Kafka topic.
    """

    BASE_URL = "https://api.apollo.io/v1"

    def __init__(self):
        super().__init__(topic="apollo_leads")
        self.api_key = os.getenv("APOLLO_API_KEY")
        if not self.api_key:
            raise ValueError("APOLLO_API_KEY is not set in environment")
        self.api_path = os.getenv("APOLLO_API_PATH", "mixed_people/search")
        self.api_url = os.getenv("APOLLO_API_URL") or f"{self.BASE_URL}/{self.api_path}"
        self.headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": self.api_key,
        }

    def fetch_data(self) -> list:
        """Fetch leads/people from Apollo search endpoint."""
        url = self.api_url
        self.logger.info(f"Using Apollo endpoint: {url}")

        payload = {
            "page": 1,
            "per_page": 25,
            "person_titles": ["CEO", "CTO", "VP of Engineering", "Head of Data"],
            "organization_num_employees_ranges": ["1,500"],
        }

        try:
            response = requests.post(url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            people = data.get("people", [])
            self.logger.info(f"Apollo returned {len(people)} records")
            return people

        except requests.exceptions.HTTPError as e:
            body = response.text if response is not None else 'no response body'
            self.logger.error(
                f"Apollo HTTP error: {e} | Status: {response.status_code} | Body: {body}"
            )
            return []
        except Exception as e:
            self.logger.error(f"Apollo fetch failed: {e}")
            return []

    def transform_record(self, person: dict) -> dict:
        """Flatten and standardize an Apollo person record."""
        org = person.get("organization") or {}
        return {
            # identifiers
            "lead_id":          person.get("id"),
            "source":           "apollo",
            # person fields
            "first_name":       person.get("first_name"),
            "last_name":        person.get("last_name"),
            "email":            person.get("email"),
            "job_title":        person.get("title"),
            "seniority":        person.get("seniority"),
            "linkedin_url":     person.get("linkedin_url"),
            # company fields
            "company_name":     org.get("name"),
            "company_domain":   org.get("primary_domain"),
            "industry":         org.get("industry"),
            "employee_count":   org.get("estimated_num_employees"),
            "company_linkedin": org.get("linkedin_url"),
            # scoring
            "apollo_score":     person.get("score"),
            # metadata
            "ingested_at":      datetime.now(timezone.utc).isoformat(),
            "raw_payload":      person,   # keep original for Bronze layer
        }

    def run(self, interval_seconds: int = 300):
        """
        Main loop — fetch from Apollo every interval_seconds.
        Default: every 5 minutes.
        """
        self.logger.info("Apollo connector started")

        while True:
            try:
                self.logger.info("Fetching from Apollo...")
                raw_records = self.fetch_data()

                if raw_records:
                    transformed = [self.transform_record(p) for p in raw_records]
                    self.send_batch(transformed, key_field="lead_id")
                    self.flush()
                else:
                    self.logger.warning("No records returned from Apollo")

                self.logger.info(f"Sleeping {interval_seconds}s until next fetch...")
                time.sleep(interval_seconds)

            except KeyboardInterrupt:
                self.logger.info("Apollo connector stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                time.sleep(30)  # wait before retrying


if __name__ == "__main__":
    connector = ApolloConnector()
    connector.run(interval_seconds=300)