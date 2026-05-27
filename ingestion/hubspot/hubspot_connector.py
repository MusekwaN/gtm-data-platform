# ingestion/hubspot/hubspot_connector.py

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

class HubSpotConnector(BaseConnector):
    """
    Pulls contacts and companies from HubSpot CRM
    and streams into the crm_events Kafka topic.
    """

    BASE_URL = "https://api.hubapi.com"

    def __init__(self):
        super().__init__(topic="crm_events")
        self.token = os.getenv("HUBSPOT_ACCESS_TOKEN")
        if not self.token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN is not set in environment")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def fetch_data(self) -> list:
        """Fetch contacts from HubSpot."""
        url = f"{self.BASE_URL}/crm/v3/objects/contacts"
        params = {
            "limit": 100,
            "properties": "firstname,lastname,email,company,jobtitle,phone,hs_lead_status,createdate"
        }

        all_contacts = []
        while url:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                all_contacts.extend(data.get("results", []))

                # handle pagination
                paging = data.get("paging", {})
                url = paging.get("next", {}).get("link")
                params = {}  # params only needed on first request
                self.logger.info(f"Fetched {len(all_contacts)} contacts so far...")

            except Exception as e:
                self.logger.error(f"HubSpot fetch error: {e}")
                break

        return all_contacts

    def transform_record(self, contact: dict) -> dict:
        """Flatten HubSpot contact into standard format."""
        props = contact.get("properties", {})
        return {
            "lead_id":       contact.get("id"),
            "source":        "hubspot",
            "first_name":    props.get("firstname"),
            "last_name":     props.get("lastname"),
            "email":         props.get("email"),
            "company_name":  props.get("company"),
            "job_title":     props.get("jobtitle"),
            "phone":         props.get("phone"),
            "lead_status":   props.get("hs_lead_status"),
            "created_at":    props.get("createdate"),
            "ingested_at":   datetime.now(timezone.utc).isoformat(),
            "event_type":    "contact_sync",
            "raw_payload":   contact,
        }

    def run(self, interval_seconds: int = 300):
        """Main loop — sync HubSpot contacts every 5 minutes."""
        self.logger.info("HubSpot connector started")

        while True:
            try:
                self.logger.info("Fetching from HubSpot...")
                raw_records = self.fetch_data()

                if raw_records:
                    transformed = [self.transform_record(c) for c in raw_records]
                    self.send_batch(transformed, key_field="lead_id")
                    self.flush()

                self.logger.info(f"Sleeping {interval_seconds}s...")
                time.sleep(interval_seconds)

            except KeyboardInterrupt:
                self.logger.info("HubSpot connector stopped")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                time.sleep(30)


if __name__ == "__main__":
    connector = HubSpotConnector()
    connector.run(interval_seconds=300)