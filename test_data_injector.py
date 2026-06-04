# test_data_injector.py
# Injects fake lead data into Kafka to test the full pipeline

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_block_ms=10000,
    )

# Fake lead data
COMPANIES = [
    {"name": "TechCorp",     "domain": "techcorp.com",     "industry": "software",    "size": 500},
    {"name": "DataSoft",     "domain": "datasoft.io",      "industry": "saas",        "size": 200},
    {"name": "FinanceHub",   "domain": "financehub.com",   "industry": "fintech",     "size": 1500},
    {"name": "HealthPlus",   "domain": "healthplus.org",   "industry": "healthcare",  "size": 300},
    {"name": "RetailMax",    "domain": "retailmax.com",    "industry": "retail",      "size": 50},
]

PEOPLE = [
    {"first": "Alice",  "last": "Johnson",  "title": "VP of Engineering",  "seniority": "vp"},
    {"first": "Bob",    "last": "Smith",    "title": "CTO",                "seniority": "c_suite"},
    {"first": "Carol",  "last": "Williams", "title": "Director of Data",   "seniority": "director"},
    {"first": "David",  "last": "Brown",    "title": "Senior Engineer",    "seniority": "senior"},
    {"first": "Emma",   "last": "Davis",    "title": "Data Manager",       "seniority": "manager"},
    {"first": "Frank",  "last": "Miller",   "title": "Software Engineer",  "seniority": "individual"},
    {"first": "Grace",  "last": "Wilson",   "title": "CEO",                "seniority": "c_suite"},
    {"first": "Henry",  "last": "Moore",    "title": "Head of Analytics",  "seniority": "director"},
]

def generate_lead(i: int) -> dict:
    person  = random.choice(PEOPLE)
    company = random.choice(COMPANIES)
    return {
        "lead_id":        f"lead_{i:04d}",
        "source":         random.choice(["apollo", "hubspot"]),
        "first_name":     person["first"],
        "last_name":      person["last"],
        "email":          f"{person['first'].lower()}.{person['last'].lower()}{i}@{company['domain']}",
        "job_title":      person["title"],
        "seniority":      person["seniority"],
        "linkedin_url":   f"https://linkedin.com/in/{person['first'].lower()}{i}",
        "company_name":   company["name"],
        "company_domain": company["domain"],
        "industry":       company["industry"],
        "employee_count": company["size"],
        "apollo_score":   round(random.uniform(50, 99), 2),
        "lead_status":    random.choice(["new", "contacted", "qualified"]),
        "ingested_at":    datetime.now(timezone.utc).isoformat(),
        "event_type":     "lead_created",
    }

def inject(count: int = 50, topic: str = "apollo_leads"):
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"Connecting to Kafka broker at {bootstrap_servers}...")

    try:
        producer = create_producer()
    except NoBrokersAvailable as exc:
        print(f"Error: could not connect to Kafka broker: {exc}")
        return
    except Exception as exc:
        print(f"Error: failed to create Kafka producer: {exc}")
        return

    try:
        print(f"Injecting {count} test leads into Kafka topic '{topic}'...")
        for i in range(count):
            lead = generate_lead(i)
            producer.send(topic, value=lead, key=lead["lead_id"])
            print(f"  Sent {lead['lead_id']} | {lead['first_name']} {lead['last_name']} | {lead['company_name']}")
            time.sleep(0.1)

        producer.flush()
        print(f"\nDone — {count} records sent to topic: {topic}")
    finally:
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject fake lead data into Kafka.")
    parser.add_argument("-c", "--count", type=int, default=50, help="Number of test leads to inject")
    parser.add_argument("-t", "--topic", default="apollo_leads", help="Kafka topic to send test leads to")
    args = parser.parse_args()

    inject(count=args.count, topic=args.topic)