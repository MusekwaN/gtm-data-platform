# simple_pipeline.py
# Pure Python Bronze -> Silver -> Gold pipeline
# Replaces Spark for local Windows development

import json
import os
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime

# ── Config ────────────────────────────────────────────────────
KAFKA_SERVER   = "localhost:9092"
TOPIC          = "apollo_leads"
BRONZE_PATH    = "./data/bronze/leads"
SILVER_PATH    = "./data/silver/leads"
GOLD_PATH      = "./data/gold/lead_scores"

# Create directories if they don't exist
for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH]:
    os.makedirs(path, exist_ok=True)

print("=" * 55)
print("  GTM Pipeline: Bronze → Silver → Gold")
print("=" * 55)

# ─────────────────────────────────────────────────────────────
# STEP 1: BRONZE — Read raw messages from Kafka
# ─────────────────────────────────────────────────────────────
print("\n[BRONZE] Reading from Kafka topic: apollo_leads...")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze_pipeline",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=8000,   # stop after 8s of no messages
)

bronze_records = []
for message in consumer:
    record = message.value
    # Add bronze metadata
    record["bronze_loaded_at"] = datetime.utcnow().isoformat()
    record["kafka_offset"]     = message.offset
    record["kafka_partition"]  = message.partition
    record["layer"]            = "bronze"
    bronze_records.append(record)

consumer.close()

if not bronze_records:
    print("  No messages found in Kafka.")
    print("  Run: python test_data_injector.py first")
    exit(1)

print(f"  Read {len(bronze_records)} records from Kafka")

# Save Bronze as parquet
bronze_df = pd.DataFrame(bronze_records)
bronze_file = f"{BRONZE_PATH}/bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
bronze_df.to_parquet(bronze_file, index=False)
print(f"  Saved Bronze: {bronze_file}")

# ─────────────────────────────────────────────────────────────
# STEP 2: SILVER — Clean and standardize
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] Cleaning and standardizing...")

silver_records = []
seen_emails = set()

for rec in bronze_records:
    email = rec.get("email", "")
    if not email:
        continue

    email_clean = email.lower().strip()

    # Deduplicate by email
    if email_clean in seen_emails:
        continue
    seen_emails.add(email_clean)

    # Extract domain
    email_domain = email_clean.split("@")[1] if "@" in email_clean else ""

    silver_records.append({
        "lead_id":        rec.get("lead_id"),
        "source":         rec.get("source", "unknown"),
        "full_name":      f"{rec.get('first_name','').strip()} {rec.get('last_name','').strip()}".strip(),
        "email":          email_clean,
        "email_domain":   email_domain,
        "job_title":      (rec.get("job_title") or "").lower().strip(),
        "seniority":      (rec.get("seniority") or "unknown").lower().strip(),
        "company_name":   (rec.get("company_name") or "").strip(),
        "company_domain": (rec.get("company_domain") or "").lower().strip(),
        "industry":       (rec.get("industry") or "unknown").lower().strip(),
        "employee_count": rec.get("employee_count"),
        "lead_status":    rec.get("lead_status"),
        "ingested_at":    rec.get("ingested_at"),
        "processed_at":   datetime.utcnow().isoformat(),
        "layer":          "silver",
    })

print(f"  Cleaned {len(silver_records)} unique records (removed duplicates)")

silver_df = pd.DataFrame(silver_records)
silver_file = f"{SILVER_PATH}/silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
silver_df.to_parquet(silver_file, index=False)
print(f"  Saved Silver: {silver_file}")

# ─────────────────────────────────────────────────────────────
# STEP 3: GOLD — Score and classify leads
# ─────────────────────────────────────────────────────────────
print("\n[GOLD] Scoring leads...")

def seniority_score(s):
    return {"c_suite": 40, "vp": 40, "director": 30,
            "manager": 20, "senior": 10}.get(s, 5)

def company_size_score(n):
    try:
        n = int(n)
        if n >= 1000: return 30
        if n >= 200:  return 25
        if n >= 50:   return 15
        if n >= 10:   return 10
    except: pass
    return 5

def industry_score(i):
    hot = ["software","technology","saas","fintech","information technology"]
    mid = ["finance","banking","healthcare"]
    if i in hot: return 20
    if i in mid: return 15
    return 5

def completeness_score(rec):
    score = 0
    if rec.get("email"):          score += 3
    if rec.get("company_name"):   score += 2
    if rec.get("job_title"):      score += 2
    if rec.get("email_domain"):   score += 3
    return score

def intent_level(score):
    if score >= 80: return "hot"
    if score >= 60: return "warm"
    if score >= 40: return "cool"
    return "cold"

def recommended_action(level):
    return {
        "hot":  "schedule_demo",
        "warm": "send_case_study",
        "cool": "add_to_nurture",
        "cold": "enrich_data"
    }.get(level, "enrich_data")

gold_records = []
for rec in silver_records:
    s_score = seniority_score(rec["seniority"])
    c_score = company_size_score(rec["employee_count"])
    i_score = industry_score(rec["industry"])
    comp_score = completeness_score(rec)
    total = s_score + c_score + i_score + comp_score
    level = intent_level(total)

    gold_records.append({
        "lead_id":             rec["lead_id"],
        "source":              rec["source"],
        "full_name":           rec["full_name"],
        "email":               rec["email"],
        "company_name":        rec["company_name"],
        "industry":            rec["industry"],
        "seniority":           rec["seniority"],
        "employee_count":      rec["employee_count"],
        "lead_score":          total,
        "seniority_score":     s_score,
        "company_size_score":  c_score,
        "industry_score":      i_score,
        "completeness_score":  comp_score,
        "intent_level":        level,
        "recommended_action":  recommended_action(level),
        "scored_at":           datetime.utcnow().isoformat(),
        "layer":               "gold",
    })

gold_df = pd.DataFrame(gold_records)
gold_file = f"{GOLD_PATH}/gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
gold_df.to_parquet(gold_file, index=False)
print(f"  Scored {len(gold_records)} leads")
print(f"  Saved Gold: {gold_file}")

# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
print("\n" + "=" * 55)
print("  PIPELINE COMPLETE")
print("=" * 55)
print(f"  Bronze records : {len(bronze_records)}")
print(f"  Silver records : {len(silver_records)}")
print(f"  Gold records   : {len(gold_records)}")

print("\n  Lead Score Distribution:")
summary = gold_df.groupby("intent_level").agg(
    count=("lead_id", "count"),
    avg_score=("lead_score", "mean")
).reset_index()
print(summary.to_string(index=False))

print("\n  Top 5 Leads:")
top5 = gold_df.nlargest(5, "lead_score")[
    ["full_name", "company_name", "lead_score",
     "intent_level", "recommended_action"]
]
print(top5.to_string(index=False))