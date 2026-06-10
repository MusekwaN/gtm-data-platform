# create_mock_insights.py
# Creates mock AI insights so Reverse ETL can run without OpenAI

import os
import json
import pandas as pd
from datetime import datetime, timezone

os.makedirs("./data/ai_insights", exist_ok=True)

mock_leads = [
    {
        "lead_id":            "lead_0002",
        "full_name":          "Bob Smith",
        "email":              "bob.smith2@financehub.com",
        "company_name":       "FinanceHub",
        "job_title":          "CTO",
        "seniority":          "c_suite",
        "industry":           "fintech",
        "employee_count":     1500,
        "lead_score":         100,
        "intent_score":       92,
        "intent_level":       "hot",
        "urgency":            "high",
        "deal_size_estimate": "enterprise",
        "next_best_action":   "Schedule executive briefing this week",
        "company_summary":    "FinanceHub is a fast-growing fintech platform",
        "company_stage":      "growth",
        "buying_readiness":   "high",
        "best_contact_channel": "email",
        "email_subject":      "Quick question about FinanceHub's data stack",
        "email_body":         "Hi Bob, I noticed FinanceHub has been scaling rapidly. Many fintech CTOs we work with struggle with data pipeline reliability at your stage. Would a 15-minute call make sense this week?",
        "linkedin_message":   "Hi Bob, saw FinanceHub's recent growth — impressive. I work with fintech CTOs on data infrastructure challenges. Would love to connect.",
        "key_value_prop":     "Real-time data pipelines built for fintech scale",
        "ai_reasoning":       "C-suite at high-growth fintech with 1500 employees signals enterprise deal potential and high buying readiness.",
        "processed_at":       datetime.now(timezone.utc).isoformat(),
    },
    {
        "lead_id":            "lead_0000",
        "full_name":          "Grace Wilson",
        "email":              "grace.wilson0@techcorp.com",
        "company_name":       "TechCorp",
        "job_title":          "CEO",
        "seniority":          "c_suite",
        "industry":           "software",
        "employee_count":     500,
        "lead_score":         95,
        "intent_score":       88,
        "intent_level":       "hot",
        "urgency":            "high",
        "deal_size_estimate": "enterprise",
        "next_best_action":   "Send personalized case study then follow up",
        "company_summary":    "TechCorp is a software company focused on enterprise solutions",
        "company_stage":      "enterprise",
        "buying_readiness":   "high",
        "best_contact_channel": "linkedin",
        "email_subject":      "How TechCorp can cut pipeline failures by 80%",
        "email_body":         "Hi Grace, CEOs at software companies your size often tell us that data reliability becomes a bottleneck at 500+ employees. We helped a similar company reduce pipeline failures by 80%. Worth a quick chat?",
        "linkedin_message":   "Hi Grace, TechCorp's growth trajectory is impressive. I help software CEOs solve data reliability challenges at scale. Would love to connect.",
        "key_value_prop":     "Enterprise-grade data reliability for software companies",
        "ai_reasoning":       "CEO of 500-person software company with high lead score indicates strong buying potential.",
        "processed_at":       datetime.now(timezone.utc).isoformat(),
    },
    {
        "lead_id":            "lead_0020",
        "full_name":          "Carol Williams",
        "email":              "carol.williams20@datasoft.io",
        "company_name":       "DataSoft",
        "job_title":          "Director of Data",
        "seniority":          "director",
        "industry":           "saas",
        "employee_count":     200,
        "lead_score":         75,
        "intent_score":       72,
        "intent_level":       "warm",
        "urgency":            "medium",
        "deal_size_estimate": "mid_market",
        "next_best_action":   "Send relevant case study and schedule demo",
        "company_summary":    "DataSoft is a SaaS company specializing in data solutions",
        "company_stage":      "growth",
        "buying_readiness":   "medium",
        "best_contact_channel": "email",
        "email_subject":      "DataSoft + modern data stack — quick question",
        "email_body":         "Hi Carol, directors of data at growing SaaS companies often face the challenge of scaling pipelines without scaling headcount. I'd love to show you how we solve that. Do you have 20 minutes this week?",
        "linkedin_message":   "Hi Carol, your work at DataSoft looks interesting. I help data directors at SaaS companies build scalable pipelines. Would love to connect.",
        "key_value_prop":     "Scale your data pipelines without growing your team",
        "ai_reasoning":       "Director-level at mid-size SaaS company shows warm intent with budget authority likely present.",
        "processed_at":       datetime.now(timezone.utc).isoformat(),
    },
]

# Save as parquet
df = pd.DataFrame(mock_leads)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
parquet_path = f"./data/ai_insights/ai_insights_{timestamp}.parquet"
df.to_parquet(parquet_path, index=False)
print(f"Created mock insights: {parquet_path}")
print(f"Records: {len(df)}")
print(f"Intent levels: {df['intent_level'].value_counts().to_dict()}")