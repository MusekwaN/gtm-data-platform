# reverse_etl/reverse_etl_engine.py
"""
Reverse ETL Engine
Reads AI insights from Snowflake and pushes
them into operational tools (Slack, CRM, Apollo).
"""

import os
import json
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime, timezone
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("ReverseETL")


# ─────────────────────────────────────────────────────────────
# SNOWFLAKE READER
# ─────────────────────────────────────────────────────────────

def fetch_hot_leads_from_snowflake() -> pd.DataFrame:
    """
    Pull hot leads from Snowflake that need action.
    """
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "GTM_DB"),
        schema="GOLD",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "GTM_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )

    query = """
        SELECT
            LEAD_ID,
            FULL_NAME,
            EMAIL,
            COMPANY_NAME,
            JOB_TITLE,
            INTENT_LEVEL,
            INTENT_SCORE,
            URGENCY,
            DEAL_SIZE_ESTIMATE,
            NEXT_BEST_ACTION,
            EMAIL_SUBJECT,
            EMAIL_BODY,
            LINKEDIN_MESSAGE,
            KEY_VALUE_PROP,
            AI_REASONING,
            PROCESSED_AT
        FROM GOLD.AI_LEAD_INSIGHTS
        WHERE INTENT_LEVEL IN ('hot', 'warm')
        ORDER BY INTENT_SCORE DESC
    """

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        logger.info(f"Fetched {len(df)} hot/warm leads from Snowflake")
        return df
    except Exception as e:
        logger.error(f"Snowflake fetch failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────
# SLACK NOTIFICATIONS
# ─────────────────────────────────────────────────────────────

def send_slack_alert(lead: dict) -> bool:
    """
    Send a formatted Slack alert for a hot lead.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url or webhook_url == "your_slack_webhook_url":
        logger.warning("Slack webhook not configured — skipping")
        return False

    # Intent level emoji
    emoji = {
        "hot":  "🔥",
        "warm": "☀️",
        "cool": "❄️",
        "cold": "🧊"
    }.get(lead.get("intent_level", "cold"), "📋")

    urgency_emoji = {
        "critical": "🚨",
        "high":     "⚡",
        "medium":   "📌",
        "low":      "📝"
    }.get(lead.get("urgency", "medium"), "📌")

    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Hot Lead Alert — {lead.get('full_name', 'Unknown')}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Company:*\n{lead.get('company_name', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Title:*\n{lead.get('job_title', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Intent Score:*\n{lead.get('intent_score', 0)}/100"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Urgency:*\n{urgency_emoji} {lead.get('urgency', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Deal Size:*\n{lead.get('deal_size_estimate', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Email:*\n{lead.get('email', 'N/A')}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🎯 Next Best Action:*\n{lead.get('next_best_action', 'N/A')}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📧 Suggested Email Subject:*\n_{lead.get('email_subject', 'N/A')}_"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🤖 AI Reasoning:*\n{lead.get('ai_reasoning', 'N/A')}"
                }
            },
            {
                "type": "divider"
            }
        ]
    }

    try:
        response = requests.post(
            webhook_url,
            json=message,
            headers={"Content-Type": "application/json"}
        )
        if response.status_code == 200:
            logger.info(f"Slack alert sent for {lead.get('full_name')}")
            return True
        else:
            logger.error(f"Slack error: {response.status_code} — {response.text}")
            return False
    except Exception as e:
        logger.error(f"Slack send failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# n8n WEBHOOK TRIGGER
# ─────────────────────────────────────────────────────────────

def trigger_n8n_workflow(lead: dict, workflow_type: str = "hot_lead") -> bool:
    """
    Send lead data to n8n webhook to trigger automated workflows.
    n8n then handles: CRM update, email sequence, enrichment.
    """
    n8n_url = os.getenv("N8N_WEBHOOK_URL", "http://localhost:5678/webhook/gtm-leads")

    payload = {
        "workflow_type": workflow_type,
        "triggered_at":  datetime.now(timezone.utc).isoformat(),
        "lead": {
            "lead_id":           lead.get("lead_id"),
            "full_name":         lead.get("full_name"),
            "email":             lead.get("email"),
            "company_name":      lead.get("company_name"),
            "job_title":         lead.get("job_title"),
            "intent_level":      lead.get("intent_level"),
            "intent_score":      lead.get("intent_score"),
            "urgency":           lead.get("urgency"),
            "next_best_action":  lead.get("next_best_action"),
            "email_subject":     lead.get("email_subject"),
            "email_body":        lead.get("email_body"),
            "linkedin_message":  lead.get("linkedin_message"),
            "key_value_prop":    lead.get("key_value_prop"),
        }
    }

    try:
        response = requests.post(
            n8n_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        if response.status_code in (200, 201):
            logger.info(f"n8n workflow triggered for {lead.get('full_name')}")
            return True
        else:
            logger.warning(f"n8n returned {response.status_code} — workflow may not be set up yet")
            return False
    except Exception as e:
        logger.warning(f"n8n not reachable: {e} — skipping workflow trigger")
        return False


# ─────────────────────────────────────────────────────────────
# MAIN REVERSE ETL RUNNER
# ─────────────────────────────────────────────────────────────

def run_reverse_etl(use_local_data: bool = False):
    """
    Main function — fetch insights and push to all destinations.
    Set use_local_data=True to use local parquet instead of Snowflake.
    """
    print("=" * 55)
    print("  GTM Reverse ETL Engine")
    print("=" * 55)

    # ── Load data ─────────────────────────────────────────────
    if use_local_data:
        import glob
        files = glob.glob("./data/ai_insights/*.parquet")
        if not files:
            print("No local AI insights found.")
            return
        df = pd.concat([pd.read_parquet(f) for f in files])
        df.columns = [c.lower() for c in df.columns]
        df = df[df["intent_level"].isin(["hot", "warm"])]
        logger.info(f"Loaded {len(df)} leads from local parquet")
    else:
        df = fetch_hot_leads_from_snowflake()

    if df.empty:
        print("No hot/warm leads found to process.")
        return

    leads = df.to_dict("records")
    print(f"\nProcessing {len(leads)} leads...\n")

    # ── Track results ─────────────────────────────────────────
    results = {
        "total":    len(leads),
        "slack":    0,
        "n8n":      0,
        "failed":   0,
    }

    for i, lead in enumerate(leads, 1):
        name  = lead.get("full_name", "Unknown")
        score = lead.get("intent_score", 0)
        level = lead.get("intent_level", "unknown")

        print(f"[{i}/{len(leads)}] {name} | {level} | score: {score}")

        # Send Slack alert
        if send_slack_alert(lead):
            results["slack"] += 1

        # Trigger n8n workflow
        if trigger_n8n_workflow(lead):
            results["n8n"] += 1

        import time
        time.sleep(0.5)

    # ── Print summary ─────────────────────────────────────────
    print("\n" + "=" * 55)
    print("  REVERSE ETL COMPLETE")
    print("=" * 55)
    print(f"  Total leads processed : {results['total']}")
    print(f"  Slack alerts sent     : {results['slack']}")
    print(f"  n8n workflows triggered: {results['n8n']}")


if __name__ == "__main__":
    # Use local data if Snowflake not loaded yet
    run_reverse_etl(use_local_data=True)