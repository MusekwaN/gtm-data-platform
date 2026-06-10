# ai_agents/lead_scoring/load_insights_to_snowflake.py

import os
import glob
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging

root_env = os.path.join(os.path.dirname(__file__), '..', '..', 'env')
load_dotenv(dotenv_path=root_env)
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run():
    files = glob.glob("./data/ai_insights/*.parquet")
    if not files:
        logger.error("No AI insights found. Run ai_pipeline.py first.")
        return

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    logger.info(f"Loaded {len(df)} AI insight records")

    # Keep only key columns for Snowflake
    keep_cols = [
        "lead_id","full_name","email","company_name",
        "industry","seniority","lead_score",
        "intent_score","intent_level","urgency",
        "deal_size_estimate","next_best_action",
        "company_summary","company_stage",
        "buying_readiness","best_contact_channel",
        "email_subject","email_body",
        "linkedin_message","key_value_prop",
        "ai_reasoning","processed_at"
    ]

    df = df[[c for c in keep_cols if c in df.columns]]
    df.columns = [c.upper() for c in df.columns]

    database = os.getenv("SNOWFLAKE_DATABASE", "GTM_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "GOLD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )

    try:
        if warehouse:
            conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
            logger.info(f"Using warehouse {warehouse}")
        if database:
            conn.cursor().execute(f"USE DATABASE {database}")
            logger.info(f"Using database {database}")
        if schema:
            conn.cursor().execute(f"USE SCHEMA {schema}")
            logger.info(f"Using schema {schema}")

        success, _, rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="AI_LEAD_INSIGHTS",
            schema=schema,
            database=database,
            overwrite=True,
            auto_create_table=True,
            use_logical_type=True,
        )

        if success:
            logger.info(f"Loaded {rows} rows into {database}.{schema}.AI_LEAD_INSIGHTS")
    finally:
        conn.close()


if __name__ == "__main__":
    run()