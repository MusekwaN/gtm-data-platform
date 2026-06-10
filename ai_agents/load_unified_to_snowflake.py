# ai_agents/load_unified_to_snowflake.py

import os
import glob
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging

root_env = Path(__file__).resolve().parents[1] / "env"
load_dotenv(dotenv_path=root_env)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run():
    # Load unified parquet files
    files = glob.glob("./data/unified/*.parquet")
    if not files:
        logger.error("No unified data found. Run identity resolution first.")
        return

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    logger.info(f"Loaded {len(df)} unified records")

    # Clean columns for Snowflake
    df.columns = [c.upper() for c in df.columns]
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).replace("nan", None)

    # Connect to Snowflake
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "GTM_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "SILVER_SILVER")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )
    if warehouse:
        conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
        logger.info(f"Using warehouse {warehouse}")
    if database:
        conn.cursor().execute(f"USE DATABASE {database}")
        logger.info(f"Using database {database}")
    if schema:
        conn.cursor().execute(f"USE SCHEMA {schema}")
        logger.info(f"Using schema {schema}")

    # Write to Snowflake
    try:
        success, _, rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="UNIFIED_LEADS",
            schema=schema,
            database=database,
            overwrite=True,
            auto_create_table=True,
            use_logical_type=True,
        )

        if success:
            logger.info(f"Loaded {rows} unified records into {database}.{schema}.UNIFIED_LEADS")
    finally:
        conn.close()


if __name__ == "__main__":
    run()