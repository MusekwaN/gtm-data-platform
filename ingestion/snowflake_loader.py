# ingestion/snowflake_loader.py

import glob
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

root_env = Path(__file__).resolve().parents[1] / "env"
load_dotenv(dotenv_path=root_env)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def parse_snowflake_account() -> str | None:
    account = os.getenv("SNOWFLAKE_ACCOUNT") or os.getenv("SNOWFLAKE_ACCOUNT_LOCATOR")
    account_url = os.getenv("SNOWFLAKE_ACCOUNT_URL")

    if account_url:
        account_url = account_url.strip()
        if account_url.endswith(".snowflakecomputing.com"):
            return account_url.split(".snowflakecomputing.com")[0]
        return account_url

    if account and account.endswith(".snowflakecomputing.com"):
        return account.split(".snowflakecomputing.com")[0]

    return account


def validate_env_vars():
    required = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if not parse_snowflake_account():
        missing.append("SNOWFLAKE_ACCOUNT or SNOWFLAKE_ACCOUNT_URL or SNOWFLAKE_ACCOUNT_LOCATOR")
    if missing:
        raise EnvironmentError(
            "Missing required Snowflake environment variables: " + ", ".join(missing)
        )


def get_connection():
    validate_env_vars()
    account = parse_snowflake_account()

    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    conn = snowflake.connector.connect(
        account=account,
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "GTM_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        warehouse=warehouse,
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )

    if warehouse:
        try:
            conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
            logger.info(f"Using warehouse {warehouse}")
        except Exception as exc:
            logger.error(f"Failed to activate warehouse {warehouse}: {exc}")
            conn.close()
            raise

    logger.info("Snowflake connected successfully")
    return conn


def read_all_parquet(folder_path: str) -> pd.DataFrame:
    """Read all parquet files in a folder into one DataFrame."""
    folder = Path(folder_path)
    files = list(folder.glob("*.parquet"))
    if not files:
        logger.warning(f"No parquet files found in {folder_path}")
        return pd.DataFrame()

    frames = [pd.read_parquet(file) for file in files]
    df = pd.concat(frames, ignore_index=True)
    logger.info(f"Read {len(df)} rows from {len(files)} files in {folder_path}")
    return df


def clean_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for Snowflake loading."""

    # Drop internal pipeline columns Snowflake tables don't have
    drop_cols = [
        "layer", "kafka_key", "kafka_offset", "kafka_partition",
        "topic", "partition", "offset", "bronze_loaded_at",
        "raw_payload"
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # Uppercase all column names (Snowflake standard)
    df.columns = [c.upper() for c in df.columns]

    # Add loaded timestamp
    df["LOADED_AT"] = datetime.now(timezone.utc)

    # Convert object columns to string and normalize missing values
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).replace({"nan": None, "None": None})

    return df


def load_to_snowflake(df: pd.DataFrame, table_name: str, conn):
    """Write a DataFrame into a Snowflake RAW table."""
    if df.empty:
        logger.warning(f"Skipping {table_name} — no data")
        return

    try:
        success, chunks, rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            schema="RAW",
            database=os.getenv("SNOWFLAKE_DATABASE", "GTM_DB"),
            overwrite=True,          # replace existing data
            auto_create_table=True,  # create table if not exists
            use_logical_type=True,   # preserve timezone-aware datetimes correctly
        )
        if success:
            logger.info(f"Loaded {rows} rows into RAW.{table_name}")
        else:
            logger.error(f"Load failed for {table_name}")
    except Exception as e:
        logger.error(f"Error loading {table_name}: {e}")
        raise


def run(
    bronze_path: str = "./data/bronze/leads",
    gold_path: str = "./data/gold/lead_scores",
):
    conn = get_connection()

    try:
        # Load Bronze leads
        bronze_df = read_all_parquet(bronze_path)
        if not bronze_df.empty:
            load_to_snowflake(
                clean_for_snowflake(bronze_df),
                "APOLLO_LEADS",
                conn
            )

        # Load Gold lead scores
        gold_df = read_all_parquet(gold_path)
        if not gold_df.empty:
            load_to_snowflake(
                clean_for_snowflake(gold_df),
                "LEAD_SCORES",
                conn
            )

        logger.info("All data loaded into Snowflake successfully")

    finally:
        conn.close()
        logger.info("Snowflake connection closed")


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Load local parquet data into Snowflake RAW tables."
    )
    parser.add_argument(
        "--bronze-path",
        default="./data/bronze/leads",
        help="Path to Bronze Parquet files",
    )
    parser.add_argument(
        "--gold-path",
        default="./data/gold/lead_scores",
        help="Path to Gold Parquet files",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(bronze_path=args.bronze_path, gold_path=args.gold_path)