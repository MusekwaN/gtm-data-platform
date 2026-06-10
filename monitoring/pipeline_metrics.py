# monitoring/pipeline_metrics.py
"""
Prometheus metrics exporter for the GTM pipeline.
Exposes pipeline health metrics on port 8000.
"""

import os
import glob
import time
import pandas as pd
from datetime import datetime
from prometheus_client import (
    start_http_server,
    Gauge, Counter, Histogram, Info
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("Metrics")

# ─────────────────────────────────────────────────────────────
# METRIC DEFINITIONS
# ─────────────────────────────────────────────────────────────

# Record counts per layer
bronze_records   = Gauge("gtm_bronze_record_count",  "Records in Bronze layer")
silver_records   = Gauge("gtm_silver_record_count",  "Records in Silver layer")
gold_records     = Gauge("gtm_gold_record_count",    "Records in Gold layer")
unified_records  = Gauge("gtm_unified_record_count", "Unified identity records")
ai_insight_count = Gauge("gtm_ai_insight_count",     "AI enriched lead records")

# Lead scoring distribution
hot_leads  = Gauge("gtm_hot_leads_count",  "Hot leads count")
warm_leads = Gauge("gtm_warm_leads_count", "Warm leads count")
cool_leads = Gauge("gtm_cool_leads_count", "Cool leads count")
cold_leads = Gauge("gtm_cold_leads_count", "Cold leads count")

# Pipeline health
avg_lead_score    = Gauge("gtm_avg_lead_score",      "Average lead score")
pipeline_runs     = Counter("gtm_pipeline_runs_total","Total pipeline runs")
quality_pass_rate = Gauge("gtm_quality_pass_rate",   "Data quality pass rate %")

# Pipeline info
pipeline_info = Info("gtm_pipeline", "GTM pipeline metadata")


def count_parquet_rows(path: str) -> int:
    """Count total rows across all parquet files in a folder."""
    files = glob.glob(f"{path}/*.parquet")
    if not files:
        return 0
    try:
        total = sum(len(pd.read_parquet(f)) for f in files)
        return total
    except Exception:
        return 0


def collect_metrics():
    """Collect and update all metrics."""
    logger.info("Collecting pipeline metrics...")

    # Layer record counts
    bronze_count  = count_parquet_rows("./data/bronze/leads")
    silver_count  = count_parquet_rows("./data/silver/leads")
    gold_count    = count_parquet_rows("./data/gold/lead_scores")
    unified_count = count_parquet_rows("./data/unified")
    ai_count      = count_parquet_rows("./data/ai_insights")

    bronze_records.set(bronze_count)
    silver_records.set(silver_count)
    gold_records.set(gold_count)
    unified_records.set(unified_count)
    ai_insight_count.set(ai_count)

    # Lead scoring breakdown
    gold_files = glob.glob("./data/gold/lead_scores/*.parquet")
    if gold_files:
        try:
            df = pd.concat(
                [pd.read_parquet(f) for f in gold_files],
                ignore_index=True
            )
            intent_counts = df["intent_level"].value_counts()
            hot_leads.set(int(intent_counts.get("hot", 0)))
            warm_leads.set(int(intent_counts.get("warm", 0)))
            cool_leads.set(int(intent_counts.get("cool", 0)))
            cold_leads.set(int(intent_counts.get("cold", 0)))

            if "lead_score" in df.columns:
                avg_lead_score.set(round(float(df["lead_score"].mean()), 2))

        except Exception as e:
            logger.warning(f"Could not read Gold metrics: {e}")

    # Pipeline info
    pipeline_info.info({
        "version":      "1.0",
        "environment":  "development",
        "last_updated": datetime.now().isoformat(),
        "bronze_rows":  str(bronze_count),
        "silver_rows":  str(silver_count),
        "gold_rows":    str(gold_count),
    })

    pipeline_runs.inc()

    logger.info(
        f"Metrics updated — "
        f"Bronze: {bronze_count} | "
        f"Silver: {silver_count} | "
        f"Gold: {gold_count} | "
        f"AI: {ai_count}"
    )


def run_metrics_server(port: int = 8000, interval: int = 30):
    """
    Start Prometheus metrics server.
    Collects metrics every interval seconds.
    """
    logger.info(f"Starting metrics server on port {port}...")
    start_http_server(port)
    logger.info(f"Metrics available at http://localhost:{port}/metrics")

    while True:
        try:
            collect_metrics()
        except Exception as e:
            logger.error(f"Metrics collection error: {e}")
        time.sleep(interval)


if __name__ == "__main__":
    run_metrics_server(port=8000, interval=30)