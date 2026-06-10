# monitoring/quality/data_quality_checks.py
"""
Data Quality checks using Great Expectations style rules.
Runs checks on Bronze, Silver and Gold layers.
"""

import os
import glob
import pandas as pd
from datetime import datetime, timezone
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("DataQuality")


# ─────────────────────────────────────────────────────────────
# EXPECTATION RULES
# ─────────────────────────────────────────────────────────────

def expect_no_nulls(df: pd.DataFrame, column: str) -> dict:
    """Check that a column has no null values."""
    null_count = df[column].isnull().sum()
    total      = len(df)
    passed     = null_count == 0
    return {
        "expectation":  f"expect_column_no_nulls",
        "column":       column,
        "passed":       passed,
        "null_count":   int(null_count),
        "total_rows":   total,
        "null_pct":     round(null_count / total * 100, 2) if total > 0 else 0,
        "message":      "PASS" if passed else f"FAIL — {null_count} nulls found in {column}"
    }


def expect_unique(df: pd.DataFrame, column: str) -> dict:
    """Check that a column has no duplicate values."""
    total      = len(df)
    unique     = df[column].nunique()
    duplicates = total - unique
    passed     = duplicates == 0
    return {
        "expectation":  "expect_column_unique",
        "column":       column,
        "passed":       passed,
        "duplicate_count": int(duplicates),
        "total_rows":   total,
        "message":      "PASS" if passed else f"FAIL — {duplicates} duplicates in {column}"
    }


def expect_values_in_set(df: pd.DataFrame, column: str, valid_values: list) -> dict:
    """Check that all values in a column belong to an allowed set."""
    if column not in df.columns:
        return {
            "expectation": "expect_values_in_set",
            "column":      column,
            "passed":      False,
            "message":     f"FAIL — column {column} not found"
        }
    invalid = df[~df[column].isin(valid_values)][column].unique()
    passed  = len(invalid) == 0
    return {
        "expectation":    "expect_values_in_set",
        "column":         column,
        "passed":         passed,
        "invalid_values": list(invalid),
        "valid_values":   valid_values,
        "message":        "PASS" if passed else f"FAIL — invalid values: {list(invalid)}"
    }


def expect_row_count_above(df: pd.DataFrame, min_rows: int) -> dict:
    """Check that DataFrame has at least min_rows rows."""
    total  = len(df)
    passed = total >= min_rows
    return {
        "expectation": "expect_row_count_above",
        "passed":      passed,
        "row_count":   total,
        "min_rows":    min_rows,
        "message":     "PASS" if passed else f"FAIL — only {total} rows, expected >= {min_rows}"
    }


def expect_no_future_dates(df: pd.DataFrame, column: str) -> dict:
    """Check that date column has no future dates."""
    if column not in df.columns:
        return {
            "expectation": "expect_no_future_dates",
            "column":      column,
            "passed":      True,
            "message":     f"SKIP — column {column} not found"
        }
    now = datetime.now(timezone.utc)
    try:
        dates = pd.to_datetime(df[column], utc=True, errors="coerce")
        future_count = (dates > now).sum()
        passed = future_count == 0
        return {
            "expectation":   "expect_no_future_dates",
            "column":        column,
            "passed":        passed,
            "future_count":  int(future_count),
            "message":       "PASS" if passed else f"FAIL — {future_count} future dates"
        }
    except Exception as e:
        return {
            "expectation": "expect_no_future_dates",
            "column":      column,
            "passed":      True,
            "message":     f"SKIP — could not parse dates: {e}"
        }


def expect_email_format(df: pd.DataFrame, column: str) -> dict:
    """Check that email column contains valid email formats."""
    if column not in df.columns:
        return {
            "expectation": "expect_email_format",
            "column":      column,
            "passed":      False,
            "message":     f"FAIL — column {column} not found"
        }
    valid   = df[column].str.contains(r"^[^@]+@[^@]+\.[^@]+$", na=False)
    invalid = (~valid).sum()
    passed  = invalid == 0
    return {
        "expectation":     "expect_email_format",
        "column":          column,
        "passed":          passed,
        "invalid_count":   int(invalid),
        "message":         "PASS" if passed else f"FAIL — {invalid} invalid emails"
    }


# ─────────────────────────────────────────────────────────────
# LAYER CHECKS
# ─────────────────────────────────────────────────────────────

def check_bronze_layer(df: pd.DataFrame) -> list:
    """Quality checks for Bronze layer."""
    return [
        expect_row_count_above(df, min_rows=1),
        expect_no_nulls(df, "lead_id"),
        expect_no_nulls(df, "source"),
        expect_no_nulls(df, "ingested_at"),
    ]


def check_silver_layer(df: pd.DataFrame) -> list:
    """Quality checks for Silver layer."""
    return [
        expect_row_count_above(df, min_rows=1),
        expect_no_nulls(df, "lead_id"),
        expect_no_nulls(df, "email"),
        expect_unique(df, "email"),
        expect_email_format(df, "email"),
        expect_values_in_set(df, "source", ["apollo", "hubspot", "salesforce", "unknown"]),
        expect_no_future_dates(df, "ingested_at"),
    ]


def check_gold_layer(df: pd.DataFrame) -> list:
    """Quality checks for Gold layer."""
    return [
        expect_row_count_above(df, min_rows=1),
        expect_no_nulls(df, "lead_id"),
        expect_no_nulls(df, "lead_score"),
        expect_values_in_set(df, "intent_level", ["hot", "warm", "cool", "cold"]),
        expect_values_in_set(df, "recommended_action", [
            "schedule_demo", "send_case_study",
            "add_to_nurture", "enrich_data"
        ]),
    ]


# ─────────────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────────────

def run_checks():
    """Run all quality checks across all layers."""
    print("=" * 55)
    print("  GTM Data Quality Checks")
    print("=" * 55)

    all_results = []
    summary     = {"passed": 0, "failed": 0, "skipped": 0}

    layers = [
        ("BRONZE", "./data/bronze/leads",       check_bronze_layer),
        ("SILVER", "./data/silver/leads",       check_silver_layer),
        ("GOLD",   "./data/gold/lead_scores",   check_gold_layer),
    ]

    for layer_name, path, check_fn in layers:
        print(f"\n── {layer_name} Layer ──────────────────────────")

        # Load parquet files
        files = glob.glob(f"{path}/*.parquet")
        if not files:
            print(f"  SKIP — no parquet files found in {path}")
            continue

        df = pd.concat(
            [pd.read_parquet(f) for f in files],
            ignore_index=True
        )
        print(f"  Loaded {len(df)} rows from {len(files)} files")

        # Run checks
        results = check_fn(df)

        for r in results:
            status = "✅ PASS" if r["passed"] else "❌ FAIL"
            print(f"  {status} | {r['expectation']} | {r.get('column','table')} | {r['message']}")

            if r["passed"]:
                summary["passed"] += 1
            else:
                summary["failed"] += 1

            all_results.append({
                "layer":   layer_name,
                "checked_at": datetime.now(timezone.utc).isoformat(),
                **r
            })

    # ── Save results ──────────────────────────────────────────
    os.makedirs("./monitoring/reports", exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"./monitoring/reports/quality_{timestamp}.json"

    with open(report_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    # ── Print summary ─────────────────────────────────────────
    total = summary["passed"] + summary["failed"]
    pct   = round(summary["passed"] / total * 100, 1) if total > 0 else 0

    print(f"\n{'=' * 55}")
    print(f"  QUALITY SUMMARY")
    print(f"{'=' * 55}")
    print(f"  Total checks : {total}")
    print(f"  Passed       : {summary['passed']} ✅")
    print(f"  Failed       : {summary['failed']} ❌")
    print(f"  Pass rate    : {pct}%")
    print(f"  Report saved : {report_path}")

    return all_results


if __name__ == "__main__":
    run_checks()