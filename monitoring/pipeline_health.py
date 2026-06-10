# monitoring/pipeline_health.py
"""
Pipeline health check — prints a full status report
of the entire GTM platform in one command.
"""

import os
import glob
import json
import pandas as pd
from datetime import datetime, timezone

def check_layer(name: str, path: str) -> dict:
    """Check health of a single data layer."""
    files = glob.glob(f"{path}/*.parquet")
    if not files:
        return {
            "layer":   name,
            "status":  "EMPTY",
            "files":   0,
            "rows":    0,
            "size_kb": 0,
        }
    rows     = sum(len(pd.read_parquet(f)) for f in files)
    size_kb  = sum(os.path.getsize(f) for f in files) // 1024
    return {
        "layer":   name,
        "status":  "OK" if rows > 0 else "EMPTY",
        "files":   len(files),
        "rows":    rows,
        "size_kb": size_kb,
    }


def run_health_check():
    print("\n" + "=" * 60)
    print("  GTM PLATFORM — PIPELINE HEALTH REPORT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # ── Data layers ───────────────────────────────────────────
    print("\n📦 DATA LAYERS")
    print(f"  {'Layer':<20} {'Status':<10} {'Files':<8} {'Rows':<10} {'Size'}")
    print(f"  {'-'*55}")

    layers = [
        ("Bronze/Leads",     "./data/bronze/leads"),
        ("Silver/Leads",     "./data/silver/leads"),
        ("Gold/LeadScores",  "./data/gold/lead_scores"),
        ("Unified/Leads",    "./data/unified"),
        ("AI/Insights",      "./data/ai_insights"),
    ]

    all_ok = True
    for name, path in layers:
        h = check_layer(name, path)
        status_icon = "✅" if h["status"] == "OK" else "❌"
        print(
            f"  {name:<20} {status_icon} {h['status']:<8} "
            f"{h['files']:<8} {h['rows']:<10} {h['size_kb']} KB"
        )
        if h["status"] != "OK":
            all_ok = False

    # ── Lead scoring summary ──────────────────────────────────
    print("\n🎯 LEAD SCORING SUMMARY")
    gold_files = glob.glob("./data/gold/lead_scores/*.parquet")
    if gold_files:
        df = pd.concat(
            [pd.read_parquet(f) for f in gold_files],
            ignore_index=True
        )
        if "intent_level" in df.columns:
            counts = df["intent_level"].value_counts()
            total  = len(df)
            for level in ["hot", "warm", "cool", "cold"]:
                count = int(counts.get(level, 0))
                pct   = round(count / total * 100, 1) if total > 0 else 0
                bar   = "█" * (count // 2)
                emoji = {"hot":"🔥","warm":"☀️","cool":"❄️","cold":"🧊"}.get(level,"")
                print(f"  {emoji} {level:<8} {count:>4} ({pct:>5}%)  {bar}")

        if "lead_score" in df.columns:
            print(f"\n  Avg Score : {df['lead_score'].mean():.1f}")
            print(f"  Max Score : {df['lead_score'].max()}")
            print(f"  Min Score : {df['lead_score'].min()}")
    else:
        print("  No Gold data found")

    # ── Quality reports ───────────────────────────────────────
    print("\n📋 LATEST QUALITY REPORT")
    reports = glob.glob("./monitoring/reports/*.json")
    if reports:
        latest = max(reports, key=os.path.getctime)
        with open(latest) as f:
            results = json.load(f)
        passed = sum(1 for r in results if r.get("passed"))
        total  = len(results)
        pct    = round(passed / total * 100, 1) if total > 0 else 0
        print(f"  Report    : {os.path.basename(latest)}")
        print(f"  Checks    : {total}")
        print(f"  Passed    : {passed} / {total} ({pct}%)")

        # Show failures
        failures = [r for r in results if not r.get("passed")]
        if failures:
            print(f"\n  ❌ Failures:")
            for f in failures:
                print(f"     {f['layer']} | {f['expectation']} | {f['message']}")
        else:
            print("  ✅ All checks passed")
    else:
        print("  No quality reports found — run data_quality_checks.py first")

    # ── Docker services ───────────────────────────────────────
    print("\n🐳 SERVICES")
    services = [
        ("Kafka UI",    "http://localhost:8080"),
        ("Grafana",     "http://localhost:3001"),
        ("Prometheus",  "http://localhost:9090"),
        ("n8n",         "http://localhost:5678"),
        ("Metrics",     "http://localhost:8000/metrics"),
    ]
    for name, url in services:
        print(f"  {name:<15} {url}")

    # ── Overall status ────────────────────────────────────────
    print("\n" + "=" * 60)
    if all_ok:
        print("  ✅ PLATFORM STATUS: HEALTHY")
    else:
        print("  ⚠️  PLATFORM STATUS: NEEDS ATTENTION")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_health_check()