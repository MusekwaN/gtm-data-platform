# ai_agents/lead_scoring/ai_pipeline.py
"""
AI Pipeline — runs all agents in sequence for each lead.
EnrichmentAgent → IntentAgent → OutreachAgent
"""

import os
import sys
import glob
import json
import pandas as pd
from datetime import datetime, timezone
import time

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from enrichment_agent import EnrichmentAgent
from intent_agent     import IntentAgent
from outreach_agent   import OutreachAgent

# ── Output path ───────────────────────────────────────────────
OUTPUT_PATH = "./data/ai_insights"
os.makedirs(OUTPUT_PATH, exist_ok=True)


def run_pipeline(max_leads: int = 10):
    """
    Run the full AI agent pipeline on top leads.
    Limit to max_leads to control API costs.
    """
    print("=" * 55)
    print("  GTM AI Lead Intelligence Pipeline")
    print("=" * 55)

    # ── Load Gold leads ───────────────────────────────────────
    gold_files = glob.glob("./data/gold/lead_scores/*.parquet")
    if not gold_files:
        print("No Gold data found. Run simple_pipeline.py first.")
        return

    gold_df = pd.concat(
        [pd.read_parquet(f) for f in gold_files],
        ignore_index=True
    )

    # Process only top leads by score to save API costs
    top_leads = (
        gold_df
        .sort_values("lead_score", ascending=False)
        .drop_duplicates(subset=["email"])
        .head(max_leads)
        .to_dict("records")
    )

    print(f"\nProcessing top {len(top_leads)} leads through AI agents...")
    print(f"Agents: Enrichment -> Intent -> Outreach\n")

    # ── Initialize agents ─────────────────────────────────────
    enrichment_agent = EnrichmentAgent()
    intent_agent     = IntentAgent()
    outreach_agent   = OutreachAgent()

    # ── Run pipeline per lead ─────────────────────────────────
    results = []
    for i, lead in enumerate(top_leads, 1):
        name    = lead.get("full_name", "Unknown")
        company = lead.get("company_name", "Unknown")
        score   = lead.get("lead_score", 0)

        print(f"[{i}/{len(top_leads)}] {name} @ {company} (score: {score})")

        try:
            # Stage 1: Enrich
            print(f"  -> Enrichment Agent...")
            enriched = enrichment_agent.run(lead)

            # Stage 2: Intent
            print(f"  -> Intent Agent...")
            with_intent = intent_agent.run(enriched)

            # Stage 3: Outreach
            print(f"  -> Outreach Agent...")
            final = outreach_agent.run(with_intent)

            # Add pipeline metadata
            final["pipeline_version"] = "1.0"
            final["processed_at"]     = datetime.now(timezone.utc).isoformat()

            results.append(final)
            print(f"  Done | Intent: {final.get('intent_level')} | "
                f"Score: {final.get('intent_score')}")

        except Exception as e:
            print(f"  ✗ Failed: {e}")
            continue

        # Small delay to avoid rate limiting
        time.sleep(1)

    # ── Save results ──────────────────────────────────────────
    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{OUTPUT_PATH}/ai_insights_{timestamp}.json"

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\nSaved {len(results)} AI insights to {output_file}")

        # Also save as parquet for Snowflake loading
        results_df = pd.DataFrame(results)

        # Flatten list fields to strings for storage
        for col in results_df.columns:
            if results_df[col].dtype == object:
                results_df[col] = results_df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, list) else x
                )

        parquet_file = f"{OUTPUT_PATH}/ai_insights_{timestamp}.parquet"
        results_df.to_parquet(parquet_file, index=False)
        print(f"Saved parquet: {parquet_file}")

        # ── Print summary ─────────────────────────────────────
        print_summary(results)

    return results


def print_summary(results: list):
    """Print a readable summary of AI insights."""
    print("\n" + "=" * 55)
    print("  AI PIPELINE RESULTS")
    print("=" * 55)

    for r in results:
        print("\n" + "-" * 50)
        print(f"  {r.get('full_name')} @ {r.get('company_name')}")
        print(f"  Title:        {r.get('job_title')}")
        print(f"  Intent:       {r.get('intent_level')} "
              f"(score: {r.get('intent_score')})")
        print(f"  Urgency:      {r.get('urgency')}")
        print(f"  Deal Size:    {r.get('deal_size_estimate')}")
        print(f"  Next Action:  {r.get('next_best_action')}")
        print(f"  Email Subj:   {r.get('email_subject')}")
        print(f"  Pain Points:  {r.get('pain_points')}")


if __name__ == "__main__":
    run_pipeline(max_leads=5)  # start with 5 to test