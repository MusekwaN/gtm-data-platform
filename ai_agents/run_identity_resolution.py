# ai_agents/run_identity_resolution.py
"""
Runs identity resolution on Silver leads
and saves unified Golden Records.
"""

import os
import sys
import glob
import pandas as pd
from datetime import datetime, timezone

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from ai_agents.identity_resolver import IdentityResolver

# ── Paths ────────────────────────────────────────────────────
SILVER_PATH  = "./data/silver/leads"
UNIFIED_PATH = "./data/unified"
os.makedirs(UNIFIED_PATH, exist_ok=True)


def load_silver_data() -> pd.DataFrame:
    """Load all Silver parquet files."""
    files = glob.glob(f"{SILVER_PATH}/*.parquet")
    if not files:
        print("No Silver data found. Run simple_pipeline.py first.")
        sys.exit(1)

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    print(f"Loaded {len(df)} Silver records from {len(files)} files")
    return df


def save_unified(df: pd.DataFrame):
    """Save unified Golden Records to parquet."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{UNIFIED_PATH}/unified_{timestamp}.parquet"
    df.to_parquet(path, index=False)
    print(f"Saved {len(df)} unified records to {path}")
    return path


def print_sample(df: pd.DataFrame):
    """Print sample unified records."""
    print("\nSample Unified Records:")
    print("-" * 80)
    cols = ["global_id","full_name","email","company_name",
            "sources","record_count","seniority"]
    print(df[cols].head(10).to_string(index=False))

    print("\nMulti-source matches (same person, multiple systems):")
    multi = df[df["source_count"] > 1][cols]
    if len(multi) > 0:
        print(multi.to_string(index=False))
    else:
        print("No multi-source matches found with current data")
        print("(Add HubSpot/Salesforce data to see cross-system matching)")


def main():
    print("=" * 55)
    print("  GTM Identity Resolution Engine")
    print("=" * 55)

    # Load Silver data
    silver_df = load_silver_data()

    # Run resolver
    resolver = IdentityResolver(confidence_threshold=75)
    unified_df = resolver.resolve(silver_df)

    # Save results
    save_unified(unified_df)

    # Print match log
    match_log = resolver.get_match_log()
    if not match_log.empty:
        print(f"\nMatch Log ({len(match_log)} matches found):")
        print(match_log.to_string(index=False))

    # Print sample
    print_sample(unified_df)

    # Summary stats
    print("\nResolution Summary:")
    print(f"  Total input records  : {len(silver_df)}")
    print(f"  Unified identities   : {len(unified_df)}")
    print(f"  Duplicates removed   : {len(silver_df) - len(unified_df)}")
    print(f"  Reduction %          : {((len(silver_df)-len(unified_df))/len(silver_df)*100):.1f}%")

    return unified_df


if __name__ == "__main__":
    main()