# ai_agents/identity_resolver.py
"""
Identity Resolution Engine
Matches the same person across Apollo, HubSpot, Salesforce
into a single unified Golden Record.
"""

import os
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from datetime import datetime, timezone
import hashlib
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# MATCHING RULES
# ─────────────────────────────────────────────────────────────

def normalize_email(email: str) -> str:
    """Lowercase and strip email."""
    if not email or email == "None":
        return ""
    return email.lower().strip()


def normalize_name(name: str) -> str:
    """Lowercase, strip, remove punctuation."""
    if not name or name == "None":
        return ""
    return name.lower().strip().replace(".", "").replace(",", "")


def normalize_domain(email: str) -> str:
    """Extract domain from email."""
    email = normalize_email(email)
    if "@" in email:
        return email.split("@")[1]
    return ""


def exact_email_match(email1: str, email2: str) -> bool:
    """Two records are same person if emails match exactly."""
    e1 = normalize_email(email1)
    e2 = normalize_email(email2)
    return bool(e1 and e2 and e1 == e2)


def domain_name_match(rec1: dict, rec2: dict, threshold: int = 85) -> bool:
    """
    Same company domain + similar full name = likely same person.
    Example: alice.johnson@techcorp.com vs alicej@techcorp.com
    """
    domain1 = normalize_domain(rec1.get("email", ""))
    domain2 = normalize_domain(rec2.get("email", ""))

    if not domain1 or not domain2:
        return False

    # Must be same company domain
    if domain1 != domain2:
        return False

    # Check name similarity
    name1 = normalize_name(rec1.get("full_name", ""))
    name2 = normalize_name(rec2.get("full_name", ""))

    if not name1 or not name2:
        return False

    similarity = fuzz.token_sort_ratio(name1, name2)
    return similarity >= threshold


def company_name_fuzzy_match(rec1: dict, rec2: dict, threshold: int = 90) -> bool:
    """
    Same company name + very similar full name = likely same person.
    Fallback when no email domain available.
    """
    name1 = normalize_name(rec1.get("full_name", ""))
    name2 = normalize_name(rec2.get("full_name", ""))
    company1 = normalize_name(rec1.get("company_name", ""))
    company2 = normalize_name(rec2.get("company_name", ""))

    if not all([name1, name2, company1, company2]):
        return False

    name_similarity    = fuzz.token_sort_ratio(name1, name2)
    company_similarity = fuzz.token_sort_ratio(company1, company2)

    return name_similarity >= threshold and company_similarity >= 90


def calculate_match_confidence(rec1: dict, rec2: dict) -> dict:
    """
    Calculate match type and confidence score between two records.
    Returns match_type and confidence (0-100).
    """
    # Rule 1: Exact email match (highest confidence)
    if exact_email_match(rec1.get("email",""), rec2.get("email","")):
        return {"match_type": "exact_email", "confidence": 100}

    # Rule 2: Same domain + similar name
    if domain_name_match(rec1, rec2, threshold=85):
        name1 = normalize_name(rec1.get("full_name",""))
        name2 = normalize_name(rec2.get("full_name",""))
        similarity = fuzz.token_sort_ratio(name1, name2)
        return {
            "match_type": "domain_name_fuzzy",
            "confidence": int(similarity * 0.9)   # slightly lower confidence
        }

    # Rule 3: Same company + very similar name
    if company_name_fuzzy_match(rec1, rec2, threshold=90):
        return {"match_type": "company_name_fuzzy", "confidence": 75}

    return {"match_type": "no_match", "confidence": 0}


# ─────────────────────────────────────────────────────────────
# GOLDEN RECORD BUILDER
# ─────────────────────────────────────────────────────────────

def generate_global_id(email: str, company_domain: str) -> str:
    """Generate a stable unique ID for a person."""
    key = f"{normalize_email(email)}:{company_domain}"
    return "gid_" + hashlib.md5(key.encode()).hexdigest()[:12]


def merge_records(records: list) -> dict:
    """
    Merge multiple records for the same person into one Golden Record.
    Strategy: take the most complete value for each field.
    """
    def best_value(field: str):
        """Return first non-null, non-None value across records."""
        for r in sorted(records, key=lambda x: x.get("apollo_score") or 0, reverse=True):
            val = r.get(field)
            if val and str(val) not in ("None", "nan", ""):
                return val
        return None

    sources = list(set(r.get("source","unknown") for r in records))
    email   = best_value("email") or ""

    return {
        "global_id":       generate_global_id(email, best_value("email_domain") or ""),
        "full_name":       best_value("full_name"),
        "email":           email,
        "email_domain":    best_value("email_domain"),
        "job_title":       best_value("job_title"),
        "seniority":       best_value("seniority"),
        "company_name":    best_value("company_name"),
        "company_domain":  best_value("company_domain"),
        "industry":        best_value("industry"),
        "employee_count":  best_value("employee_count"),
        "apollo_score":    best_value("apollo_score"),
        "lead_status":     best_value("lead_status"),
        "sources":         ",".join(sources),
        "source_count":    len(sources),
        "record_count":    len(records),
        "resolved_at":     datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# MAIN RESOLVER
# ─────────────────────────────────────────────────────────────

class IdentityResolver:

    def __init__(self, confidence_threshold: int = 75):
        self.confidence_threshold = confidence_threshold
        self.match_log = []

    def resolve(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main resolution function.
        Takes a DataFrame of leads, returns unified Golden Records.
        """
        logger.info(f"Starting identity resolution on {len(df)} records...")

        records = df.to_dict("records")

        # Build clusters of matching records
        clusters = self._cluster_records(records)

        logger.info(f"Resolved {len(records)} records into {len(clusters)} unique identities")

        # Build Golden Records from clusters
        golden_records = [merge_records(cluster) for cluster in clusters]

        result_df = pd.DataFrame(golden_records)

        # Log match statistics
        self._log_stats(records, golden_records)

        return result_df

    def _cluster_records(self, records: list) -> list:
        """
        Group records that belong to the same person.
        Uses Union-Find algorithm for efficient clustering.
        """
        n = len(records)

        # Union-Find setup
        parent = list(range(n))

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(x, y):
            parent[find(x)] = find(y)

        # Compare all pairs and union matches
        match_count = 0
        for i in range(n):
            for j in range(i + 1, n):
                result = calculate_match_confidence(records[i], records[j])
                if result["confidence"] >= self.confidence_threshold:
                    union(i, j)
                    match_count += 1
                    self.match_log.append({
                        "record_1":   records[i].get("email"),
                        "record_2":   records[j].get("email"),
                        "match_type": result["match_type"],
                        "confidence": result["confidence"],
                    })

        logger.info(f"Found {match_count} matching pairs")

        # Group by cluster
        clusters = {}
        for i in range(n):
            root = find(i)
            if root not in clusters:
                clusters[root] = []
            clusters[root].append(records[i])

        return list(clusters.values())

    def _log_stats(self, original: list, golden: list):
        """Print resolution statistics."""
        duplicates_removed = len(original) - len(golden)
        multi_source = sum(1 for g in golden if g.get("source_count", 1) > 1)

        logger.info("=" * 50)
        logger.info("  IDENTITY RESOLUTION COMPLETE")
        logger.info("=" * 50)
        logger.info(f"  Input records    : {len(original)}")
        logger.info(f"  Golden records   : {len(golden)}")
        logger.info(f"  Duplicates merged: {duplicates_removed}")
        logger.info(f"  Multi-source IDs : {multi_source}")
        logger.info("=" * 50)

    def get_match_log(self) -> pd.DataFrame:
        """Return a DataFrame of all matches found."""
        return pd.DataFrame(self.match_log)