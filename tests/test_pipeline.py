"""
Unit tests for the GTM pipeline.
Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ai_agents.identity_resolver import (
    normalize_email,
    normalize_name,
    normalize_domain,
    exact_email_match,
    domain_name_match,
    calculate_match_confidence,
    generate_global_id,
    merge_records,
)
from monitoring.quality.data_quality_checks import (
    expect_no_nulls,
    expect_unique,
    expect_row_count_above,
    expect_email_format,
    expect_values_in_set,
)


# ── Identity Resolver Tests ───────────────────────────────────

class TestNormalization:
    def test_normalize_email_lowercase(self):
        assert normalize_email("ALICE@TEST.COM") == "alice@test.com"

    def test_normalize_email_strips_spaces(self):
        assert normalize_email("  alice@test.com  ") == "alice@test.com"

    def test_normalize_email_none(self):
        assert normalize_email(None) == ""

    def test_normalize_domain(self):
        assert normalize_domain("alice@techcorp.com") == "techcorp.com"

    def test_normalize_name(self):
        assert normalize_name("  Alice Johnson  ") == "alice johnson"


class TestMatching:
    def test_exact_email_match(self):
        assert exact_email_match("alice@test.com", "alice@test.com") is True

    def test_exact_email_case_insensitive(self):
        assert exact_email_match("ALICE@TEST.COM", "alice@test.com") is True

    def test_no_email_match(self):
        assert exact_email_match("alice@test.com", "bob@test.com") is False

    def test_confidence_exact_match(self):
        rec1 = {"email": "alice@test.com", "full_name": "Alice Johnson"}
        rec2 = {"email": "alice@test.com", "full_name": "Alice J"}
        result = calculate_match_confidence(rec1, rec2)
        assert result["confidence"] == 100
        assert result["match_type"] == "exact_email"

    def test_confidence_no_match(self):
        rec1 = {"email": "alice@test.com",  "full_name": "Alice Johnson"}
        rec2 = {"email": "bob@other.com",   "full_name": "Bob Smith"}
        result = calculate_match_confidence(rec1, rec2)
        assert result["confidence"] == 0
        assert result["match_type"] == "no_match"


class TestMergeRecords:
    def test_merge_two_records(self):
        records = [
            {"email": "alice@test.com", "full_name": "Alice J",
             "company_name": "TechCorp", "apollo_score": 80,
             "email_domain": "test.com"},
            {"email": "alice@test.com", "full_name": "Alice Johnson",
             "company_name": "TechCorp", "apollo_score": 90,
             "email_domain": "test.com"},
        ]
        merged = merge_records(records)
        assert merged["email"] == "alice@test.com"
        assert merged["company_name"] == "TechCorp"
        assert merged["record_count"] == 2


# ── Data Quality Tests ────────────────────────────────────────

class TestDataQuality:
    @pytest.fixture
    def good_df(self):
        return pd.DataFrame({
            "email":        ["alice@test.com", "bob@test.com"],
            "lead_id":      ["lead_001", "lead_002"],
            "source":       ["apollo", "hubspot"],
            "intent_level": ["hot", "warm"],
        })

    @pytest.fixture
    def bad_df(self):
        return pd.DataFrame({
            "email":   ["alice@test.com", None],
            "lead_id": ["lead_001", "lead_001"],
        })

    def test_no_nulls_passes(self, good_df):
        result = expect_no_nulls(good_df, "email")
        assert result["passed"] is True

    def test_no_nulls_fails(self, bad_df):
        result = expect_no_nulls(bad_df, "email")
        assert result["passed"] is False

    def test_unique_passes(self, good_df):
        result = expect_unique(good_df, "email")
        assert result["passed"] is True

    def test_unique_fails(self, bad_df):
        result = expect_unique(bad_df, "lead_id")
        assert result["passed"] is False

    def test_row_count_passes(self, good_df):
        result = expect_row_count_above(good_df, min_rows=1)
        assert result["passed"] is True

    def test_email_format_passes(self, good_df):
        result = expect_email_format(good_df, "email")
        assert result["passed"] is True

    def test_values_in_set_passes(self, good_df):
        result = expect_values_in_set(
            good_df, "intent_level", ["hot", "warm", "cool", "cold"]
        )
        assert result["passed"] is True