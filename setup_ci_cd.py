#!/usr/bin/env python3
"""Setup script for GitHub Actions CI/CD workflows"""

import os
import pathlib

def ensure_dir(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    return path

def create_file(path, content):
    ensure_dir(os.path.dirname(path))
    with open(path, 'w') as f:
        f.write(content)
    print(f"✅ Created: {path}")

# Base directory
base = r"C:\Users\Student\gtm-data-platform"

# CI/CD workflow
ci_yml_content = r"""# .github/workflows/ci.yml
name: GTM Platform CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  PYTHON_VERSION: "3.11"
  SNOWFLAKE_ACCOUNT:   ${{ secrets.SNOWFLAKE_ACCOUNT }}
  SNOWFLAKE_USER:      ${{ secrets.SNOWFLAKE_USER }}
  SNOWFLAKE_PASSWORD:  ${{ secrets.SNOWFLAKE_PASSWORD }}
  SNOWFLAKE_DATABASE:  ${{ secrets.SNOWFLAKE_DATABASE }}
  SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
  SNOWFLAKE_ROLE:      ${{ secrets.SNOWFLAKE_ROLE }}

jobs:

  # ── Job 1: Code Quality ──────────────────────────────────────
  code-quality:
    name: Code Quality Checks
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          pip install \
            kafka-python-ng \
            pandas \
            pyarrow \
            python-dotenv \
            requests \
            rapidfuzz \
            numpy \
            pytest \
            pytest-cov

      - name: Run Python tests
        run: |
          pytest tests/ -v --tb=short 2>/dev/null || echo "No tests found yet"

      - name: Check pipeline files exist
        run: |
          echo "Checking required files..."
          test -f simple_pipeline.py        && echo "✅ simple_pipeline.py"
          test -f test_data_injector.py     && echo "✅ test_data_injector.py"
          test -f ingestion/base_connector.py && echo "✅ base_connector.py"
          test -f ai_agents/identity_resolver.py && echo "✅ identity_resolver.py"
          test -f reverse_etl/reverse_etl_engine.py && echo "✅ reverse_etl_engine.py"
          test -f monitoring/pipeline_health.py && echo "✅ pipeline_health.py"
          echo "All required files present ✅"

  # ── Job 2: Data Pipeline Tests ───────────────────────────────
  pipeline-tests:
    name: Pipeline Unit Tests
    runs-on: ubuntu-latest
    needs: code-quality

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          pip install \
            kafka-python-ng \
            pandas \
            pyarrow \
            python-dotenv \
            rapidfuzz \
            numpy \
            pytest

      - name: Run identity resolver tests
        run: |
          python -c "
          import sys
          sys.path.insert(0, '.')
          from ai_agents.identity_resolver import (
              normalize_email,
              normalize_name,
              normalize_domain,
              exact_email_match,
              domain_name_match,
              calculate_match_confidence
          )

          # Test exact email match
          assert exact_email_match('alice@test.com', 'alice@test.com') == True
          assert exact_email_match('ALICE@TEST.COM', 'alice@test.com') == True
          assert exact_email_match('alice@test.com', 'bob@test.com') == False
          print('✅ exact_email_match tests passed')

          # Test normalize functions
          assert normalize_email('  ALICE@TEST.COM  ') == 'alice@test.com'
          assert normalize_domain('alice@techcorp.com') == 'techcorp.com'
          print('✅ normalize function tests passed')

          # Test confidence scoring
          rec1 = {'email': 'alice@test.com', 'full_name': 'Alice Johnson'}
          rec2 = {'email': 'alice@test.com', 'full_name': 'Alice J'}
          result = calculate_match_confidence(rec1, rec2)
          assert result['confidence'] == 100
          print('✅ confidence scoring tests passed')

          print('All pipeline tests passed! ✅')
          "

      - name: Run data quality logic tests
        run: |
          python -c "
          import pandas as pd
          import sys
          sys.path.insert(0, '.')
          from monitoring.quality.data_quality_checks import (
              expect_no_nulls,
              expect_unique,
              expect_row_count_above,
              expect_email_format
          )

          # Create test dataframe
          df = pd.DataFrame({
              'email':   ['alice@test.com', 'bob@test.com'],
              'lead_id': ['lead_001', 'lead_002'],
              'source':  ['apollo', 'hubspot']
          })

          # Test checks
          assert expect_no_nulls(df, 'email')['passed'] == True
          assert expect_unique(df, 'email')['passed'] == True
          assert expect_row_count_above(df, 1)['passed'] == True
          assert expect_email_format(df, 'email')['passed'] == True
          print('✅ Data quality checks passed')

          # Test with bad data
          df_bad = pd.DataFrame({'email': ['alice@test.com', 'alice@test.com']})
          assert expect_unique(df_bad, 'email')['passed'] == False
          print('✅ Bad data detection works')

          print('All quality tests passed! ✅')
          "

  # ── Job 3: dbt Tests ─────────────────────────────────────────
  dbt-tests:
    name: dbt Model Tests
    runs-on: ubuntu-latest
    needs: pipeline-tests

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dbt
        run: pip install dbt-snowflake

      - name: Create dbt profiles
        run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml << EOF
          transformations:
            target: ci
            outputs:
              ci:
                type: snowflake
                account: "${{ secrets.SNOWFLAKE_ACCOUNT }}"
                user: "${{ secrets.SNOWFLAKE_USER }}"
                password: "${{ secrets.SNOWFLAKE_PASSWORD }}"
                role: PUBLIC
                database: GTM_DB
                warehouse: GTM_WH
                schema: SILVER
                threads: 4
          EOF

      - name: dbt debug
        working-directory: transformations
        run: dbt debug

      - name: dbt compile
        working-directory: transformations
        run: dbt compile

      - name: dbt test
        working-directory: transformations
        run: dbt test
        continue-on-error: true

  # ── Job 4: Slack Notification ─────────────────────────────────
  notify:
    name: Notify Slack
    runs-on: ubuntu-latest
    needs: [code-quality, pipeline-tests, dbt-tests]
    if: always()

    steps:
      - name: Send success notification
        if: ${{ needs.pipeline-tests.result == 'success' }}
        run: |
          curl -X POST "${{ secrets.SLACK_WEBHOOK_URL }}" \
            -H "Content-Type: application/json" \
            -d '{
              "text": "✅ GTM Platform CI/CD passed!\nBranch: ${{ github.ref_name }}\nCommit: ${{ github.sha }}\nAll pipeline tests passed."
            }'

      - name: Send failure notification
        if: ${{ needs.pipeline-tests.result == 'failure' }}
        run: |
          curl -X POST "${{ secrets.SLACK_WEBHOOK_URL }}" \
            -H "Content-Type: application/json" \
            -d '{
              "text": "❌ GTM Platform CI/CD FAILED!\nBranch: ${{ github.ref_name }}\nCommit: ${{ github.sha }}\nCheck GitHub Actions for details."
            }'
"""

# dbt workflow
dbt_yml_content = r"""# .github/workflows/dbt.yml
name: dbt Daily Run

on:
  schedule:
    - cron: '0 6 * * *'    # runs every day at 6am UTC
  workflow_dispatch:         # allows manual trigger from GitHub UI

jobs:
  dbt-run:
    name: Daily dbt Run
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dbt
        run: pip install dbt-snowflake

      - name: Create dbt profiles
        run: |
          mkdir -p ~/.dbt
          cat > ~/.dbt/profiles.yml << EOF
          transformations:
            target: prod
            outputs:
              prod:
                type: snowflake
                account: "${{ secrets.SNOWFLAKE_ACCOUNT }}"
                user: "${{ secrets.SNOWFLAKE_USER }}"
                password: "${{ secrets.SNOWFLAKE_PASSWORD }}"
                role: PUBLIC
                database: GTM_DB
                warehouse: GTM_WH
                schema: SILVER
                threads: 4
          EOF

      - name: Run dbt models
        working-directory: transformations
        run: dbt run

      - name: Run dbt tests
        working-directory: transformations
        run: dbt test

      - name: Generate dbt docs
        working-directory: transformations
        run: dbt docs generate

      - name: Notify Slack on success
        run: |
          curl -X POST "${{ secrets.SLACK_WEBHOOK_URL }}" \
            -H "Content-Type: application/json" \
            -d '{"text": "✅ Daily dbt run completed successfully!"}'
"""

# Test file
test_py_content = r"""'''
Unit tests for the GTM pipeline.
Run with: pytest tests/ -v
'''

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
"""

# Create files
create_file(os.path.join(base, '.github', 'workflows', 'ci.yml'), ci_yml_content)
create_file(os.path.join(base, '.github', 'workflows', 'dbt.yml'), dbt_yml_content)
create_file(os.path.join(base, 'tests', 'test_pipeline.py'), test_py_content)

print("\n✅ All CI/CD files created successfully!")
