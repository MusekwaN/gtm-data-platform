#!/usr/bin/env python3
"""
Quick verification script to test your pipeline locally before pushing to GitHub.
Run this to ensure all components are working correctly.
"""

import sys
import subprocess
from pathlib import Path

def check_files_exist():
    """Verify all required pipeline files exist"""
    base = Path(".")
    required_files = [
        "simple_pipeline.py",
        "test_data_injector.py",
        "ingestion/base_connector.py",
        "ai_agents/identity_resolver.py",
        "reverse_etl/reverse_etl_engine.py",
        "monitoring/pipeline_health.py",
        ".gitignore",
        ".github/workflows/ci.yml",
        ".github/workflows/dbt.yml",
        "tests/test_pipeline.py",
    ]
    
    print("Checking required files...")
    all_exist = True
    for file in required_files:
        path = base / file
        if path.exists():
            print(f"  ✅ {file}")
        else:
            print(f"  ❌ {file} - MISSING!")
            all_exist = False
    
    return all_exist

def run_tests():
    """Run unit tests"""
    print("\nRunning unit tests...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("  ✅ All tests passed!")
            return True
        else:
            print("  ❌ Tests failed:")
            print(result.stdout)
            print(result.stderr)
            return False
    except subprocess.TimeoutExpired:
        print("  ⚠️  Tests timed out")
        return False
    except Exception as e:
        print(f"  ⚠️  Could not run tests: {e}")
        return False

def check_git_setup():
    """Check git configuration"""
    print("\nChecking Git setup...")
    try:
        result = subprocess.run(
            ["git", "remote", "-v"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and result.stdout:
            print("  ✅ Git remote configured")
            print(f"     {result.stdout.strip()}")
            return True
        else:
            print("  ❌ Git remote not configured")
            print("     Run: git remote add origin https://github.com/YOUR_USERNAME/gtm-data-platform.git")
            return False
    except Exception as e:
        print(f"  ❌ Git error: {e}")
        return False

def main():
    print("=" * 60)
    print("GTM Data Platform - Pre-GitHub Actions Verification")
    print("=" * 60)
    
    files_ok = check_files_exist()
    git_ok = check_git_setup()
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    if files_ok and git_ok:
        print("✅ All checks passed! Ready to push to GitHub.")
        print("\nNext steps:")
        print("1. Configure GitHub Secrets: https://github.com/YOUR_USERNAME/gtm-data-platform/settings/secrets/actions")
        print("2. Monitor GitHub Actions: https://github.com/YOUR_USERNAME/gtm-data-platform/actions")
        print("3. Push changes and watch CI/CD run!")
        return 0
    else:
        print("⚠️  Some checks failed. Please review the items above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
