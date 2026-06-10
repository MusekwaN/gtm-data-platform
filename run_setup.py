#!/usr/bin/env python3
import subprocess
import sys

result = subprocess.run([sys.executable, 'setup_ci_cd.py'], cwd=r'C:\Users\Student\gtm-data-platform', capture_output=True, text=True)
print(result.stdout)
if result.stderr:
    print(result.stderr, file=sys.stderr)
sys.exit(result.returncode)
