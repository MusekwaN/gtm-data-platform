@echo off
REM Comprehensive script validation and execution guide
cd /d C:\Users\Student\gtm-data-platform

echo.
echo ============================================================================
echo   PIPELINE SCRIPTS - COMPREHENSIVE VALIDATION & EXECUTION GUIDE
echo ============================================================================
echo.

echo Running validation suite...
echo.

python run_and_validate_scripts.py

echo.
echo ============================================================================
echo   VALIDATION COMPLETE
echo ============================================================================
echo.
echo To run the actual pipeline scripts:
echo.
echo   1. Install dependencies:
echo      pip install -r requirements.txt
echo.
echo   2. Start Kafka broker (if not already running)
echo.
echo   3. Run scripts in separate terminals:
echo      Terminal 1: python streaming\spark_jobs\bronze_ingestion.py
echo      Terminal 2: python streaming\spark_jobs\silver_processing.py
echo      Terminal 3: python streaming\spark_jobs\gold_lead_scoring.py
echo.
pause
