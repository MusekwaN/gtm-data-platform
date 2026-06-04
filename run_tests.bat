@echo off
cd /d C:\Users\Student\gtm-data-platform
echo.
echo ========================================
echo Running Pipeline Script Tests
echo ========================================
echo.
python test_pipeline_scripts.py
if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo All tests PASSED successfully!
    echo ========================================
) else (
    echo.
    echo ========================================
    echo Tests FAILED - Check output above
    echo ========================================
)
pause
