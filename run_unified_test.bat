@echo off
REM Test Unified NATS Message Format
REM Usage: run_unified_test.bat [video_path] [camera_id]

echo ====================================
echo Unified Message Format Test
echo ====================================
echo.

REM Check if virtual environment exists
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Install dependencies if needed
echo Checking dependencies...
pip install -q nats-py aiokafka opencv-python

echo.
echo ====================================
echo Running Test
echo ====================================
echo.

REM Run with arguments if provided, otherwise use defaults
if "%~1"=="" (
    python test_unified_format.py
) else if "%~2"=="" (
    python test_unified_format.py "%~1"
) else (
    python test_unified_format.py "%~1" "%~2"
)

echo.
echo ====================================
echo Test Complete
echo ====================================
echo.

pause
