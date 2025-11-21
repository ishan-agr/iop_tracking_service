@echo off
REM Dual Camera Accuracy Test Runner
REM Usage: run_dual_test.bat [video1.mp4] [video2.mp4] [fps1] [fps2]

echo ====================================
echo Dual Camera Accuracy Test
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
pip install -q -r requirements.txt

echo.
echo ====================================
echo Running Dual Camera Test
echo ====================================
echo.

REM Run with arguments if provided, otherwise use defaults
if "%~1"=="" (
    python test_accuracy_dual.py
) else if "%~2"=="" (
    python test_accuracy_dual.py "%~1"
) else if "%~3"=="" (
    python test_accuracy_dual.py "%~1" "%~2"
) else if "%~4"=="" (
    python test_accuracy_dual.py "%~1" "%~2" %~3
) else (
    python test_accuracy_dual.py "%~1" "%~2" %~3 %~4
)

echo.
echo ====================================
echo Test Complete
echo ====================================
echo.

pause
