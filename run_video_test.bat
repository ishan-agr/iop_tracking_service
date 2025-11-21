@echo off
echo ================================================================================
echo VIDEO INGRESS/EGRESS TRACKING TEST
echo ================================================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://www.python.org/
    pause
    exit /b 1
)

REM Check if video file exists
set VIDEO_PATH=C:\Users\ishan\Downloads\ingress_outgress_car.mp4
if not exist "%VIDEO_PATH%" (
    echo ERROR: Video file not found at:
    echo %VIDEO_PATH%
    echo.
    echo Please update the VIDEO_PATH variable in this script
    pause
    exit /b 1
)

echo Found video: %VIDEO_PATH%
echo.

REM Install dependencies
echo Installing required Python packages...
pip install -q nats-py opencv-python numpy 2>nul
if errorlevel 1 (
    echo WARNING: Some packages may already be installed
)
echo.

REM Run the test
echo Starting video test...
echo.
python test_video.py "%VIDEO_PATH%"

echo.
echo ================================================================================
echo TEST COMPLETE
echo ================================================================================
pause
