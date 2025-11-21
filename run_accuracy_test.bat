@echo off
echo ================================================================================
echo INGRESS/EGRESS ACCURACY TEST - Standalone Mode
echo ================================================================================
echo.
echo This test runs WITHOUT Docker/NATS/Kafka
echo Processing video directly with tracking pipeline
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not installed
    pause
    exit /b 1
)

REM Check video file
set VIDEO_PATH=C:\Users\ishan\Downloads\ingress_outgress_car.mp4
if not exist "%VIDEO_PATH%" (
    echo ERROR: Video file not found at: %VIDEO_PATH%
    echo.
    echo Please update VIDEO_PATH in this script
    pause
    exit /b 1
)

echo Input video: %VIDEO_PATH%
echo Output: %VIDEO_PATH:~0,-4%_annotated.mp4
echo.

REM Install dependencies
echo Installing dependencies (if needed)...
pip install -q opencv-python numpy ultralytics boxmot pydantic pydantic-settings structlog 2>nul
echo.

REM Set minimal environment (no MinIO needed for this test)
set MINIO_ENDPOINT=localhost:9000
set MINIO_ACCESS_KEY=minioadmin
set MINIO_SECRET_KEY=minioadmin123
set MODEL_DEVICE=cuda:0
set LOG_LEVEL=INFO
set SAVE_VEHICLE_CROPS=false

echo Starting accuracy test...
echo.

REM Run test
python test_accuracy.py "%VIDEO_PATH%"

echo.
echo ================================================================================
echo Test complete! Check the annotated video.
echo ================================================================================
pause
