"""Test script to simulate 2 cameras sending frames to the tracking service.

This demonstrates production deployment with multiple camera streams.
"""

import asyncio
import base64
import json
from datetime import datetime
import nats


async def send_line_config(nc, camera_id: str, camera_name: str, area_id: str):
    """Send line configuration for a camera."""

    # Different line positions for each camera
    if "entrance" in camera_id.lower():
        line_config = {
            "cameraId": camera_id,
            "cameraName": camera_name,
            "areaId": area_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "config": [
                {
                    "points": [
                        {"x": 100, "y": 300},  # Line start
                        {"x": 500, "y": 320}   # Line end
                    ]
                },
                {
                    "points": [
                        {"x": 300, "y": 250}   # Ingress side indicator
                    ]
                }
            ]
        }
    else:  # exit camera
        line_config = {
            "cameraId": camera_id,
            "cameraName": camera_name,
            "areaId": area_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "config": [
                {
                    "points": [
                        {"x": 150, "y": 400},  # Line start
                        {"x": 550, "y": 410}   # Line end
                    ]
                },
                {
                    "points": [
                        {"x": 350, "y": 350}   # Ingress side indicator
                    ]
                }
            ]
        }

    message = json.dumps(line_config).encode()
    await nc.publish("config.tracking.lines", message)
    print(f"✅ Sent line config for {camera_id}")


async def send_test_frame(nc, camera_id: str, camera_name: str, area_id: str, frame_number: int):
    """Send a test frame (blank frame for demonstration)."""

    # Create a simple test image (100x100 black image)
    import cv2
    import numpy as np

    # Create test image with camera ID text
    img = np.zeros((480, 640, 3), dtype=np.uint8)
    cv2.putText(img, camera_id, (50, 240), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
    cv2.putText(img, f"Frame {frame_number}", (50, 280), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 1)

    # Encode to base64
    _, buffer = cv2.imencode('.jpg', img)
    frame_b64 = base64.b64encode(buffer).decode('utf-8')

    frame_message = {
        "cameraId": camera_id,
        "cameraName": camera_name,
        "areaId": area_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data": frame_b64,
        "modelId": "tracking",
        "metadata": {
            "frameNumber": frame_number,
            "width": 640,
            "height": 480,
            "sourceFps": 30
        }
    }

    message = json.dumps(frame_message).encode()
    await nc.publish("frames.ml.tracking", message)


async def simulate_camera(nc, camera_id: str, camera_name: str, area_id: str, num_frames: int = 10):
    """Simulate a camera sending frames."""

    print(f"\n🎥 Starting simulation for {camera_name} ({camera_id})")

    # Send line configuration first
    await send_line_config(nc, camera_id, camera_name, area_id)

    # Wait a bit for configuration to be processed
    await asyncio.sleep(1)

    # Send test frames
    for i in range(num_frames):
        await send_test_frame(nc, camera_id, camera_name, area_id, i + 1)
        print(f"  📹 {camera_name}: Sent frame {i + 1}/{num_frames}")
        await asyncio.sleep(0.1)  # 10 FPS for testing

    print(f"✅ Completed simulation for {camera_name}")


async def main():
    """Main test function."""

    print("=" * 80)
    print("🚀 TWO CAMERA SIMULATION TEST")
    print("=" * 80)
    print("\nThis script demonstrates:")
    print("  1. Configuring lines for 2 cameras")
    print("  2. Sending frames from both cameras simultaneously")
    print("  3. Service processes both streams independently")
    print("\n" + "=" * 80 + "\n")

    # Connect to NATS
    try:
        nc = await nats.connect("nats://localhost:4222")
        print("✅ Connected to NATS\n")
    except Exception as e:
        print(f"❌ Failed to connect to NATS: {e}")
        print("   Make sure NATS is running: docker-compose up -d nats")
        return

    # Define cameras
    cameras = [
        {
            "camera_id": "entrance-cam-01",
            "camera_name": "Main Entrance",
            "area_id": "parking-zone-A"
        },
        {
            "camera_id": "exit-cam-02",
            "camera_name": "Exit Gate",
            "area_id": "parking-zone-B"
        }
    ]

    # Run both cameras in parallel
    tasks = []
    for cam in cameras:
        task = simulate_camera(
            nc,
            cam["camera_id"],
            cam["camera_name"],
            cam["area_id"],
            num_frames=10
        )
        tasks.append(task)

    # Wait for all simulations to complete
    await asyncio.gather(*tasks)

    # Close connection
    await nc.close()

    print("\n" + "=" * 80)
    print("✅ SIMULATION COMPLETE")
    print("=" * 80)
    print("\nNext steps:")
    print("  1. Check service logs: docker logs -f ingress-egress-tracking-service")
    print("  2. View camera metrics: curl http://localhost:8100/cameras")
    print("  3. Check Kafka events: kafka-console-consumer --topic tracking.events.entrance-cam-01")
    print("\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
