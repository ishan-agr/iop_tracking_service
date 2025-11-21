"""Test ingress/egress tracking service with a real video file.

This script:
1. Reads a video file
2. Configures an ingress/egress line
3. Sends frames to NATS
4. Monitors Kafka for crossing events
"""

import asyncio
import base64
import json
import cv2
from datetime import datetime
from pathlib import Path
import nats
import sys


class VideoTester:
    """Test the tracking service with a video file."""

    def __init__(self, video_path: str, camera_id: str = "test-cam-01"):
        self.video_path = video_path
        self.camera_id = camera_id
        self.camera_name = "Test Camera"
        self.area_id = "test-area"
        self.nc = None
        self.frame_count = 0
        self.frames_sent = 0

    async def connect_nats(self):
        """Connect to NATS server."""
        try:
            self.nc = await nats.connect("nats://localhost:4222")
            print("✅ Connected to NATS")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to NATS: {e}")
            print("   Make sure NATS is running:")
            print("   docker-compose up -d nats")
            return False

    async def send_line_config(self, line_points=None, ingress_point=None):
        """Send line configuration to the service.

        Args:
            line_points: List of 2 points [(x1, y1), (x2, y2)] or None for middle horizontal line
            ingress_point: Point (x, y) indicating ingress side or None for auto
        """
        # Get video dimensions to calculate default line
        cap = cv2.VideoCapture(self.video_path)
        ret, frame = cap.read()
        if not ret:
            print("❌ Could not read video file")
            return False

        height, width = frame.shape[:2]
        cap.release()

        # Default: horizontal line across middle of frame
        if line_points is None:
            y_middle = height // 2
            line_points = [
                (int(width * 0.2), y_middle),  # Left side (20% from left)
                (int(width * 0.8), y_middle)   # Right side (80% from left)
            ]

        # Default: ingress from top (point above line)
        if ingress_point is None:
            x_middle = width // 2
            ingress_point = (x_middle, line_points[0][1] - 50)

        line_config = {
            "cameraId": self.camera_id,
            "cameraName": self.camera_name,
            "areaId": self.area_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "config": [
                {
                    "points": [
                        {"x": float(line_points[0][0]), "y": float(line_points[0][1])},
                        {"x": float(line_points[1][0]), "y": float(line_points[1][1])}
                    ]
                },
                {
                    "points": [
                        {"x": float(ingress_point[0]), "y": float(ingress_point[1])}
                    ]
                }
            ]
        }

        message = json.dumps(line_config).encode()
        await self.nc.publish("config.tracking.lines", message)

        print(f"\n✅ Sent line configuration:")
        print(f"   Video resolution: {width}x{height}")
        print(f"   Line: ({line_points[0][0]:.0f}, {line_points[0][1]:.0f}) → ({line_points[1][0]:.0f}, {line_points[1][1]:.0f})")
        print(f"   Ingress side point: ({ingress_point[0]:.0f}, {ingress_point[1]:.0f})")
        print(f"   (Ingress = vehicles crossing from top to bottom)")
        print(f"   (Egress = vehicles crossing from bottom to top)\n")

        return True

    async def send_frame(self, frame, frame_number: int, timestamp: str):
        """Send a single frame to NATS."""
        # Encode frame to JPEG
        ret, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if not ret:
            print(f"❌ Failed to encode frame {frame_number}")
            return False

        # Convert to base64
        frame_b64 = base64.b64encode(buffer).decode('utf-8')

        # Create frame message
        frame_message = {
            "cameraId": self.camera_id,
            "cameraName": self.camera_name,
            "areaId": self.area_id,
            "timestamp": timestamp,
            "data": frame_b64,
            "modelId": "tracking",
            "metadata": {
                "frameNumber": frame_number,
                "width": frame.shape[1],
                "height": frame.shape[0],
                "sourceFps": 30
            }
        }

        message = json.dumps(frame_message).encode()
        await self.nc.publish("frames.ml.tracking", message)
        self.frames_sent += 1
        return True

    async def process_video(self, max_frames: int = None, fps: int = 30, skip_frames: int = 0):
        """Process video and send frames to NATS.

        Args:
            max_frames: Maximum frames to send (None = all frames)
            fps: Frames per second to simulate
            skip_frames: Skip first N frames
        """
        print(f"\n📹 Processing video: {self.video_path}")

        if not Path(self.video_path).exists():
            print(f"❌ Video file not found: {self.video_path}")
            return False

        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            print(f"❌ Could not open video file")
            return False

        # Get video info
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        video_fps = int(cap.get(cv2.CAP_PROP_FPS))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        print(f"   Resolution: {width}x{height}")
        print(f"   Total frames: {total_frames}")
        print(f"   Original FPS: {video_fps}")
        print(f"   Sending at: {fps} FPS")
        if max_frames:
            print(f"   Max frames to send: {max_frames}")
        if skip_frames:
            print(f"   Skipping first {skip_frames} frames")

        frame_delay = 1.0 / fps  # Delay between frames

        print(f"\n🚀 Starting to send frames...\n")

        frame_number = 0
        start_time = datetime.utcnow()

        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                frame_number += 1

                # Skip initial frames if requested
                if frame_number <= skip_frames:
                    continue

                # Stop if max frames reached
                if max_frames and (frame_number - skip_frames) > max_frames:
                    break

                # Send frame
                timestamp = datetime.utcnow().isoformat() + "Z"
                await self.send_frame(frame, frame_number, timestamp)

                # Progress update every 30 frames
                if frame_number % 30 == 0:
                    elapsed = (datetime.utcnow() - start_time).total_seconds()
                    actual_fps = frame_number / elapsed if elapsed > 0 else 0
                    print(f"📊 Frame {frame_number}/{total_frames} | "
                          f"Sent: {self.frames_sent} | "
                          f"FPS: {actual_fps:.1f}")

                # Simulate real-time streaming
                await asyncio.sleep(frame_delay)

        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        finally:
            cap.release()

        elapsed_total = (datetime.utcnow() - start_time).total_seconds()
        print(f"\n✅ Finished sending frames")
        print(f"   Total frames sent: {self.frames_sent}")
        print(f"   Duration: {elapsed_total:.1f}s")
        print(f"   Average FPS: {self.frames_sent / elapsed_total:.1f}")

        return True

    async def close(self):
        """Close NATS connection."""
        if self.nc:
            await self.nc.close()
            print("\n✅ Disconnected from NATS")


async def main():
    """Main test function."""

    print("=" * 80)
    print("🎥 VIDEO INGRESS/EGRESS TRACKING TEST")
    print("=" * 80)

    # Video path from command line or default
    if len(sys.argv) > 1:
        video_path = sys.argv[1]
    else:
        video_path = r"C:\Users\ishan\Downloads\ingress_outgress_car.mp4"

    print(f"\nVideo: {video_path}\n")

    # Create tester
    tester = VideoTester(video_path, camera_id="video-test-cam-01")

    # Connect to NATS
    if not await tester.connect_nats():
        return

    # Wait a moment for connection to stabilize
    await asyncio.sleep(1)

    # Send line configuration
    # You can customize the line position here:
    # await tester.send_line_config(
    #     line_points=[(200, 400), (800, 420)],  # Custom line
    #     ingress_point=(500, 350)  # Custom ingress side
    # )
    if not await tester.send_line_config():
        return

    # Wait for configuration to be processed
    print("⏳ Waiting 3 seconds for service to process configuration...\n")
    await asyncio.sleep(3)

    # Process video
    # Options:
    # - max_frames: Limit number of frames (None = all)
    # - fps: Frames per second to send (default 30)
    # - skip_frames: Skip first N frames
    await tester.process_video(
        max_frames=None,  # Send all frames
        fps=30,           # 30 FPS
        skip_frames=0     # Don't skip any frames
    )

    # Close connection
    await tester.close()

    print("\n" + "=" * 80)
    print("📊 NEXT STEPS")
    print("=" * 80)
    print("\n1. Check service logs:")
    print("   docker logs -f ingress-egress-tracking-service")
    print("\n2. View camera metrics:")
    print("   curl http://localhost:8100/cameras | jq")
    print("\n3. Monitor Kafka events:")
    print("   docker exec -it kafka kafka-console-consumer \\")
    print("     --bootstrap-server localhost:9092 \\")
    print("     --topic tracking.events.video-test-cam-01 \\")
    print("     --from-beginning")
    print("\n4. Check MinIO storage:")
    print("   Open http://localhost:9001 (user: minioadmin, pass: minioadmin123)")
    print("   Browse bucket: vehicle-tracking")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
