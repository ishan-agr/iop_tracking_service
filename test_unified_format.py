"""Test script for unified NATS message format.

This script:
1. Reads frames from a video file
2. Encodes them as base64 JPEG
3. Sends them to NATS in the unified format (frame + line config)
4. Monitors Kafka for crossing events
"""

import asyncio
import json
import base64
import cv2
from datetime import datetime, timezone
from pathlib import Path
import sys

try:
    import nats
    from aiokafka import AIOKafkaConsumer
except ImportError:
    print("❌ Missing dependencies. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "nats-py", "aiokafka"])
    import nats
    from aiokafka import AIOKafkaConsumer


class UnifiedFormatTester:
    """Test the unified message format."""

    def __init__(
        self,
        video_path: str,
        camera_id: str = "cam-entrance-001",
        camera_name: str = "Main Entrance Gate",
        area_id: str = "parking-lot-a",
        nats_url: str = "nats://localhost:4222",
        kafka_brokers: str = "localhost:9092",
    ):
        self.video_path = video_path
        self.camera_id = camera_id
        self.camera_name = camera_name
        self.area_id = area_id
        self.nats_url = nats_url
        self.kafka_brokers = kafka_brokers

        self.nc = None
        self.kafka_consumer = None
        self.crossing_events = []

    async def connect(self):
        """Connect to NATS and Kafka."""
        print(f"🔌 Connecting to NATS at {self.nats_url}...")
        self.nc = await nats.connect(self.nats_url)
        print("✅ Connected to NATS")

        print(f"🔌 Connecting to Kafka at {self.kafka_brokers}...")
        self.kafka_consumer = AIOKafkaConsumer(
            f"tracking.events.{self.camera_id}",
            bootstrap_servers=self.kafka_brokers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        await self.kafka_consumer.start()
        print("✅ Connected to Kafka")

    async def send_frame(self, frame, frame_number: int, include_line_config: bool = False):
        """Send a single frame to NATS in unified format.

        Args:
            frame: OpenCV frame (numpy array)
            frame_number: Frame number
            include_line_config: Whether to include masking_line configuration
        """
        # Encode frame as JPEG
        ret, buffer = cv2.imencode(".jpg", frame)
        if not ret:
            raise RuntimeError("Failed to encode frame as JPEG")

        # Convert to base64
        jpg_as_text = base64.b64encode(buffer).decode("utf-8")

        # Get frame dimensions
        height, width = frame.shape[:2]

        # Create unified message
        message = {
            "cameraId": self.camera_id,
            "cameraName": self.camera_name,
            "areaId": self.area_id,
            "modelId": "vehicle_ingress_egress",
            "data": jpg_as_text,
            "frameNumber": frame_number,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "requestedFPS": 5,
            "dangerZones": [],
            "intrusion_lines_json": [],
            "metadata": {
                "frameNumber": frame_number,
                "width": width,
                "height": height,
                "sourceFps": 30,
            },
        }

        # Add masking line configuration
        if include_line_config:
            # Horizontal line across the middle of the frame
            y_middle = height // 2
            message["masking_line"] = [
                {
                    "x1": float(width * 0.2),
                    "y1": float(y_middle),
                    "x2": float(width * 0.8),
                    "y2": float(y_middle),
                    "direction": "up",  # Ingress is above the line
                }
            ]
        else:
            message["masking_line"] = []

        # Send to NATS
        subject = "frames.ml.vehicle_ingress_egress"
        await self.nc.publish(subject, json.dumps(message).encode("utf-8"))

        return message

    async def consume_events(self, duration: float = 30.0):
        """Consume events from Kafka.

        Args:
            duration: How long to consume events (seconds)
        """
        print(f"\n📊 Monitoring Kafka topic: tracking.events.{self.camera_id}")
        print(f"   (listening for {duration}s)")
        print()

        start_time = asyncio.get_event_loop().time()

        try:
            async for msg in self.kafka_consumer:
                event = msg.value
                self.crossing_events.append(event)

                # Print event
                direction = event.get("direction", "unknown")
                direction_value = event.get("directionValue", 0)
                track_id = event.get("trackId", 0)

                if direction_value == 1:
                    emoji = "🟢"
                    action = "CHECK-IN"
                else:
                    emoji = "🔴"
                    action = "CHECK-OUT"

                print(
                    f"{emoji} {action}: Track {track_id} | Direction: {direction} | Value: {direction_value:+d}"
                )
                print(
                    f"   Camera: {event.get('cameraId')} | Frame: {event.get('frameNumber')}"
                )
                print(
                    f"   Crossing Point: ({event.get('crossingPoint', {}).get('x', 0):.1f}, {event.get('crossingPoint', {}).get('y', 0):.1f})"
                )
                print()

                # Check if duration exceeded
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed > duration:
                    break

        except asyncio.CancelledError:
            pass

    async def test_video(self, max_frames: int = 100, fps: int = 5):
        """Test by sending video frames to NATS.

        Args:
            max_frames: Maximum number of frames to send
            fps: Frames per second to send
        """
        if not Path(self.video_path).exists():
            raise FileNotFoundError(f"Video not found: {self.video_path}")

        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            raise RuntimeError(f"Could not open video: {self.video_path}")

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        video_fps = int(cap.get(cv2.CAP_PROP_FPS))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        print("\n" + "=" * 80)
        print("🎬 VIDEO INFORMATION")
        print("=" * 80)
        print(f"   Path: {self.video_path}")
        print(f"   Resolution: {width}x{height}")
        print(f"   FPS: {video_fps}")
        print(f"   Total Frames: {total_frames}")
        print(f"   Sending at: {fps} FPS")
        print(f"   Max frames to send: {max_frames}")
        print("=" * 80)

        print("\n" + "=" * 80)
        print("📤 SENDING FRAMES TO NATS")
        print("=" * 80)

        frame_delay = 1.0 / fps
        frame_number = 0
        frames_sent = 0

        # Start Kafka consumer in background
        consumer_task = asyncio.create_task(self.consume_events(duration=60.0))

        try:
            while frames_sent < max_frames:
                ret, frame = cap.read()
                if not ret:
                    print("   📹 End of video reached")
                    break

                frame_number += 1

                # Send frame with line config on first frame
                include_line = frames_sent == 0
                message = await self.send_frame(frame, frame_number, include_line)

                if include_line:
                    line_info = message["masking_line"][0]
                    print(
                        f"\n✅ Frame {frame_number}: Sent with line configuration"
                    )
                    print(
                        f"   Line: ({line_info['x1']:.0f}, {line_info['y1']:.0f}) → ({line_info['x2']:.0f}, {line_info['y2']:.0f})"
                    )
                    print(f"   Direction: {line_info['direction']}")
                    print()
                else:
                    if frames_sent % 10 == 0:
                        print(f"   Frame {frame_number} sent")

                frames_sent += 1
                await asyncio.sleep(frame_delay)

        finally:
            cap.release()

        print(f"\n✅ Sent {frames_sent} frames to NATS")
        print()

        # Wait a bit more for final events
        print("⏳ Waiting for final events (10 seconds)...")
        await asyncio.sleep(10)

        # Stop consumer
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    async def print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 80)
        print("📊 TEST SUMMARY")
        print("=" * 80)

        if not self.crossing_events:
            print("   ⚠️  No crossing events detected")
            print()
            print("   Possible reasons:")
            print("   - No vehicles crossed the line")
            print("   - Line position needs adjustment")
            print("   - Service not running in unified mode")
            print("   - Check service logs for errors")
            return

        # Count events
        check_ins = sum(1 for e in self.crossing_events if e.get("directionValue") == 1)
        check_outs = sum(
            1 for e in self.crossing_events if e.get("directionValue") == -1
        )
        unique_tracks = len(set(e.get("trackId") for e in self.crossing_events))

        print(f"\n   Total Events: {len(self.crossing_events)}")
        print(f"   Check-Ins (Ingress): {check_ins}")
        print(f"   Check-Outs (Egress): {check_outs}")
        print(f"   Net Count: {check_ins - check_outs:+d}")
        print(f"   Unique Vehicles: {unique_tracks}")

        print("\n   📋 Event List:")
        print(f"      {'Frame':<8} {'Track':<8} {'Direction':<10} {'Value':<6}")
        print(f"      {'-'*8} {'-'*8} {'-'*10} {'-'*6}")
        for event in self.crossing_events[:20]:  # Show first 20
            frame = event.get("frameNumber", 0)
            track = event.get("trackId", 0)
            direction = event.get("direction", "unknown")
            value = event.get("directionValue", 0)
            print(f"      {frame:<8} {track:<8} {direction:<10} {value:+d}")

        if len(self.crossing_events) > 20:
            print(f"      ... and {len(self.crossing_events) - 20} more events")

        print()
        print("=" * 80)

    async def close(self):
        """Close connections."""
        if self.nc:
            await self.nc.close()
        if self.kafka_consumer:
            await self.kafka_consumer.stop()


async def main():
    """Main entry point."""
    print("=" * 80)
    print("🧪 UNIFIED MESSAGE FORMAT TEST")
    print("=" * 80)
    print()
    print("This script tests the new unified NATS message format where")
    print("frame data and line configuration are sent in a single message.")
    print()

    # Configuration
    video_path = r"C:\Users\ishan\Downloads\ingress_outgress_car.mp4"
    camera_id = "cam-entrance-001"
    camera_name = "Main Entrance Gate"
    area_id = "parking-lot-a"

    # Allow command line override
    if len(sys.argv) > 1:
        video_path = sys.argv[1]
    if len(sys.argv) > 2:
        camera_id = sys.argv[2]

    print(f"📹 Video: {video_path}")
    print(f"📷 Camera: {camera_id} ({camera_name})")
    print(f"📍 Area: {area_id}")
    print()

    # Check video exists
    if not Path(video_path).exists():
        print(f"❌ Error: Video not found: {video_path}")
        print()
        print("Usage:")
        print(f"  python {Path(__file__).name} [video_path] [camera_id]")
        print()
        print("Example:")
        print(f"  python {Path(__file__).name} test.mp4 cam-01")
        return 1

    # Create tester
    tester = UnifiedFormatTester(
        video_path=video_path,
        camera_id=camera_id,
        camera_name=camera_name,
        area_id=area_id,
        nats_url="nats://localhost:4222",
        kafka_brokers="localhost:9092",
    )

    try:
        # Connect
        await tester.connect()

        # Test with video
        await tester.test_video(max_frames=100, fps=5)

        # Print summary
        await tester.print_summary()

    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        await tester.close()

    print("\n✅ Test complete!")
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted")
        sys.exit(1)
