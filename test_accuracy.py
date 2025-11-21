"""Standalone accuracy testing for ingress/egress tracking.

This script:
1. Processes video directly (no NATS/Kafka needed)
2. Saves annotated video with detections and line crossings
3. Calculates total check-ins and check-outs
4. Displays detailed statistics for accuracy measurement
"""

import asyncio
import cv2
import numpy as np
from pathlib import Path
from datetime import datetime
import sys
import time

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.tracker import VehicleTrackingPipeline
from src.line_crossing import LineCrossingDetector
from src.models import IngressEgressConfig, LineConfig, Point, CrossingDirection
from src.minio_client import MinioClient
from src.config import settings


class AccuracyTester:
    """Test tracking accuracy with direct video processing."""

    def __init__(self, video_path: str, output_path: str = None):
        self.video_path = video_path
        self.output_path = output_path or video_path.replace('.mp4', '_annotated.mp4')

        # Statistics
        self.total_ingress = 0
        self.total_egress = 0
        self.crossing_events = []
        self.track_ids_seen = set()
        self.frame_count = 0

        # Components
        self.tracker = None
        self.line_detector = None
        self.minio_client = None

    def configure_line(self, line_points=None, ingress_point=None, frame_shape=None):
        """Configure the ingress/egress line.

        Args:
            line_points: [(x1, y1), (x2, y2)] or None for horizontal middle line
            ingress_point: (x, y) indicating ingress side or None for auto
            frame_shape: (height, width) of video frames
        """
        if frame_shape is None:
            # Read first frame to get dimensions
            cap = cv2.VideoCapture(self.video_path)
            ret, frame = cap.read()
            cap.release()
            if not ret:
                raise ValueError("Could not read video file")
            frame_shape = frame.shape[:2]

        height, width = frame_shape

        # Default: horizontal line across middle
        if line_points is None:
            y_middle = height // 2
            line_points = [
                (int(width * 0.2), y_middle),
                (int(width * 0.8), y_middle)
            ]

        # Default: ingress from top
        if ingress_point is None:
            x_middle = width // 2
            ingress_point = (x_middle, line_points[0][1] - 50)

        # Create configuration
        config = IngressEgressConfig(
            line=LineConfig(
                points=[
                    Point(x=float(line_points[0][0]), y=float(line_points[0][1])),
                    Point(x=float(line_points[1][0]), y=float(line_points[1][1]))
                ]
            ),
            ingress_side_point=LineConfig(
                points=[Point(x=float(ingress_point[0]), y=float(ingress_point[1]))]
            )
        )

        # Create line detector
        self.line_detector = LineCrossingDetector(
            config=config,
            distance_threshold=settings.crossing_distance_threshold,
            confirmation_frames=settings.crossing_confirmation_frames,
            hysteresis=settings.crossing_hysteresis,
        )

        print(f"✅ Line configured:")
        print(f"   Video resolution: {width}x{height}")
        print(f"   Line: ({line_points[0][0]}, {line_points[0][1]}) → ({line_points[1][0]}, {line_points[1][1]})")
        print(f"   Ingress side: ({ingress_point[0]}, {ingress_point[1]})")
        print(f"   Direction: Vehicles moving from {'TOP' if ingress_point[1] < line_points[0][1] else 'BOTTOM'} = Ingress (+1)")
        print()

    async def initialize(self):
        """Initialize the tracking pipeline."""
        print("🔧 Initializing tracking pipeline...")

        # Create minimal MinIO client (won't actually save files)
        self.minio_client = MinioClient(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
        )

        # Initialize tracker
        self.tracker = VehicleTrackingPipeline()
        await self.tracker.initialize(self.minio_client)

        print("✅ Tracking pipeline initialized")

    def draw_line_on_frame(self, frame):
        """Draw the ingress/egress line on frame."""
        if self.line_detector:
            p1 = self.line_detector.line_p1
            p2 = self.line_detector.line_p2

            # Draw line
            cv2.line(
                frame,
                (int(p1.x), int(p1.y)),
                (int(p2.x), int(p2.y)),
                (0, 255, 255),  # Yellow line
                3
            )

            # Draw direction arrow
            ingress_p = self.line_detector.ingress_point
            cv2.circle(frame, (int(ingress_p.x), int(ingress_p.y)), 10, (0, 255, 0), -1)
            cv2.putText(
                frame,
                "INGRESS",
                (int(ingress_p.x) - 40, int(ingress_p.y) - 15),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (0, 255, 0),
                2
            )

    def draw_statistics(self, frame):
        """Draw real-time statistics on frame."""
        h, w = frame.shape[:2]

        # Semi-transparent overlay
        overlay = frame.copy()
        cv2.rectangle(overlay, (10, 10), (350, 150), (0, 0, 0), -1)
        cv2.addWeighted(overlay, 0.6, frame, 0.4, 0, frame)

        # Statistics text
        stats = [
            f"Frame: {self.frame_count}",
            f"Tracks Seen: {len(self.track_ids_seen)}",
            f"Check-Ins: {self.total_ingress}",
            f"Check-Outs: {self.total_egress}",
            f"Net Count: {self.total_ingress - self.total_egress}"
        ]

        y_offset = 35
        for stat in stats:
            cv2.putText(
                frame,
                stat,
                (20, y_offset),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.6,
                (255, 255, 255),
                2
            )
            y_offset += 25

    async def process_video(self):
        """Process the entire video and generate annotated output."""
        print(f"📹 Processing video: {self.video_path}")

        if not Path(self.video_path).exists():
            print(f"❌ Video file not found: {self.video_path}")
            return False

        # Open video
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            print("❌ Could not open video file")
            return False

        # Get video properties
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        print(f"   Resolution: {width}x{height}")
        print(f"   FPS: {fps}")
        print(f"   Total frames: {total_frames}")
        print(f"   Output: {self.output_path}")
        print()

        # Configure line if not already done
        if self.line_detector is None:
            self.configure_line(frame_shape=(height, width))

        # Setup video writer
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(self.output_path, fourcc, fps, (width, height))

        print("🚀 Processing frames...")
        start_time = time.time()

        camera_id = "accuracy-test"
        camera_name = "Accuracy Test Camera"
        area_id = "test-area"

        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                self.frame_count += 1
                timestamp = datetime.utcnow().isoformat() + "Z"

                # Process frame (detect and track)
                detections, crossing_events, annotated_frame, proc_time = \
                    await self.tracker.detect_and_track(
                        frame=frame,
                        camera_id=camera_id,
                        camera_name=camera_name,
                        area_id=area_id,
                        frame_number=self.frame_count,
                        timestamp=timestamp
                    )

                # Check for line crossings
                for detection in detections:
                    if detection.centroid_x is None or detection.centroid_y is None:
                        # Calculate centroid if not provided
                        centroid_x = detection.bbox.x + detection.bbox.width / 2
                        centroid_y = detection.bbox.y + detection.bbox.height / 2
                    else:
                        centroid_x = detection.centroid_x
                        centroid_y = detection.centroid_y

                    # Track this ID
                    self.track_ids_seen.add(detection.track_id)

                    # Update line detector
                    if self.line_detector:
                        crossing_result = self.line_detector.update(
                            detection.track_id,
                            centroid_x,
                            centroid_y
                        )

                        if crossing_result is not None:
                            direction, crossing_point = crossing_result

                            # Record event
                            event = {
                                'frame': self.frame_count,
                                'track_id': detection.track_id,
                                'direction': direction.value,
                                'direction_value': 1 if direction == CrossingDirection.INGRESS else -1,
                                'crossing_point': (crossing_point.x, crossing_point.y),
                                'timestamp': timestamp
                            }
                            self.crossing_events.append(event)

                            # Update counters
                            if direction == CrossingDirection.INGRESS:
                                self.total_ingress += 1
                                color = (0, 255, 0)  # Green
                                label = f"CHECK-IN #{self.total_ingress}"
                            else:
                                self.total_egress += 1
                                color = (0, 0, 255)  # Red
                                label = f"CHECK-OUT #{self.total_egress}"

                            # Draw crossing event on frame
                            cv2.circle(
                                annotated_frame,
                                (int(crossing_point.x), int(crossing_point.y)),
                                15,
                                color,
                                -1
                            )
                            cv2.putText(
                                annotated_frame,
                                label,
                                (int(crossing_point.x) - 50, int(crossing_point.y) - 20),
                                cv2.FONT_HERSHEY_SIMPLEX,
                                0.7,
                                color,
                                2
                            )

                # Draw line on frame
                self.draw_line_on_frame(annotated_frame)

                # Draw statistics
                self.draw_statistics(annotated_frame)

                # Write annotated frame
                out.write(annotated_frame)

                # Progress update
                if self.frame_count % 30 == 0:
                    elapsed = time.time() - start_time
                    progress = (self.frame_count / total_frames) * 100
                    eta = (elapsed / self.frame_count) * (total_frames - self.frame_count)
                    print(f"   Frame {self.frame_count}/{total_frames} ({progress:.1f}%) | "
                          f"Check-in: {self.total_ingress} | Check-out: {self.total_egress} | "
                          f"ETA: {eta:.0f}s")

        except KeyboardInterrupt:
            print("\n⚠️  Processing interrupted by user")

        finally:
            cap.release()
            out.release()

        elapsed = time.time() - start_time
        print(f"\n✅ Processing complete!")
        print(f"   Time: {elapsed:.1f}s")
        print(f"   Avg FPS: {self.frame_count / elapsed:.1f}")
        print(f"   Output saved: {self.output_path}")

        return True

    def print_results(self):
        """Print detailed accuracy results."""
        print("\n" + "=" * 80)
        print("📊 ACCURACY TEST RESULTS")
        print("=" * 80)

        print(f"\n📹 Video: {Path(self.video_path).name}")
        print(f"   Total Frames: {self.frame_count}")
        print(f"   Annotated Output: {Path(self.output_path).name}")

        print(f"\n🚗 Tracking Statistics:")
        print(f"   Unique Vehicles Tracked: {len(self.track_ids_seen)}")
        print(f"   Track IDs: {sorted(self.track_ids_seen)}")

        print(f"\n✅ Line Crossing Events:")
        print(f"   Total Check-Ins (Ingress):  {self.total_ingress}")
        print(f"   Total Check-Outs (Egress):  {self.total_egress}")
        print(f"   Net Count: {self.total_ingress - self.total_egress:+d}")
        print(f"   Total Crossings: {len(self.crossing_events)}")

        if self.crossing_events:
            print(f"\n📋 Detailed Crossing Events:")
            print(f"   {'Frame':<8} {'Track ID':<10} {'Direction':<10} {'Value':<6}")
            print(f"   {'-'*8} {'-'*10} {'-'*10} {'-'*6}")
            for event in self.crossing_events:
                print(f"   {event['frame']:<8} "
                      f"{event['track_id']:<10} "
                      f"{event['direction']:<10} "
                      f"{event['direction_value']:+d}")

        print("\n" + "=" * 80)

        # Accuracy metrics
        print(f"\n🎯 Accuracy Metrics:")
        if len(self.track_ids_seen) > 0:
            crossing_rate = (len(self.crossing_events) / len(self.track_ids_seen)) * 100
            print(f"   Crossing Detection Rate: {crossing_rate:.1f}% "
                  f"({len(self.crossing_events)}/{len(self.track_ids_seen)} vehicles crossed line)")

        # Check for issues
        print(f"\n⚠️  Potential Issues:")
        issues_found = False

        if len(self.crossing_events) == 0:
            print(f"   ❌ No crossing events detected!")
            print(f"      - Check if vehicles actually cross the configured line")
            print(f"      - Adjust line position to match vehicle movement")
            issues_found = True

        if len(self.track_ids_seen) > 0 and len(self.crossing_events) == 0:
            print(f"   ⚠️  Vehicles tracked but no crossings detected")
            print(f"      - Line may be positioned incorrectly")
            issues_found = True

        if abs(self.total_ingress - self.total_egress) > len(self.track_ids_seen) * 0.5:
            print(f"   ⚠️  Imbalanced crossings (in: {self.total_ingress}, out: {self.total_egress})")
            print(f"      - Check if line direction is configured correctly")
            issues_found = True

        if not issues_found:
            print(f"   ✅ No issues detected")

        print("\n" + "=" * 80)
        print(f"\n🎥 Next Steps:")
        print(f"   1. Review annotated video: {self.output_path}")
        print(f"   2. Verify line position matches vehicle movement")
        print(f"   3. Check that crossing events align with actual crossings")
        print(f"   4. Adjust line position if needed and re-run test")
        print("\n" + "=" * 80 + "\n")


async def main():
    """Main entry point."""
    print("=" * 80)
    print("🎯 INGRESS/EGRESS ACCURACY TEST")
    print("=" * 80)
    print("\nThis test processes video directly (no NATS/Kafka needed)")
    print("Output: Annotated video + detailed statistics")
    print("\n" + "=" * 80 + "\n")

    # Get video path
    if len(sys.argv) > 1:
        video_path = sys.argv[1]
    else:
        video_path = r"C:\Users\ishan\Downloads\ingress_outgress_car.mp4"

    # Get output path
    if len(sys.argv) > 2:
        output_path = sys.argv[2]
    else:
        output_path = video_path.replace('.mp4', '_annotated.mp4')

    print(f"Input:  {video_path}")
    print(f"Output: {output_path}\n")

    # Create tester
    tester = AccuracyTester(video_path, output_path)

    # Initialize
    await tester.initialize()

    # Optional: Configure custom line position
    # Uncomment and adjust if default line doesn't work:
    # tester.configure_line(
    #     line_points=[(200, 400), (800, 420)],  # Custom line
    #     ingress_point=(500, 350)  # Custom ingress side
    # )

    # Process video
    success = await tester.process_video()

    if success:
        # Print results
        tester.print_results()
    else:
        print("❌ Video processing failed")
        return 1

    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
