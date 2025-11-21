"""Dual camera accuracy testing for ingress/egress tracking.

This script:
1. Processes TWO videos simultaneously (simulating 2 cameras)
2. Configurable FPS for each camera
3. Saves separate annotated videos for each camera
4. Calculates check-in/check-out statistics per camera
5. Provides combined accuracy metrics
"""

import asyncio
import cv2
import numpy as np
from pathlib import Path
from datetime import datetime
import sys
import time
from typing import Dict, List, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.tracker import VehicleTrackingPipeline
from src.line_crossing import LineCrossingDetector
from src.models import IngressEgressConfig, LineConfig, Point, CrossingDirection
from src.minio_client import MinioClient
from src.config import settings


class CameraProcessor:
    """Process a single camera video."""

    def __init__(self, camera_id: str, video_path: str, output_path: str,
                 target_fps: int = 30, line_config: dict = None):
        self.camera_id = camera_id
        self.camera_name = f"Camera {camera_id}"
        self.area_id = f"area-{camera_id}"
        self.video_path = video_path
        self.output_path = output_path
        self.target_fps = target_fps
        self.custom_line_config = line_config

        # Statistics
        self.total_ingress = 0
        self.total_egress = 0
        self.crossing_events = []
        self.track_ids_seen = set()
        self.frame_count = 0

        # Video properties
        self.cap = None
        self.out = None
        self.video_fps = 30
        self.width = 0
        self.height = 0
        self.total_frames = 0

        # Line detector
        self.line_detector = None

    def open_video(self):
        """Open video file and get properties."""
        if not Path(self.video_path).exists():
            raise FileNotFoundError(f"Video not found: {self.video_path}")

        self.cap = cv2.VideoCapture(self.video_path)
        if not self.cap.isOpened():
            raise RuntimeError(f"Could not open video: {self.video_path}")

        self.video_fps = int(self.cap.get(cv2.CAP_PROP_FPS))
        self.width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        self.height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        self.total_frames = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Setup video writer
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        self.out = cv2.VideoWriter(self.output_path, fourcc, self.target_fps,
                                   (self.width, self.height))

        print(f"📹 {self.camera_name}:")
        print(f"   Input: {Path(self.video_path).name}")
        print(f"   Resolution: {self.width}x{self.height}")
        print(f"   Original FPS: {self.video_fps}")
        print(f"   Target FPS: {self.target_fps}")
        print(f"   Total frames: {self.total_frames}")
        print(f"   Output: {Path(self.output_path).name}")

    def configure_line(self):
        """Configure the ingress/egress line."""
        if self.custom_line_config:
            # Use custom configuration
            line_points = self.custom_line_config.get('line_points')
            ingress_point = self.custom_line_config.get('ingress_point')
        else:
            # Default: horizontal line across middle
            y_middle = self.height // 2
            line_points = [
                (int(self.width * 0.2), y_middle),
                (int(self.width * 0.8), y_middle)
            ]
            x_middle = self.width // 2
            ingress_point = (x_middle, y_middle - 50)

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

        print(f"   Line: ({line_points[0][0]}, {line_points[0][1]}) → "
              f"({line_points[1][0]}, {line_points[1][1]})")
        print(f"   Ingress side: ({ingress_point[0]}, {ingress_point[1]})")

    def read_frame(self) -> Tuple[bool, np.ndarray]:
        """Read next frame from video."""
        return self.cap.read()

    def draw_annotations(self, frame, detections):
        """Draw line, statistics, and detection boxes on frame."""
        annotated = frame.copy()

        # Draw line
        if self.line_detector:
            p1 = self.line_detector.line_p1
            p2 = self.line_detector.line_p2
            cv2.line(annotated, (int(p1.x), int(p1.y)), (int(p2.x), int(p2.y)),
                    (0, 255, 255), 3)

            # Draw ingress indicator
            ingress_p = self.line_detector.ingress_point
            cv2.circle(annotated, (int(ingress_p.x), int(ingress_p.y)), 10, (0, 255, 0), -1)
            cv2.putText(annotated, "INGRESS", (int(ingress_p.x) - 40, int(ingress_p.y) - 15),
                       cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

        # Draw statistics overlay
        overlay = annotated.copy()
        cv2.rectangle(overlay, (10, 10), (380, 180), (0, 0, 0), -1)
        cv2.addWeighted(overlay, 0.6, annotated, 0.4, 0, annotated)

        stats = [
            f"Camera: {self.camera_id}",
            f"Frame: {self.frame_count}/{self.total_frames}",
            f"Tracks: {len(self.track_ids_seen)}",
            f"Check-Ins: {self.total_ingress}",
            f"Check-Outs: {self.total_egress}",
            f"Net: {self.total_ingress - self.total_egress:+d}"
        ]

        y_offset = 35
        for stat in stats:
            cv2.putText(annotated, stat, (20, y_offset), cv2.FONT_HERSHEY_SIMPLEX,
                       0.6, (255, 255, 255), 2)
            y_offset += 25

        return annotated

    def process_detections(self, detections):
        """Check detections for line crossings and update statistics."""
        for detection in detections:
            # Get centroid
            if detection.centroid_x is None or detection.centroid_y is None:
                centroid_x = detection.bbox.x + detection.bbox.width / 2
                centroid_y = detection.bbox.y + detection.bbox.height / 2
            else:
                centroid_x = detection.centroid_x
                centroid_y = detection.centroid_y

            # Track this ID
            self.track_ids_seen.add(detection.track_id)

            # Check for crossing
            if self.line_detector:
                crossing_result = self.line_detector.update(
                    detection.track_id, centroid_x, centroid_y
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
                        'timestamp': datetime.utcnow().isoformat() + "Z"
                    }
                    self.crossing_events.append(event)

                    # Update counters
                    if direction == CrossingDirection.INGRESS:
                        self.total_ingress += 1
                    else:
                        self.total_egress += 1

                    return crossing_point, direction

        return None, None

    def draw_crossing_marker(self, frame, crossing_point, direction):
        """Draw crossing event marker on frame."""
        if direction == CrossingDirection.INGRESS:
            color = (0, 255, 0)  # Green
            label = f"IN #{self.total_ingress}"
        else:
            color = (0, 0, 255)  # Red
            label = f"OUT #{self.total_egress}"

        cv2.circle(frame, (int(crossing_point.x), int(crossing_point.y)), 15, color, -1)
        cv2.putText(frame, label, (int(crossing_point.x) - 50, int(crossing_point.y) - 20),
                   cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)

    def write_frame(self, frame):
        """Write frame to output video."""
        self.out.write(frame)

    def close(self):
        """Release video resources."""
        if self.cap:
            self.cap.release()
        if self.out:
            self.out.release()

    def get_frame_delay(self) -> float:
        """Calculate delay between frames to achieve target FPS."""
        if self.target_fps <= 0:
            return 0.0
        return 1.0 / self.target_fps


class DualCameraAccuracyTester:
    """Test accuracy with two cameras simultaneously."""

    def __init__(self, camera_configs: List[Dict]):
        """
        Initialize with camera configurations.

        Args:
            camera_configs: List of dicts with keys:
                - camera_id: str
                - video_path: str
                - output_path: str (optional)
                - target_fps: int (optional, default 30)
                - line_config: dict (optional)
        """
        self.cameras = []

        for config in camera_configs:
            camera_id = config['camera_id']
            video_path = config['video_path']
            output_path = config.get('output_path',
                                    video_path.replace('.mp4', f'_{camera_id}_annotated.mp4'))
            target_fps = config.get('target_fps', 30)
            line_config = config.get('line_config', None)

            camera = CameraProcessor(camera_id, video_path, output_path,
                                    target_fps, line_config)
            self.cameras.append(camera)

        self.tracker = None
        self.minio_client = None

    async def initialize(self):
        """Initialize tracking pipeline and cameras."""
        print("🔧 Initializing dual camera tracking pipeline...")
        print()

        # Initialize MinIO (minimal, won't actually save)
        self.minio_client = MinioClient(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
        )

        # Initialize tracker
        self.tracker = VehicleTrackingPipeline()
        await self.tracker.initialize(self.minio_client)

        print("✅ Tracking pipeline initialized")
        print()

        # Open all cameras
        for camera in self.cameras:
            camera.open_video()
            camera.configure_line()
            print()

    async def process_videos(self):
        """Process both videos simultaneously."""
        print("🚀 Processing videos simultaneously...")
        print()

        start_time = time.time()

        # Determine which camera has more frames
        max_frames = max(cam.total_frames for cam in self.cameras)

        try:
            for frame_idx in range(max_frames):

                # Process each camera
                for camera in self.cameras:
                    # Skip if this camera is finished
                    if frame_idx >= camera.total_frames:
                        continue

                    # Read frame
                    ret, frame = camera.read_frame()
                    if not ret:
                        continue

                    camera.frame_count += 1
                    timestamp = datetime.utcnow().isoformat() + "Z"

                    # Detect and track
                    detections, crossing_events, annotated_frame, proc_time = \
                        await self.tracker.detect_and_track(
                            frame=frame,
                            camera_id=camera.camera_id,
                            camera_name=camera.camera_name,
                            area_id=camera.area_id,
                            frame_number=camera.frame_count,
                            timestamp=timestamp
                        )

                    # Check for crossings
                    crossing_point, direction = camera.process_detections(detections)

                    # Draw annotations
                    annotated_frame = camera.draw_annotations(annotated_frame, detections)

                    # Draw crossing marker if detected
                    if crossing_point and direction:
                        camera.draw_crossing_marker(annotated_frame, crossing_point, direction)

                    # Write frame
                    camera.write_frame(annotated_frame)

                # Progress update every 30 frames
                if frame_idx % 30 == 0 and frame_idx > 0:
                    elapsed = time.time() - start_time
                    progress = (frame_idx / max_frames) * 100
                    eta = (elapsed / frame_idx) * (max_frames - frame_idx)

                    cam_stats = " | ".join([
                        f"{cam.camera_id}: In={cam.total_ingress} Out={cam.total_egress}"
                        for cam in self.cameras
                    ])

                    print(f"   Frame {frame_idx}/{max_frames} ({progress:.1f}%) | "
                          f"{cam_stats} | ETA: {eta:.0f}s")

        except KeyboardInterrupt:
            print("\n⚠️  Processing interrupted")

        finally:
            # Close all cameras
            for camera in self.cameras:
                camera.close()

        elapsed = time.time() - start_time
        total_frames_processed = sum(cam.frame_count for cam in self.cameras)

        print(f"\n✅ Processing complete!")
        print(f"   Time: {elapsed:.1f}s")
        print(f"   Total frames processed: {total_frames_processed}")
        print(f"   Avg FPS: {total_frames_processed / elapsed:.1f}")

    def print_results(self):
        """Print detailed results for all cameras."""
        print("\n" + "=" * 80)
        print("📊 DUAL CAMERA ACCURACY TEST RESULTS")
        print("=" * 80)

        # Per-camera results
        for camera in self.cameras:
            print(f"\n📹 {camera.camera_name} ({camera.camera_id})")
            print(f"   Video: {Path(camera.video_path).name}")
            print(f"   Output: {Path(camera.output_path).name}")
            print(f"   Frames Processed: {camera.frame_count}")

            print(f"\n   🚗 Tracking:")
            print(f"      Unique Vehicles: {len(camera.track_ids_seen)}")
            print(f"      Track IDs: {sorted(camera.track_ids_seen)}")

            print(f"\n   ✅ Crossings:")
            print(f"      Check-Ins (Ingress):  {camera.total_ingress}")
            print(f"      Check-Outs (Egress):  {camera.total_egress}")
            print(f"      Net Count: {camera.total_ingress - camera.total_egress:+d}")
            print(f"      Total Events: {len(camera.crossing_events)}")

            if camera.crossing_events:
                print(f"\n   📋 Event Details:")
                print(f"      {'Frame':<8} {'Track ID':<10} {'Direction':<10} {'Value':<6}")
                print(f"      {'-'*8} {'-'*10} {'-'*10} {'-'*6}")
                for event in camera.crossing_events[:10]:  # Show first 10
                    print(f"      {event['frame']:<8} "
                          f"{event['track_id']:<10} "
                          f"{event['direction']:<10} "
                          f"{event['direction_value']:+d}")
                if len(camera.crossing_events) > 10:
                    print(f"      ... and {len(camera.crossing_events) - 10} more events")

            if len(camera.track_ids_seen) > 0:
                crossing_rate = (len(camera.crossing_events) / len(camera.track_ids_seen)) * 100
                print(f"\n   🎯 Metrics:")
                print(f"      Detection Rate: {crossing_rate:.1f}%")

        # Combined statistics
        print(f"\n" + "=" * 80)
        print(f"📊 COMBINED STATISTICS")
        print(f"=" * 80)

        total_ingress = sum(cam.total_ingress for cam in self.cameras)
        total_egress = sum(cam.total_egress for cam in self.cameras)
        total_crossings = sum(len(cam.crossing_events) for cam in self.cameras)
        total_tracks = sum(len(cam.track_ids_seen) for cam in self.cameras)

        print(f"\n   Total Check-Ins (All Cameras):  {total_ingress}")
        print(f"   Total Check-Outs (All Cameras): {total_egress}")
        print(f"   Combined Net Count: {total_ingress - total_egress:+d}")
        print(f"   Total Crossings: {total_crossings}")
        print(f"   Total Unique Tracks: {total_tracks}")

        print("\n" + "=" * 80)
        print(f"\n🎥 Review Annotated Videos:")
        for camera in self.cameras:
            print(f"   {camera.camera_id}: {camera.output_path}")

        print("\n" + "=" * 80 + "\n")


async def main():
    """Main entry point."""
    print("=" * 80)
    print("🎯 DUAL CAMERA ACCURACY TEST")
    print("=" * 80)
    print("\nTesting two cameras simultaneously with separate outputs")
    print("Configurable FPS and line positions per camera")
    print("\n" + "=" * 80 + "\n")

    # Configure cameras
    camera_configs = [
        {
            'camera_id': 'cam1',
            'video_path': r"C:\Users\ishan\Downloads\ingress_outgress_car.mp4",
            'target_fps': 30,  # Process at 30 FPS
            # Optional: Custom line configuration
            # 'line_config': {
            #     'line_points': [(200, 400), (800, 420)],
            #     'ingress_point': (500, 350)
            # }
        },
        {
            'camera_id': 'cam2',
            'video_path': r"C:\Users\ishan\Downloads\05.mp4",  # Can be different video
            'target_fps': 15,  # Process at 15 FPS (slower)
            # Optional: Custom line for camera 2
            # 'line_config': {
            #     'line_points': [(100, 500), (900, 520)],
            #     'ingress_point': (500, 450)
            # }
        }
    ]

    # Allow command line override
    if len(sys.argv) > 1:
        camera_configs[0]['video_path'] = sys.argv[1]
    if len(sys.argv) > 2:
        camera_configs[1]['video_path'] = sys.argv[2]
    if len(sys.argv) > 3:
        camera_configs[0]['target_fps'] = int(sys.argv[3])
    if len(sys.argv) > 4:
        camera_configs[1]['target_fps'] = int(sys.argv[4])

    print("📹 Camera Configuration:")
    print()
    for i, config in enumerate(camera_configs, 1):
        print(f"   Camera {i} ({config['camera_id']}):")
        print(f"      Video: {config['video_path']}")
        print(f"      Target FPS: {config['target_fps']}")
    print()

    # Create tester
    tester = DualCameraAccuracyTester(camera_configs)

    # Initialize
    await tester.initialize()

    # Process videos
    await tester.process_videos()

    # Print results
    tester.print_results()

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
