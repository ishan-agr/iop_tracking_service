"""Vehicle tracking pipeline with ingress/egress detection."""

import cv2
import numpy as np
import base64
import time
from typing import List, Tuple, Optional, Dict
from pathlib import Path
from boxmot import BotSort
from ultralytics import YOLO
from uuid import uuid4
from datetime import datetime

from src.models import (
    Detection,
    BoundingBox,
    CrossingEvent,
    CrossingDirection,
    IngressEgressConfig,
    Point,
)
from src.line_crossing import LineCrossingDetector
from src.minio_client import MinioClient
from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)


class VehicleTrackingPipeline:
    """Vehicle detection, tracking, and ingress/egress detection pipeline."""

    def __init__(self):
        """Initialize tracking pipeline."""
        self.device = settings.model_device
        self.car_model_confidence = settings.car_model_confidence
        self.car_iou_threshold = settings.car_model_iou_threshold
        self.bucket_name = settings.minio_bucket_name
        self.padding_percent = settings.padding_to_crop
        self.CAR_CLASS_ID = 2

        # Trackers per camera
        self.trackers: Dict[str, BotSort] = {}

        # Line crossing detectors per camera
        self.line_detectors: Dict[str, LineCrossingDetector] = {}

        # Camera metadata (camera_name, area_id)
        self.camera_metadata: Dict[str, Dict[str, str]] = {}

        # Quality thresholds
        self.min_crop_area_ratio = settings.min_crop_area_ratio
        self.blur_threshold = settings.blur_threshold

        # Models (initialized later)
        self.car_model: Optional[YOLO] = None
        self.minio_client: Optional[MinioClient] = None

    async def initialize(self, minio_client: MinioClient):
        """Load the vehicle detection model.

        Args:
            minio_client: Initialized MinIO client
        """
        self.minio_client = minio_client
        try:
            logger.info(
                "Loading vehicle detection model...",
                model=settings.car_model_name,
                device=self.device,
            )
            self.car_model = YOLO(settings.car_model_name).to(self.device)

            # Model warmup
            dummy_img = np.zeros((640, 640, 3), dtype=np.uint8)
            self.car_model(dummy_img, verbose=False)

            logger.info("Vehicle detection model loaded successfully")
            return True
        except Exception as e:
            logger.error("Failed to load vehicle detection model", error=str(e))
            raise

    def configure_line(
        self,
        camera_id: str,
        camera_name: str,
        area_id: str,
        config: IngressEgressConfig,
    ):
        """Configure ingress/egress line for a camera.

        Args:
            camera_id: Camera identifier
            camera_name: Camera name
            area_id: Area identifier
            config: Line configuration
        """
        try:
            self.line_detectors[camera_id] = LineCrossingDetector(
                config=config,
                distance_threshold=settings.crossing_distance_threshold,
                confirmation_frames=settings.crossing_confirmation_frames,
                hysteresis=settings.crossing_hysteresis,
            )

            self.camera_metadata[camera_id] = {
                "camera_name": camera_name,
                "area_id": area_id,
            }

            logger.info(
                "Line configured for camera",
                camera_id=camera_id,
                camera_name=camera_name,
                area_id=area_id,
            )
        except Exception as e:
            logger.error(
                "Failed to configure line",
                camera_id=camera_id,
                error=str(e),
            )
            raise

    def decode_frame(self, frame_data: str) -> Optional[np.ndarray]:
        """Decode frame data to numpy array.

        Args:
            frame_data: Base64 or hex encoded frame

        Returns:
            Decoded frame as numpy array or None if failed
        """
        try:
            if not frame_data:
                logger.error("Empty frame data")
                return None

            frame_bytes = None

            # Try base64 first
            if (
                frame_data.startswith("/9j/")
                or frame_data.startswith("iVBOR")
                or "+" in frame_data
                or "/" in frame_data
            ):
                try:
                    frame_bytes = base64.b64decode(frame_data)
                except Exception as e:
                    logger.error("Failed to decode as base64", error=str(e))
                    return None
            else:
                # Try hex
                try:
                    if len(frame_data) % 2 != 0:
                        logger.error(
                            "Invalid hex data format", hex_length=len(frame_data)
                        )
                        return None
                    frame_bytes = bytes.fromhex(frame_data)
                except ValueError as e:
                    logger.error("Failed to decode as hex", error=str(e))
                    return None

            if frame_bytes is None:
                logger.error("Could not decode frame data")
                return None

            nparr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if frame is None:
                logger.error("Failed to decode frame - cv2.imdecode returned None")
                return None

            if frame.shape[0] == 0 or frame.shape[1] == 0:
                logger.error("Decoded frame has invalid dimensions", shape=frame.shape)
                return None

            return frame

        except Exception as e:
            logger.error("Error decoding frame", error=str(e))
            return None

    async def _is_quality_crop(
        self, cropped_car: np.ndarray, original_frame: np.ndarray
    ) -> bool:
        """Check if crop meets quality standards.

        Args:
            cropped_car: Cropped vehicle image
            original_frame: Original frame

        Returns:
            True if crop passes quality checks
        """
        original_area = original_frame.shape[0] * original_frame.shape[1]
        crop_area = cropped_car.shape[0] * cropped_car.shape[1]

        if (crop_area / original_area) < self.min_crop_area_ratio:
            return False

        # Blur detection using Laplacian variance
        gray_crop = cv2.cvtColor(cropped_car, cv2.COLOR_BGR2GRAY)
        laplacian_var = cv2.Laplacian(gray_crop, cv2.CV_64F).var()

        if laplacian_var < self.blur_threshold:
            return False

        return True

    async def _save_crop_to_minio(
        self, image: np.ndarray, object_name: str
    ) -> Optional[str]:
        """Encode and upload image to MinIO.

        Args:
            image: Image to save
            object_name: Object path in MinIO

        Returns:
            Object path if successful, None otherwise
        """
        try:
            is_success, buffer = cv2.imencode(".jpg", image)
            if not is_success:
                logger.warning(
                    "Failed to encode image for MinIO", object_name=object_name
                )
                return None

            image_bytes = buffer.tobytes()
            await self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=image_bytes,
                length=len(image_bytes),
                content_type="image/jpeg",
            )

            return object_name
        except Exception as e:
            logger.error("Failed to save crop to MinIO", error=str(e))
            return None

    async def detect_and_track(
        self,
        frame: np.ndarray,
        camera_id: str,
        camera_name: str,
        area_id: str,
        frame_number: int,
        timestamp: str,
    ) -> Tuple[List[Detection], List[CrossingEvent], np.ndarray, float]:
        """Detect and track vehicles, detect line crossings.

        Args:
            frame: Input frame
            camera_id: Camera identifier
            camera_name: Camera name
            area_id: Area identifier
            frame_number: Frame number
            timestamp: Frame timestamp

        Returns:
            Tuple of (detections, crossing_events, annotated_frame, processing_time)
        """
        start_time = time.time()
        annotated_frame = frame.copy()
        detections_list = []
        crossing_events = []

        # Production safety: Limit number of cameras
        if len(self.trackers) >= settings.max_cameras and camera_id not in self.trackers:
            logger.error(
                "Max camera limit reached",
                max_cameras=settings.max_cameras,
                current=len(self.trackers),
                rejected_camera=camera_id,
            )
            return [], [], annotated_frame, (time.time() - start_time) * 1000

        try:
            # Run vehicle detection
            results = self.car_model(
                frame,
                conf=self.car_model_confidence,
                iou=self.car_iou_threshold,
                verbose=False,
                classes=[self.CAR_CLASS_ID],
            )

            detections = [box.data.squeeze().tolist() for box in results[0].boxes]
            detections_np = np.array(detections) if detections else np.empty((0, 6))

            # Initialize tracker for this camera if needed
            if camera_id not in self.trackers:
                self.trackers[camera_id] = BotSort(
                    reid_weights=Path("Models/osnet_x0_25_msmt17.pt"),
                    device=0 if "cuda" in self.device else "cpu",
                    half=True,
                    max_age=settings.tracker_max_age,
                    min_hits=settings.tracker_min_hits,
                )

            tracker = self.trackers[camera_id]
            tracks = tracker.update(detections_np, frame)

            if not hasattr(tracks, "size") or tracks.size == 0:
                processing_time = (time.time() - start_time) * 1000
                return [], [], annotated_frame, processing_time

            # Get line detector for this camera
            line_detector = self.line_detectors.get(camera_id)

            # Process each track
            for track in tracks:
                x1, y1, x2, y2, track_id, conf, _, _ = track
                x1, y1, x2, y2, track_id = map(int, [x1, y1, x2, y2, track_id])

                # Calculate centroid
                centroid_x = (x1 + x2) / 2
                centroid_y = (y1 + y2) / 2

                # Create detection
                detection = Detection(
                    class_name="car",
                    track_id=track_id,
                    confidence=float(conf),
                    bbox=BoundingBox(x=x1, y=y1, width=x2 - x1, height=y2 - y1),
                    centroid_x=centroid_x,
                    centroid_y=centroid_y,
                )
                detections_list.append(detection)

                # Check for line crossing
                if line_detector:
                    crossing_result = line_detector.update(
                        track_id, centroid_x, centroid_y
                    )

                    if crossing_result is not None:
                        direction, crossing_point = crossing_result

                        # Save vehicle crop on crossing
                        image_path = None
                        if settings.save_vehicle_crops:
                            bbox_width, bbox_height = x2 - x1, y2 - y1
                            pad_x = int(bbox_width * self.padding_percent)
                            pad_y = int(bbox_height * self.padding_percent)
                            h, w, _ = frame.shape
                            x1_pad = max(0, x1 - pad_x)
                            y1_pad = max(0, y1 - pad_y)
                            x2_pad = min(w, x2 + pad_x)
                            y2_pad = min(h, y2 + pad_y)
                            cropped_car = frame[y1_pad:y2_pad, x1_pad:x2_pad]

                            if await self._is_quality_crop(cropped_car, frame):
                                object_name = f"{camera_id}/{area_id}/{direction.value}/{track_id}/{uuid4()}.jpg"
                                image_path = await self._save_crop_to_minio(
                                    cropped_car, object_name
                                )

                        # Create crossing event
                        crossing_event = CrossingEvent(
                            track_id=track_id,
                            direction=direction,
                            direction_value=1 if direction == CrossingDirection.INGRESS else -1,
                            timestamp=timestamp,
                            camera_id=camera_id,
                            camera_name=camera_name,
                            area_id=area_id,
                            confidence=float(conf),
                            bbox=BoundingBox(x=x1, y=y1, width=x2 - x1, height=y2 - y1),
                            crossing_point=crossing_point,
                            frame_number=frame_number,
                            image_path=image_path,
                        )
                        crossing_events.append(crossing_event)

                # Draw on annotated frame
                color = (0, 255, 0)
                text = f"ID:{track_id}"
                cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), color, 2)
                cv2.circle(
                    annotated_frame,
                    (int(centroid_x), int(centroid_y)),
                    5,
                    (0, 0, 255),
                    -1,
                )
                cv2.putText(
                    annotated_frame,
                    text,
                    (x1, y1 - 10),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    color,
                    2,
                )

            # Draw line on frame
            if line_detector:
                line_p1 = line_detector.line_p1
                line_p2 = line_detector.line_p2
                cv2.line(
                    annotated_frame,
                    (int(line_p1.x), int(line_p1.y)),
                    (int(line_p2.x), int(line_p2.y)),
                    (255, 0, 0),
                    3,
                )

            processing_time = (time.time() - start_time) * 1000
            return detections_list, crossing_events, annotated_frame, processing_time

        except Exception as e:
            logger.error("Vehicle tracking failed", error=str(e))
            processing_time = (time.time() - start_time) * 1000
            return [], [], annotated_frame, processing_time

    async def process_frame(
        self,
        frame_data: str,
        camera_id: str,
        camera_name: str,
        area_id: str,
        frame_number: int,
        timestamp: str,
    ) -> Tuple[List[Detection], List[CrossingEvent], Optional[np.ndarray], float]:
        """Process a frame for vehicle tracking and line crossing detection.

        Args:
            frame_data: Encoded frame data
            camera_id: Camera identifier
            camera_name: Camera name
            area_id: Area identifier
            frame_number: Frame number
            timestamp: Frame timestamp

        Returns:
            Tuple of (detections, crossing_events, annotated_frame, processing_time)
        """
        frame = self.decode_frame(frame_data)
        if frame is None:
            return [], [], None, 0.0

        return await self.detect_and_track(
            frame, camera_id, camera_name, area_id, frame_number, timestamp
        )
