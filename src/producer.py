"""Kafka producer for ingress/egress events and annotated frames."""

import json
from typing import Optional, List, Dict, Any
from aiokafka import AIOKafkaProducer
from datetime import datetime

from src.config import settings
from src.models import CrossingEvent, Detection
from src.logger import get_logger

logger = get_logger(__name__)


class EventProducer:
    """Produces crossing events and annotated frames to Kafka."""

    def __init__(self):
        """Initialize producer."""
        self.producer: Optional[AIOKafkaProducer] = None

    async def connect(self):
        """Connect to Kafka."""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_brokers,
                compression_type="gzip",
                linger_ms=settings.kafka_linger_ms,
                max_request_size=5000000,
                max_batch_size=16384,
            )

            await self.producer.start()

            logger.info("Connected to Kafka", brokers=settings.kafka_brokers)
            return True

        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            raise

    async def stop(self):
        """Stop producer."""
        logger.info("Stopping Kafka producer...")

        if self.producer:
            await self.producer.stop()

        logger.info("Kafka producer stopped")

    def is_connected(self) -> bool:
        """Check if connected to Kafka."""
        return self.producer is not None

    async def send_crossing_event(self, event: CrossingEvent):
        """Send crossing event to Kafka.

        Args:
            event: Crossing event to send
        """
        if not self.producer:
            logger.error("Producer not started")
            return

        topic = f"{settings.kafka_topic_prefix}.{event.camera_id}"

        try:
            message = {
                "eventType": "line_crossing",
                "trackId": event.track_id,
                "direction": event.direction.value,
                "directionValue": event.direction_value,
                "timestamp": event.timestamp,
                "processingTime": datetime.now().isoformat(),
                "cameraId": event.camera_id,
                "cameraName": event.camera_name,
                "areaId": event.area_id,
                "confidence": event.confidence,
                "bbox": {
                    "x": event.bbox.x,
                    "y": event.bbox.y,
                    "width": event.bbox.width,
                    "height": event.bbox.height,
                },
                "crossingPoint": {
                    "x": event.crossing_point.x,
                    "y": event.crossing_point.y,
                },
                "frameNumber": event.frame_number,
                "imagePath": event.image_path,
            }

            message_bytes = json.dumps(message).encode("utf-8")
            await self.producer.send(topic, message_bytes)

            logger.info(
                "Sent crossing event",
                topic=topic,
                track_id=event.track_id,
                direction=event.direction.value,
                direction_value=event.direction_value,
                camera_id=event.camera_id,
            )

        except Exception as e:
            logger.error("Failed to send crossing event", error=str(e))

    async def send_annotated_frame(
        self,
        camera_id: str,
        model_id: str,
        frame_number: int,
        timestamp: str,
        annotated_frame: str,
        detections: List[Detection],
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Send annotated frame to Kafka for HLS encoding.

        Args:
            camera_id: Camera identifier
            model_id: Model identifier
            frame_number: Frame number
            timestamp: Frame timestamp
            annotated_frame: Base64 encoded annotated frame
            detections: List of detections
            metadata: Optional metadata
        """
        if not self.producer:
            logger.error("Producer not started")
            return

        topic = f"annotated.{camera_id}.{model_id}"

        try:
            # Convert detections to serializable format
            detections_data = []
            for detection in detections:
                detections_data.append(
                    {
                        "class": detection.class_name,
                        "trackId": detection.track_id,
                        "confidence": detection.confidence,
                        "bbox": {
                            "x": detection.bbox.x,
                            "y": detection.bbox.y,
                            "width": detection.bbox.width,
                            "height": detection.bbox.height,
                        },
                        "centroid": {
                            "x": detection.centroid_x,
                            "y": detection.centroid_y,
                        }
                        if detection.centroid_x is not None
                        else None,
                    }
                )

            message = {
                "cameraId": camera_id,
                "modelId": model_id,
                "frameNumber": frame_number,
                "timestamp": timestamp,
                "processingTime": datetime.now().isoformat(),
                "image": annotated_frame,
                "detections": detections_data,
                "detectionCount": len(detections),
                "metadata": metadata or {},
            }

            message_bytes = json.dumps(message).encode("utf-8")
            await self.producer.send(topic, message_bytes)

            logger.debug(
                "Sent annotated frame",
                topic=topic,
                camera_id=camera_id,
                frame_number=frame_number,
            )

        except Exception as e:
            logger.error("Failed to send annotated frame", error=str(e))
