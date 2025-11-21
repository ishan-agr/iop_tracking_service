"""Main service orchestrator for Ingress/Egress Tracking."""

import time
import cv2
import base64
import traceback
import asyncio
from typing import Dict, Any

from src.tracker import VehicleTrackingPipeline
from src.config import settings
from src.models import FrameMessage, LineConfigMessage
from src.consumer import MultiTopicConsumer
from src.producer import EventProducer
from src.minio_client import MinioClient
from src.discord_notifier import DiscordNotifier
from src.logger import get_logger

logger = get_logger(__name__)


class TrackingService:
    """Main Ingress/Egress Tracking Service."""

    def __init__(self):
        """Initialize service components."""
        self.tracker = VehicleTrackingPipeline()
        self.consumer = MultiTopicConsumer(
            frame_callback=self.process_frame,
            line_config_callback=self.process_line_config,
        )
        self.producer = EventProducer()
        self.minio_client: MinioClient = None
        self.discord = DiscordNotifier(
            webhook_url=settings.discord_webhook_url,
            enabled=settings.discord_enabled,
        )

        # Metrics
        self.start_time = time.time()
        self.frames_processed = 0
        self.crossing_events_detected = 0
        self.total_processing_time = 0.0
        self.last_error = None
        self.active_cameras = set()

        # Per-camera metrics for production monitoring
        self.camera_metrics: Dict[str, Dict[str, Any]] = {}  # camera_id -> metrics

        # Health monitoring
        self.last_nats_status = True
        self.last_kafka_status = True
        self.last_model_status = False
        self.health_check_task = None

    async def start(self):
        """Start the tracking service."""
        logger.info("Starting Ingress/Egress Tracking Service...")

        try:
            # Initialize MinIO client
            self.minio_client = MinioClient(
                endpoint=settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
            )
            await self.minio_client.ensure_bucket_exists(settings.minio_bucket_name)

            # Connect to NATS and Kafka
            await self.consumer.connect()
            await self.producer.connect()

            # Initialize tracker with MinIO client
            try:
                await self.tracker.initialize(minio_client=self.minio_client)
                self.last_model_status = True
            except Exception as e:
                self.last_model_status = False
                await self.discord.send_model_load_error(str(e))
                raise

            # Send startup notification
            await self.discord.send_service_started()

            # Start health monitoring
            self.health_check_task = asyncio.create_task(self._health_monitor())

            # Start consuming messages
            logger.info("Service started successfully")
            await self.consumer.start_consuming()

        except Exception as e:
            logger.error("Failed to start service", error=str(e))
            self.last_error = str(e)
            raise

    async def process_line_config(self, config_msg: LineConfigMessage):
        """Process line configuration message.

        Args:
            config_msg: Line configuration message from NATS
        """
        try:
            logger.info(
                "Processing line configuration",
                camera_id=config_msg.camera_id,
                camera_name=config_msg.camera_name,
                area_id=config_msg.area_id,
            )

            # Convert to structured config
            ingress_egress_config = config_msg.to_ingress_egress_config()

            # Configure line for this camera
            self.tracker.configure_line(
                camera_id=config_msg.camera_id,
                camera_name=config_msg.camera_name,
                area_id=config_msg.area_id,
                config=ingress_egress_config,
            )

            self.active_cameras.add(config_msg.camera_id)

            logger.info(
                "Line configuration applied successfully",
                camera_id=config_msg.camera_id,
            )

        except Exception as e:
            error_traceback = traceback.format_exc()
            logger.error(
                "Error processing line configuration",
                error=str(e),
                traceback=error_traceback,
                camera_id=config_msg.camera_id,
            )
            self.last_error = str(e)

    async def process_frame(self, frame_msg: FrameMessage):
        """Process a single frame.

        Args:
            frame_msg: Frame message from NATS
        """
        try:
            logger.debug(
                "Processing frame",
                camera_id=frame_msg.camera_id,
                frame_number=(
                    frame_msg.metadata.frame_number if frame_msg.metadata else None
                ),
            )

            # Get camera metadata
            camera_name = frame_msg.camera_name or "unknown"
            area_id = frame_msg.area_id or "unknown"

            # Check if line is configured for this camera
            if frame_msg.camera_id not in self.tracker.line_detectors:
                logger.warning(
                    "No line configured for camera, skipping frame",
                    camera_id=frame_msg.camera_id,
                )
                return

            # Process frame
            detections, crossing_events, annotated_frame, processing_time = (
                await self.tracker.process_frame(
                    frame_data=frame_msg.data,
                    camera_id=frame_msg.camera_id,
                    camera_name=camera_name,
                    area_id=area_id,
                    frame_number=(
                        frame_msg.metadata.frame_number if frame_msg.metadata else 0
                    ),
                    timestamp=frame_msg.timestamp,
                )
            )

            # Send crossing events to Kafka
            for event in crossing_events:
                await self.producer.send_crossing_event(event)
                self.crossing_events_detected += 1

                logger.info(
                    "Crossing event published",
                    track_id=event.track_id,
                    direction=event.direction.value,
                    direction_value=event.direction_value,
                    camera_id=event.camera_id,
                    area_id=event.area_id,
                )

                # Send Discord notification if enabled
                if settings.discord_notify_on_crossing:
                    await self.discord.send_crossing_event(
                        track_id=event.track_id,
                        direction=event.direction.value,
                        camera_id=event.camera_id,
                        area_id=event.area_id,
                    )

            # Send annotated frame to Kafka
            if annotated_frame is not None:
                success, buffer = cv2.imencode(".jpg", annotated_frame)
                if success:
                    annotated_frame_b64 = base64.b64encode(buffer).decode("utf-8")

                    metadata = {
                        "sourceWidth": (
                            frame_msg.metadata.width if frame_msg.metadata else None
                        ),
                        "sourceHeight": (
                            frame_msg.metadata.height if frame_msg.metadata else None
                        ),
                        "sourceFps": (
                            frame_msg.metadata.source_fps if frame_msg.metadata else None
                        ),
                    }

                    await self.producer.send_annotated_frame(
                        camera_id=frame_msg.camera_id,
                        model_id=frame_msg.model_id or "tracking",
                        frame_number=(
                            frame_msg.metadata.frame_number if frame_msg.metadata else 0
                        ),
                        timestamp=frame_msg.timestamp,
                        annotated_frame=annotated_frame_b64,
                        detections=detections,
                        metadata=metadata,
                    )

            # Update metrics
            self.frames_processed += 1
            self.total_processing_time += processing_time

            # Update per-camera metrics
            if settings.enable_per_camera_metrics:
                if frame_msg.camera_id not in self.camera_metrics:
                    self.camera_metrics[frame_msg.camera_id] = {
                        "frames_processed": 0,
                        "crossing_events": 0,
                        "total_processing_time": 0.0,
                        "last_seen": time.time(),
                    }

                cam_metrics = self.camera_metrics[frame_msg.camera_id]
                cam_metrics["frames_processed"] += 1
                cam_metrics["crossing_events"] += len(crossing_events)
                cam_metrics["total_processing_time"] += processing_time
                cam_metrics["last_seen"] = time.time()

            # Log stats every 100 frames
            if self.frames_processed % 100 == 0:
                avg_time = self.total_processing_time / self.frames_processed
                logger.info(
                    "Processing stats",
                    frames_processed=self.frames_processed,
                    crossing_events=self.crossing_events_detected,
                    avg_processing_time=f"{avg_time:.2f}ms",
                    active_cameras=len(self.active_cameras),
                    camera_breakdown={
                        cam_id: metrics["frames_processed"]
                        for cam_id, metrics in self.camera_metrics.items()
                    }
                    if settings.enable_per_camera_metrics
                    else {},
                )

        except Exception as e:
            error_traceback = traceback.format_exc()
            logger.error(
                "Error processing frame",
                error=str(e),
                traceback=error_traceback,
                camera_id=frame_msg.camera_id,
            )
            self.last_error = str(e)

    async def _health_monitor(self):
        """Periodically monitor service health and send alerts."""
        logger.info("Health monitoring started")

        while True:
            try:
                await asyncio.sleep(settings.health_check_interval)

                # Check NATS connection
                nats_connected = self.consumer.is_connected()
                if nats_connected != self.last_nats_status:
                    if nats_connected:
                        await self.discord.send_connection_restored("NATS")
                        logger.info("NATS connection restored")
                    else:
                        await self.discord.send_connection_lost("NATS")
                        logger.error("NATS connection lost")
                    self.last_nats_status = nats_connected

                # Check Kafka connection
                kafka_connected = self.producer.is_connected()
                if kafka_connected != self.last_kafka_status:
                    if kafka_connected:
                        await self.discord.send_connection_restored("Kafka")
                        logger.info("Kafka connection restored")
                    else:
                        await self.discord.send_connection_lost("Kafka")
                        logger.error("Kafka connection lost")
                    self.last_kafka_status = kafka_connected

                # Check model status
                model_loaded = self.tracker.car_model is not None
                if model_loaded != self.last_model_status:
                    if not model_loaded:
                        await self.discord.send_model_load_error("Model became unavailable")
                        logger.error("Model no longer loaded")
                    self.last_model_status = model_loaded

            except asyncio.CancelledError:
                logger.info("Health monitoring cancelled")
                break
            except Exception as e:
                logger.error("Error in health monitor", error=str(e))

    async def stop(self):
        """Stop the service gracefully."""
        logger.info("Stopping tracking service...")

        # Cancel health monitoring
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass

        await self.consumer.stop()
        await self.producer.stop()
        await self.discord.send_service_stopped()

        logger.info("Tracking service stopped")

    def get_status(self) -> Dict[str, Any]:
        """Get service status.

        Returns:
            Service status dictionary
        """
        uptime = time.time() - self.start_time
        avg_processing_time = (
            self.total_processing_time / self.frames_processed
            if self.frames_processed > 0
            else 0.0
        )

        status_data = {
            "status": "healthy" if self.last_error is None else "degraded",
            "uptime": uptime,
            "frames_processed": self.frames_processed,
            "crossing_events_detected": self.crossing_events_detected,
            "avg_processing_time": avg_processing_time,
            "active_cameras": len(self.active_cameras),
            "active_connections": {
                "nats": self.consumer.is_connected(),
                "kafka": self.producer.is_connected(),
            },
            "model_loaded": self.tracker.car_model is not None,
            "last_error": self.last_error,
        }
        return status_data
