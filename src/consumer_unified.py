"""NATS consumer for unified frame + line configuration messages."""

import json
import asyncio
from typing import Optional, Callable
import nats

from src.config import settings
from src.models import FrameMessage
from src.logger import get_logger

logger = get_logger(__name__)


class UnifiedConsumer:
    """Consumes unified messages (frame + line config) from NATS.

    This consumer handles the new message format where both frame data
    and line configuration come in a single NATS message.
    """

    def __init__(
        self,
        frame_callback: Callable,
        line_config_callback: Callable,
    ):
        """Initialize consumer.

        Args:
            frame_callback: Async function to process frames
            line_config_callback: Async function to process line configurations
        """
        self.nc: Optional[nats.NATS] = None
        self.subscription = None
        self.frame_callback = frame_callback
        self.line_config_callback = line_config_callback
        self.is_running = False
        self._shutdown_event = asyncio.Event()

        # Track line configurations per camera
        self._configured_cameras = set()

    async def connect(self):
        """Connect to NATS."""
        try:
            self.nc = await nats.connect(
                servers=[settings.nats_url],
                max_reconnect_attempts=5,
                reconnect_time_wait=2,
            )

            logger.info("Connected to NATS", url=settings.nats_url)
            return True

        except Exception as e:
            logger.error("Failed to connect to NATS", error=str(e))
            raise

    async def start_consuming(self):
        """Start consuming unified messages from NATS."""
        if not self.nc or not self.nc.is_connected:
            raise RuntimeError("Not connected to NATS")

        try:
            # Subscribe to unified frame + config messages
            self.subscription = await self.nc.subscribe(
                subject=settings.nats_frame_subject,
                queue=settings.nats_queue_group,
                cb=self._message_handler,
                pending_msgs_limit=settings.nats_max_pending,
            )

            logger.info(
                "Started consuming unified messages",
                subject=settings.nats_frame_subject,
                queue=settings.nats_queue_group,
            )

            self.is_running = True

            # Wait until shutdown
            await self._shutdown_event.wait()

        except Exception as e:
            logger.error("Error in consumer", error=str(e))
            raise

    async def _message_handler(self, msg):
        """Handle incoming unified NATS message.

        Args:
            msg: NATS message containing both frame data and line config
        """
        try:
            # Parse message
            data = json.loads(msg.data.decode())
            frame_msg = FrameMessage(**data)

            # Check if this message contains line configuration
            line_config = frame_msg.get_line_config()

            if line_config is not None:
                # Check if we've already configured this camera
                camera_key = f"{frame_msg.camera_id}"

                if camera_key not in self._configured_cameras:
                    # First time seeing this camera with line config
                    logger.info(
                        "Configuring line for camera",
                        camera_id=frame_msg.camera_id,
                        camera_name=frame_msg.camera_name,
                        area_id=frame_msg.area_id,
                        line=f"({line_config.line.points[0].x},{line_config.line.points[0].y}) -> ({line_config.line.points[1].x},{line_config.line.points[1].y})",
                        ingress_side=f"({line_config.ingress_side_point.points[0].x},{line_config.ingress_side_point.points[0].y})"
                    )

                    # Create a line config message for the callback
                    from src.models import LineConfigMessage, LineConfig

                    line_config_msg = LineConfigMessage(
                        cameraId=frame_msg.camera_id,
                        cameraName=frame_msg.camera_name or "Unknown",
                        areaId=frame_msg.area_id or "Unknown",
                        config=[line_config.line, line_config.ingress_side_point],
                        timestamp=frame_msg.timestamp
                    )

                    # Call line config callback
                    await self.line_config_callback(line_config_msg)

                    # Mark camera as configured
                    self._configured_cameras.add(camera_key)

            # Always process the frame
            await self.frame_callback(frame_msg)

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in message", error=str(e))
        except Exception as e:
            logger.error(
                "Error processing message", error=str(e), subject=msg.subject
            )
            import traceback
            logger.error("Traceback", trace=traceback.format_exc())

    async def stop(self):
        """Stop consuming and disconnect."""
        logger.info("Stopping consumer...")

        self.is_running = False
        self._shutdown_event.set()

        if self.subscription:
            await self.subscription.unsubscribe()

        if self.nc and self.nc.is_connected:
            await self.nc.drain()
            await self.nc.close()

        logger.info("Consumer stopped")

    def is_connected(self) -> bool:
        """Check if connected to NATS."""
        return self.nc is not None and self.nc.is_connected
