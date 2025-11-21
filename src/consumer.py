"""NATS consumer for frames and line configurations."""

import json
import asyncio
from typing import Optional, Callable
import nats

from src.config import settings
from src.models import FrameMessage, LineConfigMessage
from src.logger import get_logger

logger = get_logger(__name__)


class MultiTopicConsumer:
    """Consumes frames and line configurations from NATS."""

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
        self.frame_subscription = None
        self.line_subscription = None
        self.frame_callback = frame_callback
        self.line_config_callback = line_config_callback
        self.is_running = False
        self._shutdown_event = asyncio.Event()

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
        """Start consuming frames and line configurations from NATS."""
        if not self.nc or not self.nc.is_connected:
            raise RuntimeError("Not connected to NATS")

        try:
            # Subscribe to frame messages
            self.frame_subscription = await self.nc.subscribe(
                subject=settings.nats_frame_subject,
                queue=settings.nats_queue_group,
                cb=self._frame_message_handler,
                pending_msgs_limit=settings.nats_max_pending,
            )

            logger.info(
                "Started consuming frames",
                subject=settings.nats_frame_subject,
                queue=settings.nats_queue_group,
            )

            # Subscribe to line configuration messages
            self.line_subscription = await self.nc.subscribe(
                subject=settings.nats_line_config_subject,
                cb=self._line_config_handler,
            )

            logger.info(
                "Started consuming line configurations",
                subject=settings.nats_line_config_subject,
            )

            self.is_running = True

            # Wait until shutdown
            await self._shutdown_event.wait()

        except Exception as e:
            logger.error("Error in consumer", error=str(e))
            raise

    async def _frame_message_handler(self, msg):
        """Handle incoming frame NATS message.

        Args:
            msg: NATS message containing frame data
        """
        try:
            # Parse message
            data = json.loads(msg.data.decode())
            frame_msg = FrameMessage(**data)

            # Process frame
            await self.frame_callback(frame_msg)

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in frame message", error=str(e))
        except Exception as e:
            logger.error(
                "Error processing frame message", error=str(e), subject=msg.subject
            )

    async def _line_config_handler(self, msg):
        """Handle incoming line configuration NATS message.

        Args:
            msg: NATS message containing line configuration
        """
        try:
            # Parse message
            data = json.loads(msg.data.decode())
            line_config_msg = LineConfigMessage(**data)

            # Process line configuration
            await self.line_config_callback(line_config_msg)

            logger.info(
                "Received line configuration",
                camera_id=line_config_msg.camera_id,
                camera_name=line_config_msg.camera_name,
                area_id=line_config_msg.area_id,
            )

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in line config message", error=str(e))
        except Exception as e:
            logger.error(
                "Error processing line config message",
                error=str(e),
                subject=msg.subject,
            )

    async def stop(self):
        """Stop consuming and disconnect."""
        logger.info("Stopping consumer...")

        self.is_running = False
        self._shutdown_event.set()

        if self.frame_subscription:
            await self.frame_subscription.unsubscribe()

        if self.line_subscription:
            await self.line_subscription.unsubscribe()

        if self.nc and self.nc.is_connected:
            await self.nc.drain()
            await self.nc.close()

        logger.info("Consumer stopped")

    def is_connected(self) -> bool:
        """Check if connected to NATS."""
        return self.nc is not None and self.nc.is_connected
