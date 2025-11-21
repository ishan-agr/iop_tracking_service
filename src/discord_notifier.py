"""Discord webhook notifier for service alerts."""

import httpx
import asyncio
from typing import Optional
from datetime import datetime

from src.logger import get_logger

logger = get_logger(__name__)


class DiscordNotifier:
    """Send notifications to Discord via webhook."""

    def __init__(self, webhook_url: Optional[str] = None, enabled: bool = True):
        """Initialize Discord notifier.

        Args:
            webhook_url: Discord webhook URL
            enabled: Whether notifications are enabled
        """
        self.webhook_url = webhook_url
        self.enabled = enabled and webhook_url is not None

        if self.enabled:
            logger.info("Discord notifications enabled")
        else:
            logger.info("Discord notifications disabled")

    async def send_alert(
        self,
        title: str,
        message: str,
        level: str = "info",
        fields: Optional[dict] = None,
    ):
        """Send alert to Discord.

        Args:
            title: Alert title
            message: Alert message
            level: Alert level (info, warning, error, critical)
            fields: Optional additional fields to include
        """
        if not self.enabled:
            return

        try:
            # Color based on level
            colors = {
                "info": 3447003,  # Blue
                "warning": 16776960,  # Yellow
                "error": 16711680,  # Red
                "critical": 10038562,  # Dark Red
            }
            color = colors.get(level, 3447003)

            # Build embed
            embed = {
                "title": f"🚨 {title}",
                "description": message,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Ingress/Egress Tracking Service"},
            }

            # Add fields if provided
            if fields:
                embed["fields"] = [
                    {"name": k, "value": str(v), "inline": True}
                    for k, v in fields.items()
                ]

            payload = {"embeds": [embed]}

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(self.webhook_url, json=payload)
                response.raise_for_status()

            logger.debug("Discord notification sent", title=title, level=level)

        except Exception as e:
            logger.error("Failed to send Discord notification", error=str(e))

    async def send_service_started(self):
        """Send service started notification."""
        await self.send_alert(
            title="Service Started",
            message="Ingress/Egress Tracking Service has started successfully",
            level="info",
        )

    async def send_service_stopped(self):
        """Send service stopped notification."""
        await self.send_alert(
            title="Service Stopped",
            message="Ingress/Egress Tracking Service has stopped",
            level="warning",
        )

    async def send_connection_lost(self, connection_type: str):
        """Send connection lost alert.

        Args:
            connection_type: Type of connection lost (NATS, Kafka, etc.)
        """
        await self.send_alert(
            title=f"{connection_type} Connection Lost",
            message=f"Lost connection to {connection_type}",
            level="error",
            fields={"Connection": connection_type, "Status": "Disconnected"},
        )

    async def send_connection_restored(self, connection_type: str):
        """Send connection restored notification.

        Args:
            connection_type: Type of connection restored
        """
        await self.send_alert(
            title=f"{connection_type} Connection Restored",
            message=f"Connection to {connection_type} has been restored",
            level="info",
            fields={"Connection": connection_type, "Status": "Connected"},
        )

    async def send_model_load_error(self, error: str):
        """Send model load error alert.

        Args:
            error: Error message
        """
        await self.send_alert(
            title="Model Load Error",
            message=f"Failed to load ML model: {error}",
            level="critical",
            fields={"Error": error},
        )

    async def send_crossing_event(
        self, track_id: int, direction: str, camera_id: str, area_id: str
    ):
        """Send crossing event notification (optional, can be noisy).

        Args:
            track_id: Vehicle track ID
            direction: Crossing direction
            camera_id: Camera ID
            area_id: Area ID
        """
        await self.send_alert(
            title="Vehicle Crossing Detected",
            message=f"Vehicle {track_id} crossed the line",
            level="info",
            fields={
                "Track ID": track_id,
                "Direction": direction,
                "Camera": camera_id,
                "Area": area_id,
            },
        )
