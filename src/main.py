"""Main entry point for Ingress/Egress Tracking Service."""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.config import settings
from src.service import TrackingService
from src.api import app, set_tracking_service
from src.logger import get_logger

logger = get_logger(__name__)

# Global service instance
tracking_service: TrackingService = None


async def shutdown_handler(sig):
    """Handle shutdown signals gracefully.

    Args:
        sig: Signal received
    """
    logger.info(f"Received signal {sig.name}, shutting down...")
    if tracking_service:
        await tracking_service.stop()
    sys.exit(0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager.

    Args:
        app: FastAPI application

    Yields:
        None
    """
    global tracking_service

    try:
        # Create and start tracking service
        tracking_service = TrackingService()
        set_tracking_service(tracking_service)

        # Start service in background
        asyncio.create_task(tracking_service.start())

        # Give service time to initialize
        await asyncio.sleep(2)

        logger.info("Service initialization complete")

    except Exception as e:
        logger.error("Failed to start service", error=str(e))
        sys.exit(1)

    yield

    # Shutdown
    logger.info("Shutting down service...")
    if tracking_service:
        await tracking_service.stop()


# Set lifespan for FastAPI app
app.router.lifespan_context = lifespan


def main():
    """Main entry point."""
    # Setup signal handlers
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda s, _: asyncio.create_task(shutdown_handler(s)))

    # Configure uvicorn
    config = uvicorn.Config(
        app=app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        access_log=True,
        use_colors=True,
    )

    server = uvicorn.Server(config)

    logger.info(
        "Starting Ingress/Egress Tracking Service",
        host=settings.api_host,
        port=settings.api_port,
        frame_subject=settings.nats_frame_subject,
        line_config_subject=settings.nats_line_config_subject,
    )

    # Run server
    loop.run_until_complete(server.serve())


if __name__ == "__main__":
    main()
