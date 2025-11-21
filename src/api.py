"""FastAPI endpoints for service health and status."""

import time
from fastapi import FastAPI, HTTPException
from typing import Optional

from src.models import ServiceStatus
from src.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(
    title="Ingress/Egress Tracking Service",
    description="Vehicle tracking service with line crossing detection for ingress/egress events",
    version="1.0.0",
)

# Global service instance (set by main)
_service: Optional[object] = None


def set_tracking_service(service):
    """Set the global tracking service instance.

    Args:
        service: TrackingService instance
    """
    global _service
    _service = service


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Ingress/Egress Tracking Service",
        "version": "1.0.0",
        "status": "running",
    }


@app.get("/health")
async def health():
    """Health check endpoint.

    Returns:
        Health status
    """
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        status = _service.get_status()

        if status["status"] == "healthy":
            return {"status": "healthy", "message": "Service is running normally"}
        else:
            return {
                "status": "degraded",
                "message": "Service is running but encountered errors",
                "last_error": status.get("last_error"),
            }

    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@app.get("/status", response_model=ServiceStatus)
async def status():
    """Get detailed service status.

    Returns:
        ServiceStatus: Detailed service status including metrics
    """
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        status_data = _service.get_status()
        return ServiceStatus(**status_data)

    except Exception as e:
        logger.error("Status check failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")


@app.get("/metrics")
async def metrics():
    """Get service metrics.

    Returns:
        Service metrics in Prometheus-compatible format
    """
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        status_data = _service.get_status()

        metrics_text = f"""

frames_processed_total {status_data['frames_processed']}


crossing_events_detected_total {status_data['crossing_events_detected']}


avg_processing_time_ms {status_data['avg_processing_time']}


active_cameras {status_data['active_cameras']}


service_uptime_seconds {status_data['uptime']}
"""

        return metrics_text

    except Exception as e:
        logger.error("Metrics retrieval failed", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Metrics retrieval failed: {str(e)}"
        )


@app.get("/cameras")
async def cameras():
    """Get per-camera metrics and status.

    Returns:
        Per-camera breakdown of processing metrics
    """
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        camera_data = {}

        for camera_id, metrics in _service.camera_metrics.items():
            avg_time = (
                metrics["total_processing_time"] / metrics["frames_processed"]
                if metrics["frames_processed"] > 0
                else 0.0
            )

            camera_data[camera_id] = {
                "frames_processed": metrics["frames_processed"],
                "crossing_events": metrics["crossing_events"],
                "avg_processing_time_ms": round(avg_time, 2),
                "last_seen_seconds_ago": round(time.time() - metrics["last_seen"], 2),
                "has_line_configured": camera_id in _service.tracker.line_detectors,
                "metadata": _service.tracker.camera_metadata.get(camera_id, {}),
            }

        return {
            "total_cameras": len(camera_data),
            "cameras": camera_data,
        }

    except Exception as e:
        logger.error("Camera metrics retrieval failed", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Camera metrics failed: {str(e)}"
        )
