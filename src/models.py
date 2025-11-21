"""Data Models for Ingress/Egress Tracking Service.

Pydantic models for message validation and serialization.
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class Point(BaseModel):
    """2D point coordinates."""

    x: float
    y: float


class LineConfig(BaseModel):
    """Line configuration for ingress/egress detection."""

    points: List[Point]

    @field_validator("points")
    @classmethod
    def validate_points(cls, v):
        """Validate points list."""
        if len(v) == 0:
            raise ValueError("Points list cannot be empty")
        return v


class IngressEgressConfig(BaseModel):
    """Complete ingress/egress line configuration."""

    line: LineConfig  # First element - the line itself (2 points)
    ingress_side_point: LineConfig  # Second element - point indicating ingress side

    @field_validator("line")
    @classmethod
    def validate_line(cls, v):
        """Ensure line has exactly 2 points."""
        if len(v.points) != 2:
            raise ValueError(f"Line must have exactly 2 points, got {len(v.points)}")
        return v

    @field_validator("ingress_side_point")
    @classmethod
    def validate_ingress_point(cls, v):
        """Ensure ingress side has exactly 1 point."""
        if len(v.points) != 1:
            raise ValueError(
                f"Ingress side point must have exactly 1 point, got {len(v.points)}"
            )
        return v


class LineConfigMessage(BaseModel):
    """Line configuration message from NATS."""

    camera_id: str = Field(alias="cameraId")
    camera_name: str = Field(alias="cameraName")
    area_id: str = Field(alias="areaId")
    config: List[LineConfig]  # Raw config as received
    timestamp: Optional[str] = None

    class Config:
        populate_by_name = True

    def to_ingress_egress_config(self) -> IngressEgressConfig:
        """Convert raw config to structured IngressEgressConfig."""
        if len(self.config) != 2:
            raise ValueError(
                f"Config must have exactly 2 elements (line and ingress point), got {len(self.config)}"
            )
        return IngressEgressConfig(line=self.config[0], ingress_side_point=self.config[1])


class FrameMetadata(BaseModel):
    """Frame metadata from NATS."""

    frame_number: int = Field(alias="frameNumber")
    width: int
    height: int
    source_fps: int = Field(alias="sourceFps")

    class Config:
        populate_by_name = True


class FrameMessage(BaseModel):
    """Input frame message from NATS."""

    camera_id: str = Field(alias="cameraId")
    camera_name: Optional[str] = Field(default=None, alias="cameraName")
    area_id: Optional[str] = Field(default=None, alias="areaId")
    timestamp: str
    data: str  # Base64 encoded frame
    model_id: Optional[str] = Field(default=None, alias="modelId")
    metadata: Optional[FrameMetadata] = None

    class Config:
        populate_by_name = True
        protected_namespaces = ()


class BoundingBox(BaseModel):
    """Bounding box coordinates."""

    x: int
    y: int
    width: int
    height: int


class Detection(BaseModel):
    """Detection result."""

    class_name: str = Field(alias="class")
    track_id: int
    confidence: float
    bbox: BoundingBox
    centroid_x: Optional[float] = None
    centroid_y: Optional[float] = None

    class Config:
        populate_by_name = True


class CrossingDirection(str, Enum):
    """Direction of line crossing."""

    INGRESS = "ingress"  # Check-in (value: 1)
    EGRESS = "egress"  # Check-out (value: -1)


class CrossingEvent(BaseModel):
    """Vehicle line crossing event."""

    track_id: int
    direction: CrossingDirection
    direction_value: int  # 1 for ingress, -1 for egress
    timestamp: str
    camera_id: str
    camera_name: str
    area_id: str
    confidence: float
    bbox: BoundingBox
    crossing_point: Point
    frame_number: int
    image_path: Optional[str] = None  # MinIO path to vehicle crop


class ServiceStatus(BaseModel):
    """Service health status."""

    status: str
    uptime: float
    frames_processed: int
    avg_processing_time: float
    crossing_events_detected: int
    active_cameras: int
    active_connections: Dict[str, bool]
    model_loaded: bool
    last_error: Optional[str] = None
