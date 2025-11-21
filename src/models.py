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


class MaskingLine(BaseModel):
    """Masking line configuration for ingress/egress detection."""

    x1: float
    y1: float
    x2: float
    y2: float
    direction: str  # "up" or "down" - indicates which side is ingress

    def to_ingress_egress_config(self) -> IngressEgressConfig:
        """Convert masking line format to IngressEgressConfig.

        The 'direction' field indicates the ingress side:
        - "up": ingress is above the line (vehicles moving up cross in)
        - "down": ingress is below the line (vehicles moving down cross in)
        """
        # Create line from endpoints
        line = LineConfig(
            points=[
                Point(x=self.x1, y=self.y1),
                Point(x=self.x2, y=self.y2),
            ]
        )

        # Calculate a point on the ingress side
        # Use the midpoint of the line and offset perpendicular to it
        mid_x = (self.x1 + self.x2) / 2
        mid_y = (self.y1 + self.y2) / 2

        # Offset distance (pixels)
        offset = 50

        # Calculate perpendicular offset based on direction
        if self.direction.lower() == "up":
            # Ingress is above the line (negative y direction)
            ingress_x = mid_x
            ingress_y = mid_y - offset
        else:  # "down"
            # Ingress is below the line (positive y direction)
            ingress_x = mid_x
            ingress_y = mid_y + offset

        ingress_side_point = LineConfig(
            points=[Point(x=ingress_x, y=ingress_y)]
        )

        return IngressEgressConfig(
            line=line,
            ingress_side_point=ingress_side_point
        )


class FrameMessage(BaseModel):
    """Input frame message from NATS."""

    camera_id: str = Field(alias="cameraId")
    camera_name: Optional[str] = Field(default=None, alias="cameraName")
    area_id: Optional[str] = Field(default=None, alias="areaId")
    model_id: Optional[str] = Field(default=None, alias="modelId")
    timestamp: str
    data: str  # Base64 encoded JPEG frame
    frame_number: int = Field(alias="frameNumber")
    requested_fps: Optional[int] = Field(default=None, alias="requestedFPS")

    # Line configuration (embedded in frame message)
    masking_line: Optional[List[MaskingLine]] = Field(default=None, alias="masking_line")
    danger_zones: Optional[List[Any]] = Field(default=[], alias="dangerZones")
    intrusion_lines_json: Optional[List[Any]] = Field(default=[], alias="intrusion_lines_json")

    metadata: Optional[FrameMetadata] = None

    class Config:
        populate_by_name = True
        protected_namespaces = ()

    def get_line_config(self) -> Optional[IngressEgressConfig]:
        """Extract and convert masking_line to IngressEgressConfig.

        Returns:
            IngressEgressConfig if masking_line is present, None otherwise
        """
        if not self.masking_line or len(self.masking_line) == 0:
            return None

        # Use the first masking line
        return self.masking_line[0].to_ingress_egress_config()


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
    image_path: Optional[str] = None 
   


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

    class Config:
        protected_namespaces = ()
