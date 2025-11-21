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
    """Masking line configuration for ingress/egress detection.

    Accepts both formats:
    1. Flat format: x1, y1, x2, y2, direction
    2. Points array format: points array (from frame router)
    """

    # Flat format (original)
    x1: Optional[float] = None
    y1: Optional[float] = None
    x2: Optional[float] = None
    y2: Optional[float] = None
    direction: Optional[str] = None  # "up" or "down" - indicates which side is ingress

    # Points array format (from frame router)
    points: Optional[List[Point]] = None

    def to_ingress_egress_config(self, frame_width: int, frame_height: int) -> IngressEgressConfig:
        """Convert masking line format to IngressEgressConfig.

        Supports two input formats:
        1. Flat format: x1, y1, x2, y2, direction fields
        2. Points array format: points array with 2 points for line

        The 'direction' field indicates the ingress side:
        - "up": ingress is above the line (vehicles moving up cross in)
        - "down": ingress is below the line (vehicles moving down cross in)

        Args:
            frame_width: Frame width in pixels (for percentage conversion)
            frame_height: Frame height in pixels (for percentage conversion)
        """
        # Handle points array format (from frame router)
        if self.points is not None and len(self.points) >= 2:
            # Extract coordinates from points array
            x1 = self.points[0].x
            y1 = self.points[0].y
            x2 = self.points[1].x
            y2 = self.points[1].y

        # Handle flat format (original)
        elif self.x1 is not None and self.y1 is not None and self.x2 is not None and self.y2 is not None:
            x1, y1, x2, y2 = self.x1, self.y1, self.x2, self.y2
        else:
            raise ValueError("MaskingLine must have either 'points' array or x1/y1/x2/y2 fields")

        # Determine if coordinates are percentage (0-100) or pixel values
        # If all coordinates are <= 100, assume percentage coordinates
        is_percentage = all(coord <= 100 for coord in [x1, y1, x2, y2])

        # Convert percentage to pixels
        if is_percentage:
            x1_px = int(frame_width * x1 / 100)
            y1_px = int(frame_height * y1 / 100)
            x2_px = int(frame_width * x2 / 100)
            y2_px = int(frame_height * y2 / 100)
            offset = 50.0  # Use pixel offset
        else:
            # Already in pixels
            x1_px = int(x1)
            y1_px = int(y1)
            x2_px = int(x2)
            y2_px = int(y2)
            offset = 50.0

        # Create line from pixel coordinates
        line = LineConfig(
            points=[
                Point(x=float(x1_px), y=float(y1_px)),
                Point(x=float(x2_px), y=float(y2_px)),
            ]
        )

        # Calculate ingress point using pixel coordinates
        mid_x = (x1_px + x2_px) / 2
        mid_y = (y1_px + y2_px) / 2

        # Calculate perpendicular offset based on direction
        # Default to "down" if direction is not specified
        direction = self.direction or "down"

        if direction.lower() == "up":
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

        Supports two formats:
        1. Full config: [line with 2 points, ingress_side_point with 1 point]
        2. Single line with direction: [line with 2 points + direction field]

        Returns:
            IngressEgressConfig if masking_line is present, None otherwise
        """
        if not self.masking_line or len(self.masking_line) == 0:
            return None

        # Get frame dimensions from metadata
        if not self.metadata:
            from src.logger import get_logger
            logger = get_logger(__name__)
            logger.warning("No metadata available for coordinate conversion")
            return None

        # Check if we have a full config (2 elements: line + ingress point)
        if len(self.masking_line) == 2:
            line_element = self.masking_line[0]
            ingress_element = self.masking_line[1]

            # Validate structure: first has 2 points, second has 1 point
            if (line_element.points and len(line_element.points) == 2 and
                ingress_element.points and len(ingress_element.points) == 1):

                # Convert percentage to pixels for both elements
                frame_width = self.metadata.width
                frame_height = self.metadata.height

                # Convert line points
                x1, y1 = line_element.points[0].x, line_element.points[0].y
                x2, y2 = line_element.points[1].x, line_element.points[1].y
                is_percentage = all(coord <= 100 for coord in [x1, y1, x2, y2])

                if is_percentage:
                    line_points = [
                        Point(x=frame_width * x1 / 100, y=frame_height * y1 / 100),
                        Point(x=frame_width * x2 / 100, y=frame_height * y2 / 100)
                    ]
                else:
                    line_points = line_element.points

                # Convert ingress point
                ing_x, ing_y = ingress_element.points[0].x, ingress_element.points[0].y
                if ing_x <= 100 and ing_y <= 100:  # Assume percentage
                    ingress_points = [
                        Point(x=frame_width * ing_x / 100, y=frame_height * ing_y / 100)
                    ]
                else:
                    ingress_points = ingress_element.points

                # Create IngressEgressConfig directly
                line = LineConfig(points=line_points)
                ingress_side_point = LineConfig(points=ingress_points)

                return IngressEgressConfig(
                    line=line,
                    ingress_side_point=ingress_side_point
                )

        # Fallback: Single line with direction - calculate ingress point
        return self.masking_line[0].to_ingress_egress_config(
            frame_width=self.metadata.width,
            frame_height=self.metadata.height
        )


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
