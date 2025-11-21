"""Line crossing detection algorithm for ingress/egress tracking.

This module implements a robust line crossing detection algorithm that:
1. Determines which side of a line a point is on using cross product
2. Tracks vehicle movement across the line
3. Determines crossing direction (ingress vs egress)
4. Implements hysteresis to prevent false triggers
"""

import numpy as np
from typing import Tuple, Optional, Dict
from collections import deque
from src.models import Point, IngressEgressConfig, CrossingDirection
from src.logger import get_logger

logger = get_logger(__name__)


class LineCrossingDetector:
    """Detects when tracked objects cross a defined line."""

    def __init__(
        self,
        config: IngressEgressConfig,
        distance_threshold: float = 20.0,
        confirmation_frames: int = 3,
        hysteresis: float = 5.0,
    ):
        """Initialize line crossing detector.

        Args:
            config: Line configuration with line and ingress side point
            distance_threshold: Minimum distance from line to trigger crossing (pixels)
            confirmation_frames: Number of frames to confirm direction
            hysteresis: Distance buffer to prevent oscillation (pixels)
        """
        self.config = config
        self.distance_threshold = distance_threshold
        self.confirmation_frames = confirmation_frames
        self.hysteresis = hysteresis

        # Extract line points
        self.line_p1 = config.line.points[0]
        self.line_p2 = config.line.points[1]
        self.ingress_point = config.ingress_side_point.points[0]

        # Determine which side is ingress using the reference point
        self.ingress_side = self._get_side(self.ingress_point)

        # Track state for each object
        self.track_history: Dict[int, deque] = {}  # track_id -> deque of (x, y, side)
        self.track_state: Dict[int, str] = {}  # track_id -> state
        self.track_crossed: Dict[int, bool] = {}  # track_id -> has_crossed

        logger.info(
            "LineCrossingDetector initialized",
            line_p1=f"({self.line_p1.x:.1f}, {self.line_p1.y:.1f})",
            line_p2=f"({self.line_p2.x:.1f}, {self.line_p2.y:.1f})",
            ingress_side=self.ingress_side,
            distance_threshold=distance_threshold,
        )

    def _get_side(self, point: Point) -> str:
        """Determine which side of the line a point is on.

        Uses the cross product method:
        - Positive cross product = left side (relative to line direction)
        - Negative cross product = right side
        - Zero = on the line

        Args:
            point: Point to check

        Returns:
            "left" or "right"
        """
        # Vector from line_p1 to line_p2
        line_vec_x = self.line_p2.x - self.line_p1.x
        line_vec_y = self.line_p2.y - self.line_p1.y

        # Vector from line_p1 to point
        point_vec_x = point.x - self.line_p1.x
        point_vec_y = point.y - self.line_p1.y

        # Cross product (z-component of 3D cross product)
        cross_product = line_vec_x * point_vec_y - line_vec_y * point_vec_x

        return "left" if cross_product > 0 else "right"

    def _distance_to_line(self, point: Point) -> float:
        """Calculate perpendicular distance from point to line.

        Args:
            point: Point to measure distance from

        Returns:
            Distance in pixels
        """
        # Line equation: ax + by + c = 0
        # Direction vector
        dx = self.line_p2.x - self.line_p1.x
        dy = self.line_p2.y - self.line_p1.y

        # Perpendicular distance formula
        numerator = abs(
            dy * (point.x - self.line_p1.x) - dx * (point.y - self.line_p1.y)
        )
        denominator = np.sqrt(dx * dx + dy * dy)

        if denominator == 0:
            return 0.0

        return numerator / denominator

    def _get_crossing_point(self, p1: Point, p2: Point) -> Optional[Point]:
        """Calculate the intersection point between track movement and the line.

        Args:
            p1: Previous position
            p2: Current position

        Returns:
            Intersection point or None if no intersection
        """
        # Line segment 1: the detection line (self.line_p1 to self.line_p2)
        # Line segment 2: track movement (p1 to p2)

        x1, y1 = self.line_p1.x, self.line_p1.y
        x2, y2 = self.line_p2.x, self.line_p2.y
        x3, y3 = p1.x, p1.y
        x4, y4 = p2.x, p2.y

        denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)

        if abs(denom) < 1e-10:
            # Lines are parallel
            return None

        t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
        u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / denom

        if 0 <= t <= 1 and 0 <= u <= 1:
            # Intersection exists
            x = x1 + t * (x2 - x1)
            y = y1 + t * (y2 - y1)
            return Point(x=x, y=y)

        return None

    def update(
        self, track_id: int, centroid_x: float, centroid_y: float
    ) -> Optional[Tuple[CrossingDirection, Point]]:
        """Update tracking state and detect line crossing.

        Args:
            track_id: Unique track ID
            centroid_x: X coordinate of track centroid
            centroid_y: Y coordinate of track centroid

        Returns:
            Tuple of (CrossingDirection, crossing_point) if crossing detected, else None
        """
        current_point = Point(x=centroid_x, y=centroid_y)
        current_side = self._get_side(current_point)
        distance = self._distance_to_line(current_point)

        # Initialize tracking for new tracks
        if track_id not in self.track_history:
            self.track_history[track_id] = deque(
                maxlen=self.confirmation_frames * 2
            )
            self.track_state[track_id] = "not_crossed"
            self.track_crossed[track_id] = False

        # Add current position to history
        self.track_history[track_id].append((centroid_x, centroid_y, current_side))

        # Need at least 2 points to detect crossing
        if len(self.track_history[track_id]) < 2:
            return None

        # Get previous position
        prev_x, prev_y, prev_side = self.track_history[track_id][-2]
        prev_point = Point(x=prev_x, y=prev_y)

        # Check if we have already detected a crossing for this track
        if self.track_crossed[track_id]:
            # Reset only if track moves far enough from the line
            if distance > self.distance_threshold + self.hysteresis:
                self.track_crossed[track_id] = False
                self.track_state[track_id] = "not_crossed"
            return None

        # Detect side change (potential crossing)
        if prev_side != current_side:
            # Confirm the crossing by checking if we're close enough to the line
            if distance <= self.distance_threshold + self.hysteresis:
                # Determine crossing direction
                if prev_side == self.ingress_side and current_side != self.ingress_side:
                    # Moving from ingress side to egress side = EGRESS (checkout)
                    direction = CrossingDirection.EGRESS
                elif (
                    prev_side != self.ingress_side and current_side == self.ingress_side
                ):
                    # Moving from egress side to ingress side = INGRESS (checkin)
                    direction = CrossingDirection.INGRESS
                else:
                    # Shouldn't happen, but handle it
                    return None

                # Calculate crossing point
                crossing_point = self._get_crossing_point(prev_point, current_point)
                if crossing_point is None:
                    # Use midpoint as fallback
                    crossing_point = Point(
                        x=(prev_x + centroid_x) / 2, y=(prev_y + centroid_y) / 2
                    )

                # Mark as crossed to prevent duplicate detections
                self.track_crossed[track_id] = True
                self.track_state[track_id] = "crossed"

                logger.info(
                    "Line crossing detected",
                    track_id=track_id,
                    direction=direction.value,
                    crossing_point=f"({crossing_point.x:.1f}, {crossing_point.y:.1f})",
                    distance_to_line=f"{distance:.1f}px",
                )

                return (direction, crossing_point)

        return None

    def reset_track(self, track_id: int):
        """Reset tracking state for a specific track.

        Args:
            track_id: Track ID to reset
        """
        if track_id in self.track_history:
            del self.track_history[track_id]
        if track_id in self.track_state:
            del self.track_state[track_id]
        if track_id in self.track_crossed:
            del self.track_crossed[track_id]

    def reset_all(self):
        """Reset all tracking state."""
        self.track_history.clear()
        self.track_state.clear()
        self.track_crossed.clear()
        logger.info("All tracking state reset")

    def get_track_count(self) -> int:
        """Get number of currently tracked objects."""
        return len(self.track_history)
