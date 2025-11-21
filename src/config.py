"""Configuration for Ingress/Egress Tracking Service."""

from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    """Service configuration."""

    # Service info
    service_name: str = "Ingress-Egress-Tracking-Service"
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # API configuration
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8100, env="API_PORT")

    # NATS configuration
    nats_url: str = Field(default="nats://localhost:4222", env="NATS_URL")
    nats_frame_subject: str = Field(
        default="frames.ml.tracking", env="NATS_FRAME_SUBJECT"
    )
    nats_line_config_subject: str = Field(
        default="config.tracking.lines", env="NATS_LINE_CONFIG_SUBJECT"
    )
    nats_queue_group: str = Field(
        default="tracking-service", env="NATS_QUEUE_GROUP"
    )
    nats_max_pending: int = Field(default=1000, env="NATS_MAX_PENDING")

    # Consumer mode: "dual" (separate frame+config topics) or "unified" (single topic with both)
    nats_consumer_mode: str = Field(default="dual", env="NATS_CONSUMER_MODE")

    # Kafka configuration
    kafka_brokers: str = Field(default="localhost:9092", env="KAFKA_BROKERS")
    kafka_topic_prefix: str = Field(
        default="tracking.events", env="KAFKA_TOPIC_PREFIX"
    )
    kafka_linger_ms: int = Field(default=100, env="KAFKA_LINGER_MS")

    # MinIO configuration
    minio_bucket_name: str = Field(
        default="vehicle-tracking",
        env="MINIO_BUCKET_NAME",
    )
    minio_endpoint: str = Field(
        default="localhost:9000",
        env="MINIO_ENDPOINT",
    )
    minio_access_key: str = Field(
        default="minioadmin",
        env="MINIO_ACCESS_KEY",
    )
    minio_secret_key: str = Field(
        default="minioadmin123",
        env="MINIO_SECRET_KEY",
    )

    # Model configuration
    car_model_name: str = Field(
        default=str(Path(__file__).parent.parent / "Models" / "car_detection.pt"),
        env="CAR_MODEL_NAME",
    )
    car_model_confidence: float = Field(default=0.6, env="CAR_MODEL_CONFIDENCE")
    car_model_iou_threshold: float = Field(default=0.4, env="CAR_MODEL_IOU_THRESHOLD")
    model_device: str = Field(default="cuda:0", env="MODEL_DEVICE")

    # Tracker configurations
    tracker_max_age: int = Field(default=120, env="TRACKER_MAX_AGE")
    tracker_min_hits: int = Field(default=10, env="TRACKER_MIN_HITS")

    # Line crossing detection parameters
    crossing_distance_threshold: float = Field(
        default=20.0, env="CROSSING_DISTANCE_THRESHOLD"
    )  # pixels
    crossing_confirmation_frames: int = Field(
        default=3, env="CROSSING_CONFIRMATION_FRAMES"
    )
    crossing_hysteresis: float = Field(
        default=5.0, env="CROSSING_HYSTERESIS"
    )  # pixels

    # Quality configuration
    min_crop_area_ratio: float = Field(default=0.05, env="MIN_CROP_AREA_RATIO")
    blur_threshold: int = Field(default=100, env="BLUR_THRESHOLD")
    padding_to_crop: float = Field(default=0.1, env="PADDING_TO_CROP")

    # Save configuration
    save_vehicle_crops: bool = Field(default=True, env="SAVE_VEHICLE_CROPS")
    save_crossing_events_only: bool = Field(
        default=True, env="SAVE_CROSSING_EVENTS_ONLY"
    )

    # Discord notifications
    discord_webhook_url: Optional[str] = Field(default=None, env="DISCORD_WEBHOOK_URL")
    discord_enabled: bool = Field(default=False, env="DISCORD_ENABLED")
    discord_notify_on_crossing: bool = Field(
        default=False, env="DISCORD_NOTIFY_ON_CROSSING"
    )

    # Health check configuration
    health_check_interval: int = Field(default=30, env="HEALTH_CHECK_INTERVAL")

    # Multi-camera production settings
    max_cameras: int = Field(default=10, env="MAX_CAMERAS")
    max_tracks_per_camera: int = Field(default=100, env="MAX_TRACKS_PER_CAMERA")
    camera_frame_timeout: float = Field(default=5.0, env="CAMERA_FRAME_TIMEOUT")
    enable_per_camera_metrics: bool = Field(default=True, env="ENABLE_PER_CAMERA_METRICS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        protected_namespaces = ()


# Global settings instance
settings = Settings()
