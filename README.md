# Ingress/Egress Tracking Service

Production-ready vehicle tracking service with line crossing detection for ingress/egress events. Built with YOLO object detection, BotSort tracking, and precision line crossing algorithms.

## ✅ Multi-Camera Support

**PRODUCTION READY FOR 2-10 CAMERAS** on a single GPU instance:
- ✅ Independent tracking per camera
- ✅ Separate line configurations per camera
- ✅ Per-camera metrics and monitoring
- ✅ GPU sharing with fair scheduling
- ✅ Kafka events segregated by camera_id

**See [PRODUCTION_DEPLOYMENT.md](./PRODUCTION_DEPLOYMENT.md) for complete multi-camera setup guide.**

## Features

- **Accurate Line Crossing Detection**: Robust algorithm using cross-product mathematics with hysteresis and confirmation frames
- **Real-time Vehicle Tracking**: YOLO + BotSort for reliable multi-object tracking
- **Ingress/Egress Classification**: Automatic determination of check-in (value: 1) vs check-out (value: -1)
- **NATS Integration**: Receives frames and line configurations via NATS
- **Kafka Event Publishing**: Publishes crossing events with full metadata
- **MinIO Storage**: Automatic saving of vehicle crops on crossing events
- **Discord Notifications**: Optional webhook alerts for service health and events
- **Production Ready**: Health checks, metrics, proper error handling, and graceful shutdown

## Architecture

```
NATS (Frames) ──┐
                ├──> Service ──> Line Crossing Detection ──> Kafka Events
NATS (Config) ──┘                                        └──> MinIO Storage
```

## Output Format

Each crossing event published to Kafka contains:

```json
{
  "eventType": "line_crossing",
  "trackId": 123,
  "direction": "ingress",          // or "egress"
  "directionValue": 1,              // 1 for ingress (check-in), -1 for egress (check-out)
  "cameraId": "camera-01",
  "cameraName": "Main Entrance",
  "areaId": "area-parking-A",
  "confidence": 0.89,
  "crossingPoint": {"x": 320.5, "y": 240.8},
  "frameNumber": 1024,
  "timestamp": "2025-11-20T10:30:45.123Z",
  "bbox": {"x": 100, "y": 150, "width": 80, "height": 120},
  "imagePath": "camera-01/area-parking-A/ingress/123/abc-123.jpg"
}
```

## Line Configuration Format

Send line configurations via NATS subject `config.tracking.lines`:

```json
{
  "cameraId": "camera-01",
  "cameraName": "Main Entrance",
  "areaId": "area-parking-A",
  "config": [
    {
      "points": [
        {"x": 14.0625, "y": 68.167},
        {"x": 83.4375, "y": 74.208}
      ]
    },
    {
      "points": [
        {"x": 43.462, "y": 78.757}
      ]
    }
  ]
}
```

- **First element**: Two points defining the line
- **Second element**: Single point indicating the ingress side (opposite side is egress)

## Quick Start

### Prerequisites

- NVIDIA GPU with CUDA support
- Docker and Docker Compose
- YOLO model files in `Models/` directory
- BotSort Re-ID weights: `Models/osnet_x0_25_msmt17.pt`

### Installation

1. Clone the repository:
```bash
cd iop-tracking-service
```

2. Add your YOLO model:
```bash
mkdir -p Models
# Copy car_detection.pt and osnet_x0_25_msmt17.pt to Models/
```

3. Configure environment:
```bash
cp .env.example .env
# Edit .env with your settings
```

4. Start the service:
```bash
docker-compose up -d
```

5. Check service health:
```bash
curl http://localhost:8100/health
```

## Configuration

### Environment Variables

See `.env.example` for all available configuration options.

**Key Parameters:**

- `CROSSING_DISTANCE_THRESHOLD`: Distance from line to trigger crossing (default: 20px)
- `CROSSING_CONFIRMATION_FRAMES`: Frames to confirm direction (default: 3)
- `CROSSING_HYSTERESIS`: Buffer zone to prevent oscillation (default: 5px)
- `TRACKER_MAX_AGE`: Max frames to keep lost tracks (default: 120)
- `TRACKER_MIN_HITS`: Min detections before tracking (default: 10)

### Discord Notifications

Enable Discord alerts for service monitoring:

```env
DISCORD_ENABLED=true
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_WEBHOOK_URL
DISCORD_NOTIFY_ON_CROSSING=false  # Set true for crossing event notifications (can be noisy)
```

Discord will alert on:
- Service start/stop
- Connection failures (NATS/Kafka)
- Model load errors
- Optionally: Line crossing events

## API Endpoints

- `GET /` - Service info
- `GET /health` - Health check
- `GET /status` - Detailed status with metrics
- `GET /metrics` - Prometheus-compatible metrics

## Accuracy Features

The line crossing detection algorithm ensures high accuracy through:

1. **Cross Product Mathematics**: Precise side determination
2. **Perpendicular Distance Calculation**: Accurate proximity detection
3. **Line Segment Intersection**: True crossing point calculation
4. **Hysteresis Band**: Prevents false triggers from noise
5. **Direction Confirmation**: Multi-frame validation
6. **Duplicate Prevention**: State tracking to avoid repeat detections

## Development

### Project Structure

```
iop-tracking-service/
├── src/
│   ├── line_crossing.py      # Core crossing detection algorithm
│   ├── tracker.py             # Vehicle detection and tracking
│   ├── consumer.py            # NATS message consumer
│   ├── producer.py            # Kafka event producer
│   ├── service.py             # Main service orchestrator
│   ├── discord_notifier.py    # Discord webhook integration
│   ├── config.py              # Configuration management
│   ├── models.py              # Data models
│   ├── minio_client.py        # MinIO client
│   ├── api.py                 # FastAPI endpoints
│   ├── logger.py              # Structured logging
│   └── main.py                # Entry point
├── Models/                    # YOLO and Re-ID models
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run service
python -m src.main
```

## Monitoring

### Health Checks

The service performs automatic health monitoring every 30 seconds (configurable):
- NATS connection status
- Kafka connection status
- Model load status

Alerts are sent via Discord when status changes occur.

### Metrics

Access Prometheus metrics at `/metrics`:
- `frames_processed_total`
- `crossing_events_detected_total`
- `avg_processing_time_ms`
- `active_cameras`
- `service_uptime_seconds`

## Troubleshooting

### Common Issues

**No crossing events detected:**
- Verify line configuration is sent via NATS
- Check `CROSSING_DISTANCE_THRESHOLD` is appropriate for your resolution
- Ensure vehicles are tracked (check logs for track IDs)

**High false positive rate:**
- Increase `CROSSING_CONFIRMATION_FRAMES`
- Increase `CROSSING_HYSTERESIS`
- Adjust `TRACKER_MIN_HITS`

**Model not loading:**
- Verify model file exists at configured path
- Check GPU availability and CUDA version
- Review logs for specific error messages

## Testing Multiple Cameras

Test the service with 2 simulated cameras:

```bash
# Install test dependencies
pip install nats-py opencv-python numpy

# Run 2-camera simulation
python test_two_cameras.py
```

This will:
1. Configure lines for Camera 1 (Entrance) and Camera 2 (Exit)
2. Send 10 test frames from each camera
3. Verify service processes both streams

**Check results:**
```bash
# Per-camera metrics
curl http://localhost:8100/cameras

# Service logs
docker logs -f ingress-egress-tracking-service

# Kafka events
kafka-console-consumer --topic tracking.events.entrance-cam-01
kafka-console-consumer --topic tracking.events.exit-cam-02
```

## License

Proprietary - Icons of Porsche ML Services

## Support

For issues and support, check Discord notifications or service logs.
