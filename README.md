# 🍕 Pizza Store Violation Detection System

A real-time computer vision system that monitors pizza store hygiene compliance by detecting whether workers use a **scooper** when handling protein ingredients. Built on a microservices architecture with Kafka, YOLO, and WebSocket streaming.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services](#services)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [How Violation Detection Works](#how-violation-detection-works)
- [API Reference](#api-reference)
- [Running Locally Without Kafka](#running-locally-without-kafka)
- [Docker Deployment](#docker-deployment)

---

## Overview

Workers at pizza stations must use a scooper when picking up protein ingredients from designated containers (ROIs — Regions of Interest). This system watches camera feeds and flags any instance where a worker reaches into an ROI and places an ingredient on a pizza **without using a scooper**.

**What the system detects:**
- ✅ Hand enters ROI + scooper present → no violation
- ❌ Hand enters ROI + no scooper → **violation flagged**
- ✅ Hand enters ROI but picks up nothing (e.g. cleaning) → no violation
- ✅ Two workers at the table simultaneously → both tracked independently

---

## Architecture

```
┌─────────────────┐     ┌──────────────────────┐     ┌───────────────────────┐
│  Frame Reader   │────▶│   Kafka              │────▶│  Detection Service    │
│                 │     │   [video-frames]     │     │                       │
│  OpenCV / RTSP  │     └──────────────────────┘     │  YOLO11 + ByteTrack   │
└─────────────────┘                                  │  Violation Engine     │
                                                     │  PostgreSQL           │
                                                     └──────────┬────────────┘
                                                                │
                                                     ┌──────────▼────────────┐
                                                     │   Kafka               │
                                                     │   [detection-results] │
                                                     └──────────┬────────────┘
                                                                │
  ┌─────────────────┐     ┌─────────────────────────────────────▼────────┐
  │    Frontend     │◀────│              Streaming Service               │
  │                 │     │                                               │
  │  WebSocket      │     │  Receive message → Annotator → WebSocket      │
  │  REST polling   │     │  REST API  /violations/count  /violations     │
  └─────────────────┘     └───────────────────────────────────────────────┘
```

**Key design decision — Unified Data Stream:** To reduce architectural complexity and synchronization overhead, the detection service now publishes the full payload (metadata + frame bytes) to detection-results. This transforms the pipeline into a linear flow, allowing the streaming service to act as a simple pass-through annotator without needing to buffer or join separate topics.

---

## Services

### 1. Frame Reader Service
Reads video from a file or RTSP camera feed and publishes raw frames to Kafka.

- Supports local video files, webcam (`0`, `1`...), and RTSP streams
- Throttles to a configurable FPS to avoid overwhelming the broker
- Resizes frames before encoding to cap memory usage
- Publishes: `frame_id`, `timestamp`, `source`, `width`, `height`, `frame` (base64 JPEG)

### 2. Detection Service
The core intelligence of the system. Subscribes to `video-frames`, runs the ML pipeline, and publishes results.

- **YOLO11 medium** — detects `Hand`, `Person`, `Pizza`, `Scooper`
- **ByteTrack** — assigns stable track IDs per worker across frames
- **Violation Engine** — stateful logic: tracks hand-ROI interactions, checks for scooper presence
- **ROI Manager** — loads polygon ROIs from `rois.json` (drawn interactively on first run)
- **PostgreSQL** — persists violation records with frame path, timestamp, worker ID, ROI ID
- Publishes to `detection-results`: frame_id + detections + violation (if any) + running count

### 3. Streaming Service
Consumes the enriched `detection-results` topic, annotates the frames, and serves them in real-time.

- **Annotator** — OpenCV draws bounding boxes, ROI highlights, and a red violation banner
- **WebSocket** `/ws/stream` — broadcasts annotated frames to all connected clients
- **REST API** — `/violations/count` and `/violations` for the frontend dashboard

---

## Tech Stack

| Component | Technology |
|---|---|
| Message Broker | Apache Kafka (KRaft mode — no ZooKeeper) |
| Object Detection | YOLOv11 medium (trained on 1254 images) |
| Object Tracking | ByteTrack |
| Video Processing | OpenCV |
| Streaming API | FastAPI + WebSocket |
| Database | PostgreSQL |
| Kafka Client | confluent-kafka |
| Containerisation | Docker + Docker Compose |

---

## Project Structure

```
scooper-violation-system/
│
├── frame_reader/                   # Service 1
│   ├── main.py                     # Entry point — read → encode → publish
│   ├── producer.py                 # Kafka producer wrapper
│   ├── config.py                   # Env-var based config
│   ├── requirements.txt
│   └── Dockerfile
│
├── detection_service/              # Service 2
│   ├── main.py                     # Production entry point (Kafka mode)
│   ├── run_local.py                # Local test without Kafka + ROI drawing
│   ├── detection_manager.py        # Orchestrates detect → track → engine → publish
│   ├── core/
│   │   └── interfaces.py           # Abstract base classes (IDetector, ITracker, etc.)
│   ├── domain/
│   │   └── engine.py               # Scooper violation business logic
│   ├── infrastructure/
│   │   ├── detector.py             # YOLO11 wrapper
│   │   ├── byteTrack_tracker.py    # ByteTrack wrapper
│   │   ├── kafka_consumer.py       # IFrameConsumer implementation
│   │   ├── detection_result_publisher.py  # IDetectionResultPublisher implementation
│   │   ├── roi_manager.py          # ROI loading, saving, interactive drawing
│   │   ├── visualization.py        # OpenCV drawing helpers
│   │   └── postgress_repo.py       # PostgreSQL violation repository
│   ├── helpers/
│   │   └── config.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── streaming_service/              # Service 3
│   ├── main.py                     # FastAPI app — starts background threads
│   ├── state_store.py              # Thread-safe violation store + WS registry
│   ├── annotator.py                # OpenCV frame annotation
│   ├── config.py
│   ├── consumers/
│   │   └── result_consumer.py      # Daemon thread: detection-results topic
│   ├── routes/
│   │   ├── websocket.py            # WS /ws/stream
│   │   └── violations.py           # GET /violations, /violations/count
│   ├── requirements.txt
│   └── Dockerfile
│
├── docker-compose.yml              # Kafka + all services
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker + Docker Compose
- GPU recommended for detection service (CPU works but will be slower)

### 1. Clone the repository

```bash
git clone https://github.com/your-org/scooper-violation-system.git
cd scooper-violation-system
```

### 2. Draw your ROIs (first time only)

ROIs define the protein container zones the system watches. This step runs once and saves `rois.json` which all future runs reuse.

```bash
cd detection_service
pip install -r requirements.txt

# Set your test video path in .env or pass directly:
TEST_VIDEO_PATH=test_data/your_video.mp4 python run_local.py
```

A window will open on the first frame of your video. Follow the on-screen prompts to draw polygons around the protein containers. Press **Q** to save and exit.

> **Controls during `run_local.py`:**
> - `Q` — quit and save ROIs
> - `S` — force-save ROIs mid-session

### 3. Start all services with Docker Compose

```bash
docker-compose up --build
```

This starts:
- Kafka (KRaft, no ZooKeeper)
- Frame Reader (reads your video file)
- Detection Service (runs YOLO + ByteTrack)
- Streaming Service (WebSocket on port 8000)

### 4. Connect your frontend

```
WebSocket:  ws://localhost:8000/ws/stream
REST:       http://localhost:8000/violations/count
            http://localhost:8000/violations
```

---

## Configuration

Each service is configured entirely through environment variables. Create a `.env` file in each service directory or set them in `docker-compose.yml`.

### Frame Reader

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker address |
| `KAFKA_TOPIC` | `video-frames` | Topic to publish frames to |
| `VIDEO_SOURCE` | `0` | `0` = webcam, file path, or `rtsp://...` |
| `MAX_FPS` | `15` | Max frames per second published to Kafka |
| `JPEG_QUALITY` | `80` | JPEG compression quality (0–100) |

### Detection Service

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker address |
| `KAFKA_FRAMES_TOPIC` | `video-frames` | Topic to consume frames from |
| `KAFKA_DETECTION_RESULTS_TOPIC` | `detection-results` | Topic to publish results to |
| `KAFKA_GROUP_ID` | `detection-service` | Kafka consumer group |
| `MODEL_PATH` | `weights/best.pt` | Path to YOLO weights file |
| `ROI_CONFIG_PATH` | `config/rois.json` | Path to saved ROI polygons |
| `DB_CONN_STR` | `postgresql://...` | PostgreSQL connection string |
| `TEST_VIDEO_PATH` | `test_data/sample.mp4` | Video path for `run_local.py` |

### Streaming Service

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker address |
| `KAFKA_FRAMES_TOPIC` | `video-frames` | Raw frames topic |
| `KAFKA_DETECTION_RESULTS_TOPIC` | `detection-results` | Detection metadata topic |
| `KAFKA_FRAMES_GROUP` | `streaming-frames` | Consumer group for frames |
| `KAFKA_RESULTS_GROUP` | `streaming-results` | Consumer group for results |
| `SYNC_BUFFER_MAX_SIZE` | `300` | Max frame_ids held in sync buffer |
| `JPEG_QUALITY` | `80` | Annotated frame re-encode quality |
| `PORT` | `8000` | FastAPI server port |

> **Kafka addresses:**
> - `kafka:9092` — use inside Docker containers
> - `localhost:29092` — use when running services on your host machine

---

## How Violation Detection Works

```
Frame arrives
     │
     ▼
YOLO detects: Hand, Person, Pizza, Scooper
     │
     ▼
ByteTrack assigns stable track_id to each detection
     │
     ▼
Violation Engine evaluates (per track_id):
     │
     ├── Hand enters ROI?
     │        │
     │        ├── NO  → nothing to check
     │        │
     │        └── YES → was a Scooper present and overlapping?
     │                       │
     │                       ├── YES → ✅ no violation
     │                       │
     │                       └── NO  → was something grabbed? (hand moved to pizza)
     │                                       │
     │                                       ├── NO  (cleaning) → ✅ no violation
     │                                       │
     │                                       └── YES → ❌ VIOLATION logged
     ▼
Result published to detection-results topic
```

The engine is **stateful per track_id** — it remembers whether a hand entered an ROI on previous frames and resolves the interaction once the hand moves away from the ROI zone.

---

## API Reference

### WebSocket — `ws://host:8000/ws/stream`

Receives one JSON message per processed frame:

```json
{
  "type":            "frame",
  "frame_id":        42,
  "timestamp":       1714300000.123,
  "frame":           "<base64 annotated JPEG>",
  "detections": [
    {
      "track_id":   1,
      "label":      "Hand",
      "confidence": 0.95,
      "x1": 100, "y1": 200, "x2": 150, "y2": 250,
      "in_roi":     true
    }
  ],
  "violation": {
    "violation_id": "uuid",
    "track_id":     1,
    "roi_id":       "protein_zone",
    "frame_path":   "violations/frame_042.jpg",
    "timestamp":    1714300000.0
  },
  "violation_count": 3
}
```

`violation` is `null` when no violation occurred on that frame. A `{"type": "heartbeat"}` message is sent every ~1 second when no frames are available.

### REST Endpoints

| Method | Path | Response |
|---|---|---|
| `GET` | `/health` | `{"status": "ok"}` |
| `GET` | `/violations/count` | `{"count": 4}` |
| `GET` | `/violations?limit=50` | `{"count": 4, "violations": [...]}` |

---

## Running Locally Without Kafka

Use `run_local.py` inside `detection_service` to test the full ML pipeline on a local video file — no Kafka or Docker required.

```bash
cd detection_service
pip install -r requirements.txt

# First run: draws ROIs interactively
python run_local.py

# Subsequent runs: loads rois.json automatically
python run_local.py
```

What you see:
- Live video window with bounding boxes drawn on every frame
- ROI zones highlighted in yellow
- Red bounding box + banner when a violation is detected
- Running violation counter in the top-left corner

---

## Docker Deployment

### Starting everything

```bash
docker-compose up --build
```

### Starting individual services

```bash
docker-compose up kafka
docker-compose up frame-reader
docker-compose up detection-service
docker-compose up streaming-service
```

### Stopping

```bash
docker-compose down
```

### Using a live RTSP camera instead of a video file

In `docker-compose.yml`, change the `frame-reader` environment:

```yaml
environment:
  VIDEO_SOURCE: rtsp://admin:password@192.168.1.100:554/stream
```

### Kafka topics are created automatically

The Kafka broker is configured with `KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'`. Topics `video-frames` and `detection-results` are created on first publish.

---

## Model

The pretrained model is **YOLO11 medium** trained on 1254 annotated images of the pizza store. It detects four classes:

| Class | Description |
|---|---|
| `Hand` | Worker's hand |
| `Person` | Full worker body |
| `Pizza` | Pizza being assembled |
| `Scooper` | The serving scooper tool |

Place your model weights at `detection_service/weights/best.pt` or set `MODEL_PATH` in the environment.
