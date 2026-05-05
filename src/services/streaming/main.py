"""
main.py — Streaming Service Entry Point
========================================
Starts two Kafka consumer threads and exposes:
  ws://host:8000/ws/stream          → live annotated video frames
  GET http://host:8000/violations   → violation list
  GET http://host:8000/violations/count → running total
  GET http://host:8000/health       → health check

Run:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Or directly:
    python main.py
"""

import logging
import sys
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from annotator import annotate, jpeg_to_base64
from config import get_settings
from consumers.frame_consumer import FrameConsumerThread
from consumers.result_consumer import ResultConsumerThread
from frame_synchronizer import FrameSynchronizer
from state_store import connection_manager, state_store
from routes.websocket import router as ws_router
from routes.violations import router as violations_router
from logging_config import configure_logging, LogLevels

configure_logging(log_level=LogLevels.INFO)
logger = logging.getLogger(__name__)
settings    = get_settings()

# ── Lifespan Handler ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles setup and teardown logic for the application.
    Everything before 'yield' runs on startup.
    Everything after 'yield' runs on shutdown.
    """
    # --- Startup Logic ---
    logger.info("Starting streaming service…")

    synchronizer = FrameSynchronizer(
        max_size=settings.sync_buffer_max_size,
        on_ready=on_frame_ready,
    )

    frame_thread = FrameConsumerThread(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_frames_topic,
        group_id=settings.kafka_frames_group,
        synchronizer=synchronizer,
    )

    result_thread = ResultConsumerThread(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_detection_results_topic,
        group_id=settings.kafka_results_group,
        synchronizer=synchronizer,
        state_store=state_store,
    )

    frame_thread.start()
    result_thread.start()
    
    # Store references in app.state to prevent GC
    app.state.frame_thread = frame_thread
    app.state.result_thread = result_thread
    
    logger.info("Kafka consumer threads started.")

    yield  # The application runs here

    # --- Shutdown Logic ---
    logger.info("Shutting down streaming service…")
    app.state.frame_thread.stop()
    app.state.result_thread.stop()

# ── App Initialization ────────────────────────────────────────────────────────

app = FastAPI(
    title="Scooper Violation Streaming Service",
    lifespan=lifespan  # Register the lifespan handler here
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ws_router)
app.include_router(violations_router)

# ── Synchronizer callback ─────────────────────────────────────────────────────

def on_frame_ready(frame_id: int, jpeg_bytes: bytes, result: dict) -> None:
    """
    Called by FrameSynchronizer when both the raw frame and its detection
    result are available for a given frame_id.

    Steps:
      1. Annotate the frame (draw boxes + violation banner)
      2. Build the WebSocket message
      3. Broadcast to all connected clients
    """
    violation   = result.get("violation")       # dict or None
    detections  = result.get("detections", [])
    timestamp   = result.get("timestamp", 0.0)
    vcount      = result.get("violation_count", state_store.get_count())

    # Draw on the frame
    annotated_jpeg = annotate(
        jpeg_bytes=jpeg_bytes,
        detections=detections,
        violation=violation,
        jpeg_quality=settings.jpeg_quality,
    )

    message = {
        "type":            "frame",
        "frame_id":        frame_id,
        "timestamp":       timestamp,
        "frame":           jpeg_to_base64(annotated_jpeg),
        "detections":      detections,
        "violation":       violation,
        "violation_count": vcount,
    }

    connection_manager.broadcast(message)


# ── Dev entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)