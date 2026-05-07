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

from config import get_settings
from consumers.result_consumer import ResultConsumerThread
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

    result_thread = ResultConsumerThread(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_detection_results_topic,
        group_id=settings.kafka_results_group,
        state_store=state_store,
        connection_manager=connection_manager,
        jpeg_quality=settings.jpeg_quality,
    )
    result_thread.start()
 
    app.state.result_thread = result_thread
    logger.info("Result consumer thread started.")

    yield  # The application runs here

    # --- Shutdown Logic ---
    logger.info("Shutting down streaming service…")
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


# ── Dev entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)