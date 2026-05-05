"""
routes/websocket.py
====================
WebSocket endpoint: ws://host:8000/ws/stream

Each connected client gets an asyncio.Queue injected into the
ConnectionManager. The Kafka consumer threads (running in daemon
threads) call connection_manager.broadcast() which puts messages
into every client queue. The WebSocket coroutine pulls from its
own queue and sends to the browser.

Message sent to frontend (JSON):
{
    "type":            "frame",
    "frame_id":        42,
    "timestamp":       1714300000.123,
    "frame":           "<base64 annotated JPEG>",
    "detections": [
        {
            "track_id": 1, "label": "Hand", "confidence": 0.95,
            "x1": 100, "y1": 200, "x2": 150, "y2": 250, "in_roi": true
        }
    ],
    "violation":       null,        // or violation dict
    "violation_count": 3
}
"""

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from state_store import connection_manager

logger  = logging.getLogger(__name__)
router  = APIRouter()


@router.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket client connected: %s", websocket.client)

    # Each client owns a dedicated asyncio queue.
    # The broadcast() method (called from Kafka threads) puts messages here.
    # maxsize=30 → if a slow client can't keep up, old frames are dropped
    # to prevent memory build-up.
    queue: asyncio.Queue = asyncio.Queue(maxsize=30)
    connection_manager.add(queue)

    try:
        while True:
            try:
                # Wait up to 1 s for a new frame.
                # This also lets us detect a disconnected client promptly.
                message = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # Send a lightweight heartbeat so the browser knows we're alive
                await websocket.send_text(json.dumps({"type": "heartbeat"}))
                continue

            await websocket.send_text(json.dumps(message))

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected: %s", websocket.client)
    except Exception as exc:
        logger.error("WebSocket error: %s", exc)
    finally:
        connection_manager.remove(queue)