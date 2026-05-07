"""
state_store.py
==============
Thread-safe in-memory store shared by:
  - Kafka consumer threads  (writers)
  - FastAPI route handlers  (readers)
  - WebSocket broadcaster   (reader)
"""

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
import asyncio

@dataclass
class ViolationRecord:
    violation_id: str
    frame_id:     int
    track_id:     int
    roi_id:       str
    timestamp: str


# "violation_id": str(violation.id),
# "track_id":     int(violation.track_id),
# "roi_id":       str(violation.roi_name),

class StateStore:
    def __init__(self, max_violations: int = 500):
        self._lock              = threading.Lock()
        self._violation_count   = 0
        self._violations        = deque(maxlen=max_violations)  # most recent N violations

    # ── Violation API ─────────────────────────────────────────────────────────

    def add_violation(self, violation_dict: dict) -> None:
        record = ViolationRecord(
            violation_id = violation_dict["violation_id"],
            frame_id     = violation_dict.get("frame_id", -1),
            track_id     = violation_dict["track_id"],
            roi_id       = violation_dict["roi_id"],
            timestamp= violation_dict.get("timestamp", ""),
        )
        with self._lock:
            # Use the authoritative count from the detection service
            # (it tracks the true session total even if we missed messages)
            self._violation_count = violation_dict.get("violation_count", self._violation_count + 1)
            self._violations.append(record)

    def get_count(self) -> int:
        with self._lock:
            return self._violation_count

    def get_violations(self, limit: int = 50) -> list:
        with self._lock:
            items = list(self._violations)
        # Return most recent first
        items.reverse()
        return [
            {
                "violation_id": v.violation_id,
                "frame_id":     v.frame_id,
                "track_id":     v.track_id,
                "roi_id":       v.roi_id,
                "timestamp":    v.timestamp,
            }
            for v in items[:limit]
        ]


# ── WebSocket connection registry ─────────────────────────────────────────────

class ConnectionManager:
    """Tracks all active WebSocket connections and broadcasts to all of them."""

    def __init__(self):
        self._lock  = threading.Lock()
        self._conns = {}          # queue → event_loop


    def add(self, queue, loop) -> None:      # ← accept the loop
        with self._lock:
            self._conns[queue] = loop

    def remove(self, queue) -> None:
        with self._lock:
            self._conns.pop(queue, None)


    def broadcast(self, message: dict) -> None:
        with self._lock:
            pairs = list(self._conns.items())
        for q, loop in pairs:
           loop.call_soon_threadsafe(_put_or_drop, q, message)

def _put_or_drop(q: asyncio.Queue, message: dict) -> None:
    """
    For a live video stream we always want the *latest* frame.
    If the consumer is behind, evict the oldest queued frame and
    enqueue the new one — this keeps latency low instead of building
    a growing backlog.
    """
    if q.full():
        try:
            q.get_nowait()   # discard oldest frame
        except asyncio.QueueEmpty:
            pass
    try:
        q.put_nowait(message)
    except asyncio.QueueFull:
        pass   # extremely unlikely after the drain above; just drop

# ── Singletons ────────────────────────────────────────────────────────────────
state_store        = StateStore()
connection_manager = ConnectionManager()