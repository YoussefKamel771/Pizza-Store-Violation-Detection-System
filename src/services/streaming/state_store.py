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


@dataclass
class ViolationRecord:
    violation_id: str
    frame_id:     int
    track_id:     int
    roi_id:       str


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
            }
            for v in items[:limit]
        ]


# ── WebSocket connection registry ─────────────────────────────────────────────

class ConnectionManager:
    """Tracks all active WebSocket connections and broadcasts to all of them."""

    def __init__(self):
        self._lock  = threading.Lock()
        self._conns = set()     # set of asyncio.Queue — one per connected client

    def add(self, queue) -> None:
        with self._lock:
            self._conns.add(queue)

    def remove(self, queue) -> None:
        with self._lock:
            self._conns.discard(queue)

    def broadcast(self, message: dict) -> None:
        """
        Put the message into every client's queue.
        The WebSocket route handler pulls from its own queue and sends to client.
        Using per-client queues decouples the Kafka thread from FastAPI's event loop.
        """
        with self._lock:
            conns = list(self._conns)
        for q in conns:
            try:
                q.put_nowait(message)
            except Exception:
                pass  # queue full or closed — client will be cleaned up on disconnect


# ── Singletons ────────────────────────────────────────────────────────────────
state_store        = StateStore()
connection_manager = ConnectionManager()