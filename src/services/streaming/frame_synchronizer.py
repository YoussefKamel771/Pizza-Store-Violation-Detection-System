"""
frame_synchronizer.py
======================
Joins raw frames (from video-frames topic) with detection metadata
(from detection-results topic) using frame_id as the key.

Both Kafka consumer threads call into this class:
  - frame_consumer    → put_frame(frame_id, jpeg_bytes)
  - result_consumer   → put_result(frame_id, result_dict)

When both sides of a frame_id are present, the synchronizer calls
the registered on_ready callback with the joined data so the
annotator can draw boxes and broadcast to WebSocket clients.

Why a buffer and not a simple dict?
  Detection takes a few milliseconds, so results arrive slightly
  after frames. We hold frames in a buffer until the matching
  result arrives, then evict. Entries are also evicted by age
  (max_size) to prevent unbounded memory growth.
"""

import logging
import threading
from collections import OrderedDict
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class FrameSynchronizer:
    def __init__(self, max_size: int = 300, on_ready: Optional[Callable] = None):
        """
        max_size   : evict oldest entries when buffer exceeds this many frame_ids
        on_ready   : callback(frame_id, jpeg_bytes, result_dict) called when joined
        """
        self._lock     = threading.Lock()
        self._max_size = max_size
        self._on_ready = on_ready

        # OrderedDict so we evict the oldest frame_id when we hit max_size
        # Each value is a dict: {"frame": bytes | None, "result": dict | None}
        self._buffer: OrderedDict[int, dict] = OrderedDict()

    def set_callback(self, cb: Callable) -> None:
        self._on_ready = cb

    # ── Called by frame consumer thread ──────────────────────────────────────

    def put_frame(self, frame_id: int, jpeg_bytes: bytes) -> None:
        ready = None
        with self._lock:
            entry = self._buffer.setdefault(frame_id, {"frame": None, "result": None})
            entry["frame"] = jpeg_bytes
            self._buffer.move_to_end(frame_id)

            if entry["result"] is not None:
                ready = (frame_id, entry["frame"], entry["result"])
                del self._buffer[frame_id]

            self._evict_if_needed()

        if ready and self._on_ready:
            self._on_ready(*ready)

    # ── Called by result consumer thread ─────────────────────────────────────

    def put_result(self, frame_id: int, result: dict) -> None:
        ready = None
        with self._lock:
            entry = self._buffer.setdefault(frame_id, {"frame": None, "result": None})
            entry["result"] = result
            self._buffer.move_to_end(frame_id)

            if entry["frame"] is not None:
                ready = (frame_id, entry["frame"], entry["result"])
                del self._buffer[frame_id]

            self._evict_if_needed()

        if ready and self._on_ready:
            self._on_ready(*ready)

    # ── Internals ─────────────────────────────────────────────────────────────

    def _evict_if_needed(self) -> None:
        """Drop oldest entries when buffer is full. Called under lock."""
        while len(self._buffer) > self._max_size:
            evicted_id, _ = self._buffer.popitem(last=False)
            logger.debug("Evicted unmatched frame_id=%d from sync buffer.", evicted_id)