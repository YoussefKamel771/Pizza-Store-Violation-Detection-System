"""
consumers/result_consumer.py
=============================
Single background thread that reads from `detection-results`.
 
The detection service now publishes everything in one message:
    frame_id, timestamp, frame (base64 JPEG), detections, violation, violation_count
 
This thread:
  1. Decodes the frame
  2. Annotates it (draws boxes + violation banner)
  3. Broadcasts the WebSocket message to all connected clients
  4. Updates the StateStore when a violation is present
"""

import json
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
import numpy as np
from datetime import datetime, timezone
from state_store import ConnectionManager, StateStore

logger = logging.getLogger(__name__)


class ResultConsumerThread(threading.Thread):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        state_store: StateStore,
        connection_manager: ConnectionManager,
        jpeg_quality: int = 80,
    ):
        super().__init__(name="kafka-result-consumer", daemon=True)
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._group_id          = group_id
        self._state_store       = state_store
        self._connection_manager = connection_manager
        self._jpeg_quality      = jpeg_quality
        self._stop_event        = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info("Result consumer thread starting | topic=%s", self._topic)
        consumer = self._create_consumer()

        try:
            while not self._stop_event.is_set():
                msg = consumer.poll(timeout=0.05)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Result consumer error: %s", msg.error())
                    continue

                try:
                    self._process(json.loads(msg.value().decode("utf-8")))

                except Exception as exc:
                    logger.error(
                        "Failed to process result at offset %d: %s", msg.offset(), exc
                    )

        finally:
            consumer.close()
            logger.info("Result consumer thread stopped.")

    # ── Message processing ────────────────────────────────────────────────────
 
    def _process(self, result: dict) -> None:
        frame_b64       = result["frame"]
        frame_id        = result["frame_id"]
        # Convert float to ISO 8601 string (e.g., "2026-05-07T12:00:00+00:00")
        formatted_ts = datetime.fromtimestamp(result["timestamp"], tz=timezone.utc).isoformat()
        violation       = result.get("violation")       # dict or None
        violation_count = result.get("violation_count", 0)
 
        # 3. Update violation state store
        if violation is not None:
            violation["frame_id"]        = frame_id
            violation["violation_count"] = violation_count
            violation["timestamp"]       = formatted_ts
            self._state_store.add_violation(violation)
            logger.info(
                "Violation recorded | id=%s  frame_id=%d  total=%d",
                violation["violation_id"], frame_id, violation_count,
            )
 
        # 4. Broadcast annotated frame to all WebSocket clients
        self._connection_manager.broadcast({
            "type":            "frame",
            "frame_id":        frame_id,
            "timestamp":       formatted_ts,
            "frame":           frame_b64,
            # "detections":      detections,
            "violation":       violation,
            "violation_count": violation_count,
        })
 
        logger.info("Broadcast frame_id=%d to WebSocket clients.", frame_id)

    def _create_consumer(self) -> Consumer:
        retries = 0
        while True:
            try:
                c = Consumer(
                    {
                        "bootstrap.servers":  self._bootstrap_servers,
                        "group.id":           self._group_id,
                        "auto.offset.reset":  "latest",
                        "enable.auto.commit": True,
                    }
                )
                c.subscribe([self._topic])
                logger.info("Result consumer subscribed to '%s'.", self._topic)
                return c
            except KafkaException as exc:
                retries += 1
                wait = min(2 ** retries, 30)
                logger.warning("Kafka not ready (attempt %d): %s — retry in %ds", retries, exc, wait)
                time.sleep(wait)

