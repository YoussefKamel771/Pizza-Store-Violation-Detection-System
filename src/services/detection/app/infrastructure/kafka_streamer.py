"""
infrastructure/violation_publisher.py
======================================
Synchronous Kafka violation publisher implementing IViolationPublisher.

Violation message schema (JSON):
{
    "violation_id": str,
    "frame_id":     int,
    "timestamp":    float,
    "frame_path":   str,
    "track_id":     int,
    "roi_name":       str,
    "boxes": [
        {"label": str, "confidence": float, "x1": int, "y1": int, "x2": int, "y2": int}
    ]
}
"""

import json
import logging
from typing import Any

from confluent_kafka import Producer, KafkaException

from core.interfaces import IViolationPublisher

logger = logging.getLogger(__name__)


class KafkaViolationPublisher(IViolationPublisher):
    """
    Publishes violation events to the 'violations' Kafka topic so the
    Streaming Service can forward them to the frontend in real-time.
    """

    def __init__(self, bootstrap_servers: str, topic: str = "violations") -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._producer: Producer | None = None

    # ── IViolationPublisher ───────────────────────────────────────────────────

    def connect(self) -> None:
        logger.info(
            "Connecting Kafka violation publisher → servers=%s  topic=%s",
            self._bootstrap_servers, self._topic,
        )
        self._producer = Producer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "acks":              "1",
                "retries":           3,
                "retry.backoff.ms":  300,
            }
        )
        logger.info("Kafka violation publisher ready.")

    def disconnect(self) -> None:
        if self._producer:
            remaining = self._producer.flush(timeout=10.0)
            if remaining:
                logger.warning(
                    "%d violation message(s) NOT delivered before shutdown.", remaining
                )
            else:
                logger.info("All violation messages flushed.")

    def publish(self, violation: Any) -> None:
        """
        Serialize a domain Violation object and send it to Kafka.
        The violation object must have these attributes:
            violation_id, frame_id, timestamp, frame_path,
            track_id, roi_id, detections (list of dicts)
        """
        if self._producer is None:
            raise RuntimeError("Call connect() before publish().")

        try:
            # logger.info("violation: %s", violation.detections)
            payload = {
                "violation_id": str(violation.id),
                "frame_id":     violation.frame_id,
                "timestamp":    violation.timestamp.isoformat() if hasattr(violation.timestamp, 'isoformat') else violation.timestamp,
                "frame_path":   violation.frame_path,
                "track_id":     violation.track_id,
                "roi_name":       str(violation.roi_name),
                "boxes": 
                    {
                        "label":      violation.detections["label"],
                        "confidence": round(float(violation.detections["confidence"]), 3),
                        "x1":         int(violation.detections["bbox"][0]),
                        "y1":         int(violation.detections["bbox"][1]),
                        "x2":         int(violation.detections["bbox"][2]),
                        "y2":         int(violation.detections["bbox"][3]),
                    },
            }

            self._producer.produce(
                self._topic,
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            logger.info(
                "Published violation → frame_id=%d  track_id=%d  roi=%s",
                violation.frame_id, violation.track_id, violation.roi_name,
            )
            # Non-blocking poll to trigger delivery callbacks
            self._producer.poll(0)

        except KafkaException as exc:
            logger.error("Failed to publish violation: %s", exc)

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            logger.error("Violation delivery failed: %s", err)
        else:
            logger.debug(
                "Violation delivered → topic=%s  partition=%d  offset=%d",
                msg.topic(), msg.partition(), msg.offset(),
            )