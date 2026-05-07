
import base64
import json
import logging
from typing import Any, Optional

from confluent_kafka import Producer, KafkaException
import cv2
import numpy as np
from core.interfaces import IPublisherPort

logger = logging.getLogger(__name__)


class KafkaStreamer(IPublisherPort):
    """
    Publishes detection results to the 'detections' Kafka topic so the
    Streaming Service can forward them to the frontend in real-time.
    """

    def __init__(self, bootstrap_servers: str, topic: str = "detection-results") -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._producer: Producer | None = None

    # ── IPublisherPort ────────────────────────────────────────────────────────

    def connect(self) -> None:
        logger.info(
            "Connecting Kafka detection publisher | servers=%s  topic=%s",
            self._bootstrap_servers, self._topic,
        )
        self._producer = Producer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "acks":              "1",
                "retries":           3,
                "retry.backoff.ms":  300,
                # Cap buffer so RAM doesn't grow if the streaming service is slow
                "queue.buffering.max.messages": 100,
                "queue.buffering.max.kbytes":   131072 ,  # 128 MB
                "linger.ms":                    5,
            }
        )
        logger.info("Kafka detection publisher ready.")

    def disconnect(self) -> None:
        if self._producer:
            remaining = self._producer.flush(timeout=10.0)
            if remaining:
                logger.warning(
                    "%d detection message(s) NOT delivered before shutdown.", remaining
                )
            else:
                logger.info("All detection messages flushed.")

    def publish(
        self,
        frame: np.ndarray,
        frame_id: int,
        timestamp: float,
        # detections: list,
        violation,          # domain Violation object or None
        violation_count: int,
    ) -> None:
        """
        Serialize a domain Detection object and send it to Kafka.
        The detection object must have these attributes:
            detection_id, frame_id, timestamp, frame_path,
            track_id, roi_id, detections (list of dicts)
        """
        if self._producer is None:
            raise RuntimeError("Call connect() before publish().")

        try:
            # logger.info("detection: %s", detections)
            payload = {
                "frame_id":        frame_id,
                "timestamp":       timestamp,
                "frame":           self.encode_frame(frame),
                # "detections":      self._serialize_detections(detections),
                "violation":       self._serialize_violation(violation),
                "violation_count": violation_count,
            }

            self._producer.produce(
                self._topic,
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            if violation is not None:
                # logger.info(
                #     "Published violation | frame_id=%d  track_id=%d  roi=%s",
                #     violation.frame_id, violation.track_id, violation.roi_name,
                # )
                logger.info("published violations = %s", violation)
            else:
                logger.info("Published frame %d — no violation.  violation=%s violation_count=%d",
                             frame_id, violation, violation_count)
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
                "Violation delivered | topic=%s  partition=%d  offset=%d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    @staticmethod
    def encode_frame(frame, quality: int = 70) -> str:
        """JPEG-compress and base64-encode a frame. Frees intermediate buffer immediately."""
        success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
        if not success:
            raise RuntimeError("cv2.imencode failed")
        encoded = base64.b64encode(buffer).decode("utf-8")
        del buffer      # free raw JPEG bytes right away
        return encoded

    # ── Serialisation helpers ─────────────────────────────────────────────────────
    @staticmethod
    def _serialize_violation(violation) -> Optional[dict]:
        """
        Serialize a domain Violation object.
        Returns None (JSON null) if no violation occurred this frame.
        """
        if violation is None:
            return None
        return {
            "violation_id": str(violation.id),
            "track_id":     int(violation.track_id),
            "frame_id":     int(violation.frame_id),
            "roi_id":       str(violation.roi_name),
        }
 
    # @staticmethod
    # def _serialize_detections(detections: list) -> list:
    #     """
    #     Convert tracked detection dicts to a clean, JSON-safe list.
    #     Each dict is expected to have at minimum:
    #         track_id, label, confidence, x1, y1, x2, y2
    #     and optionally:
    #         in_roi (bool) — set by the engine when a hand enters an ROI
    #     """
    #     result = []
    #     for d in detections:
    #         result.append({
    #             "track_id":   int(d.get("track_id",   -1)),
    #             "label":      str(d.get("label",       "")),
    #             "confidence": round(float(d.get("confidence", 0.0)), 3),
    #             "x1":         int(d["bbox"][0]),
    #             "y1":         int(d["bbox"][1]),
    #             "x2":         int(d["bbox"][2]),
    #             "y2":         int(d["bbox"][3]),
    #             "in_roi":     bool(d.get("in_roi", False)),
    #         })
    #         # logger.info(f"detections serialized = {result[-1]}")
    #     return result
 