import base64
import json
import logging
import time

import cv2
import numpy as np
from confluent_kafka import Consumer, KafkaError, KafkaException

from core.interfaces import IConsumerPort, BrokerMessage

logger = logging.getLogger(__name__)


class KafkaFrameConsumer(IConsumerPort):
    """
    Reads frames from the 'video-frames' Kafka topic.

    Each yielded BrokerMessage has:
        msg.payload = {
            "frame_id":  int,
            "timestamp": float,
            "frame":     np.ndarray   # BGR image, decoded from base64 JPEG
        }
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str          = "detection-service",
        auto_offset_reset: str = "latest",
        poll_timeout_s: float  = 1.0,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._group_id          = group_id
        self._auto_offset_reset = auto_offset_reset
        self._poll_timeout      = poll_timeout_s
        self._consumer: Consumer | None = None

    # ── IConsumerPort ────────────────────────────────────────────────────────

    def connect(self) -> None:
        logger.info(
            "Connecting Kafka consumer | servers=%s  topic=%s  group=%s",
            self._bootstrap_servers, self._topic, self._group_id,
        )
        self._consumer = Consumer(
            {
                "bootstrap.servers":  self._bootstrap_servers,
                "group.id":           self._group_id,
                "auto.offset.reset":  self._auto_offset_reset,
                # Manual commit so we only ack after successful processing
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([self._topic])

        # Wait until Kafka is reachable (retry with back-off)
        retries = 0
        while True:
            msg = self._consumer.poll(timeout=2.0)
            if msg is None:
                retries += 1
                wait = min(2 ** retries, 30)
                logger.warning(
                    "Kafka not ready yet (attempt %d) — retrying in %ds…",
                    retries, wait,
                )
                time.sleep(wait)
                continue
            if msg.error():
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    retries += 1
                    time.sleep(min(2 ** retries, 30))
                    continue
                raise KafkaException(msg.error())
            # Got a real message — process it in subscribe() instead
            self._buffered = msg
            break

        logger.info("Kafka consumer connected and subscribed to '%s'.", self._topic)
        self._buffered = None   # cleared after first successful poll above

    def disconnect(self) -> None:
        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer disconnected.")

    def subscribe(self):
        """
        Yield BrokerMessages indefinitely until the loop is broken externally
        (e.g. KeyboardInterrupt or a break in the caller).
        """
        if self._consumer is None:
            raise RuntimeError("Call connect() before subscribe().")

        while True:
            msg = self._consumer.poll(timeout=self._poll_timeout)

            if msg is None:
                continue    # no message in this poll window — keep waiting

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue    # reached end of partition — normal for file-based sources
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            try:
                raw   = json.loads(msg.value().decode("utf-8"))
                frame = _decode_frame(raw["frame"])
                payload = {
                    "frame_id":  raw["frame_id"],
                    "timestamp": raw["timestamp"],
                    "frame":     frame,
                }
            except Exception as exc:
                logger.error(
                    "Failed to decode frame at offset %d: %s — skipping.",
                    msg.offset(), exc,
                )
                self._consumer.commit(message=msg)
                continue

            consumer_ref = self._consumer

            def _ack(m=msg):
                consumer_ref.commit(message=m)

            yield BrokerMessage(
                message_id=f"{msg.topic()}-{msg.partition()}-{msg.offset()}",
                payload=payload,
                ack_fn=_ack,
            )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _decode_frame(b64_string: str) -> np.ndarray:
    """Decode a base64-encoded JPEG string back to a BGR numpy array."""
    jpeg_bytes = base64.b64decode(b64_string)
    arr        = np.frombuffer(jpeg_bytes, dtype=np.uint8)
    frame      = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        raise ValueError("cv2.imdecode returned None — corrupt JPEG?")
    return frame