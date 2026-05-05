"""
consumers/frame_consumer.py
============================
Background thread that reads raw frames from the `video-frames` Kafka topic
and hands them to the FrameSynchronizer.

Runs in a daemon thread started by main.py.
"""

import base64
import json
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from frame_synchronizer import FrameSynchronizer

logger = logging.getLogger(__name__)


class FrameConsumerThread(threading.Thread):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        synchronizer: FrameSynchronizer,
    ):
        super().__init__(name="kafka-frame-consumer", daemon=True)
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._group_id          = group_id
        self._synchronizer      = synchronizer
        self._stop_event        = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info("Frame consumer thread starting | topic=%s", self._topic)
        consumer = self._create_consumer()

        try:
            while not self._stop_event.is_set():
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Frame consumer error: %s", msg.error())
                    continue

                try:
                    raw      = json.loads(msg.value().decode("utf-8"))
                    frame_id = raw["frame_id"]

                    # logger.info("Received frame | id=%d  timestamp=%.3f", frame_id, raw["timestamp"])

                    # Decode base64 → raw JPEG bytes
                    jpeg_bytes = base64.b64decode(raw["frame"])

                    # Hand off to synchronizer (no ack needed — we use auto-commit
                    # here since dropping a streaming frame is acceptable)
                    self._synchronizer.put_frame(frame_id, jpeg_bytes)

                except Exception as exc:
                    logger.error(
                        "Failed to process frame at offset %d: %s", msg.offset(), exc
                    )

        finally:
            consumer.close()
            logger.info("Frame consumer thread stopped.")

    def _create_consumer(self) -> Consumer:
        retries = 0
        while True:
            try:
                c = Consumer(
                    {
                        "bootstrap.servers":  self._bootstrap_servers,
                        "group.id":           self._group_id,
                        "auto.offset.reset":  "latest",     # start from live frames
                        "enable.auto.commit": True,          # fire-and-forget for streaming
                    }
                )
                c.subscribe([self._topic])
                logger.info("Frame consumer subscribed to '%s'.", self._topic)
                return c
            except KafkaException as exc:
                retries += 1
                wait = min(2 ** retries, 30)
                logger.warning("Kafka not ready (attempt %d): %s — retry in %ds", retries, exc, wait)
                time.sleep(wait)