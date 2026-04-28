import json
import logging
from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


class FrameProducer:
    """
    Thin wrapper around confluent-kafka Producer.
    Serialises frame metadata + base64 JPEG as a JSON message
    and publishes it to the configured Kafka topic.
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                # Wait for the leader to acknowledge before continuing
                "acks": "1",
                # Batch up to 64 KB before flushing (reduces network round-trips)
                "batch.size": 65536,
                # Wait up to 5 ms to fill the batch
                "linger.ms": 5,
                # Retry on transient failures
                "retries": 3,
                "retry.backoff.ms": 300,
                "queue.buffering.max.messages": 50,
                "queue.buffering.max.kbytes": 65536,   # 64 MB max
            }
        )
        logger.info(
            "KafkaProducer initialised → servers=%s  topic=%s",
            bootstrap_servers,
            topic,
        )

    # ── Internal delivery callback ────────────────────────────────────────────
    @staticmethod
    def _on_delivery(err, msg):
        if err:
            logger.error("Delivery failed for frame: %s", err)
        else:
            logger.debug(
                "Frame delivered → topic=%s  partition=%d  offset=%d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    # ── Public API ────────────────────────────────────────────────────────────
    def publish(self, frame_message: dict) -> None:
        """Serialise `frame_message` to JSON and send to Kafka."""
        try:
            value = json.dumps(frame_message).encode("utf-8")
            self._producer.produce(
                self.topic,
                value=value,
                on_delivery=self._on_delivery,
            )
            # Non-blocking poll to trigger delivery callbacks without stalling
            self._producer.poll(0)
        except KafkaException as exc:
            logger.error("Failed to produce message: %s", exc)

    def poll_events(self, timeout: float = 0.01) -> None:
        """
        Drain Kafka's internal delivery callback queue.
        Call this every N frames to release memory held by delivered messages.
        """
        self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all queued messages are delivered (or timeout)."""
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.warning("%d message(s) were NOT delivered before flush timeout.", remaining)
        else:
            logger.info("All messages flushed successfully.")