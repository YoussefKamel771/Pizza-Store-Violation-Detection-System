"""
consumers/result_consumer.py
=============================
Background thread that reads detection metadata from `detection-results`
and hands it to the FrameSynchronizer.

Also updates StateStore whenever a violation is present in the message.
"""

import json
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from frame_synchronizer import FrameSynchronizer
from state_store import StateStore

logger = logging.getLogger(__name__)


class ResultConsumerThread(threading.Thread):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        synchronizer: FrameSynchronizer,
        state_store: StateStore,
    ):
        super().__init__(name="kafka-result-consumer", daemon=True)
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._group_id          = group_id
        self._synchronizer      = synchronizer
        self._state_store       = state_store
        self._stop_event        = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info("Result consumer thread starting | topic=%s", self._topic)
        consumer = self._create_consumer()

        try:
            while not self._stop_event.is_set():
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Result consumer error: %s", msg.error())
                    continue

                try:
                    result   = json.loads(msg.value().decode("utf-8"))
                    frame_id = result["frame_id"]

                    # Update violation state if this frame had one
                    violation = result.get("violation")
                    if violation is not None:
                        # Attach frame_id so StateStore can record it
                        violation["frame_id"]       = frame_id
                        violation["violation_count"] = result.get("violation_count", 0)
                        self._state_store.add_violation(violation)
                        logger.info(
                            "Violation recorded | id=%s  frame_id=%d  total=%d",
                            violation["violation_id"], frame_id,
                            result.get("violation_count", "?"),
                        )

                    # Hand full result to synchronizer
                    self._synchronizer.put_result(frame_id, result)

                except Exception as exc:
                    logger.error(
                        "Failed to process result at offset %d: %s", msg.offset(), exc
                    )

        finally:
            consumer.close()
            logger.info("Result consumer thread stopped.")

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