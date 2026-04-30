"""
detection_manager.py
=====================
Synchronous detection manager.
Consumes frames from the broker, runs detection, publishes violations.
"""

import logging

import cv2
import numpy as np

from core.interfaces import (
    IDetector,
    ITracker,
    IFrameConsumer,
    IViolationPublisher,
    IViolationRepository,
)
from domain.engine import ScooperViolationEngine
from infrastructure.roi_manager import RoiManager
from infrastructure.visualization import Visualizer

logger = logging.getLogger(__name__)


class DetectionManager:
    def __init__(
        self,
        detector:            IDetector,
        tracker:             ITracker,
        broker:              IFrameConsumer | None,   # None when running locally
        repo:                IViolationRepository,
        engine:              ScooperViolationEngine,
        violation_publisher: IViolationPublisher | None = None,  # None in local mode
        roi_manager:         RoiManager | None = None,         # Optional for visualization
        visualizer:          Visualizer | None = None,        # Optional for visualization
    ):
        self.detector            = detector
        self.tracker             = tracker
        self.broker              = broker
        self.repo                = repo
        self.engine              = engine
        self.violation_publisher = violation_publisher
        self.roi_manager         =  roi_manager 
        self.visualizer          =  visualizer

    # ── Core per-frame logic ──────────────────────────────────────────────────

    def on_frame_received(self, payload: dict):
        """
        Run the full detection pipeline on a single BGR frame.
        Returns (tracked_detections, violations, annotated_frame).

        Called directly by run_local.py (no broker).
        Called inside start() for every Kafka message in production.
        """
        # 1. Detect objects: Hand, Person, Pizza, Scooper
        frame = payload["frame"]     # BGR numpy array
        raw_detections = self.detector.detect(frame)

        # 2. Temporal association — assigns stable track_id per worker
        tracked_detections = self.tracker.update(raw_detections)

        # logger.info("Received payload keys: %s", payload.keys())
        # 3. Violation logic engine
        violations = self.engine.process_frame(tracked_detections, 
                                               payload["frame_id"], 
                                               payload["timestamp"], 
                                               payload.get("source", "unknown"),)

        # 4. Persist + publish each violation
        for v in violations:
            logger.info(
                "Violation detected → frame_id=%d  track_id=%s",
                payload["frame_id"], v.track_id,
            )

            # Save the violation frame image to disk
            cv2.imwrite(v.frame_path, frame)

            # Persist metadata to Postgres
            if self.repo is not None:
                self.repo.save_violation(v)

            # Publish to Kafka violations topic (skip in local mode)
            if self.violation_publisher is not None:
                self.violation_publisher.publish(v)

        if self.visualizer is not None:
            annotated_frame = self.visualizer.draw_frame(
                frame.copy(), tracked_detections, self.roi_manager, violations
            )

        return tracked_detections, violations, annotated_frame

    # ── Production broker loop ────────────────────────────────────────────────

    def start(self) -> None:
        """
        Connect to Kafka, consume frames, and run detection indefinitely.
        Blocks the calling thread until KeyboardInterrupt or an unrecoverable error.

        This is the production entry point (called by main.py).
        """
        if self.broker is None:
            raise RuntimeError(
                "No broker configured. Pass a KafkaFrameConsumer to use start()."
            )

        logger.info("DetectionManager starting — connecting to Kafka frame topic…")

        with self.broker:                       # connect() on enter, disconnect() on exit
            with self.violation_publisher:      # connect() / disconnect()
                for msg in self.broker.subscribe():
                    try:
                        payload  = msg.payload

                        logger.debug("Processing frame_id=%d", payload["frame_id"])

                        _, violations, display = self.on_frame_received(payload)

                        # Overlay running violation count
                        if self.visualizer is not None and display is not None:
                            cv2.putText(display, f"Violations: {len(violations)}", (15, 35), cv2.FONT_HERSHEY_SIMPLEX,
                                1.0,(0, 0, 255),2,cv2.LINE_AA,)

                            cv2.imshow("Pizza Store — Local Debug", display)
                            key = cv2.waitKey(1) & 0xFF

                        # Commit offset only after successful processing
                        msg.acknowledge()

                    except KeyboardInterrupt:
                        logger.info("KeyboardInterrupt — stopping detection loop.")
                        break

                    except Exception as exc:
                        logger.exception(
                            "Unhandled error on frame_id=%s: %s",
                            msg.payload.get("frame_id", "?"), exc,
                        )
                        # Ack anyway so a persistently bad frame doesn't block the queue
                        msg.acknowledge()

        logger.info("DetectionManager stopped.")