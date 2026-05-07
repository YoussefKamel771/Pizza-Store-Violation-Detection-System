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
    IConsumerPort,
    IPublisherPort,
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
        broker:              IConsumerPort,   # None when running locally
        repo:                IViolationRepository,
        engine:              ScooperViolationEngine,
        visualizer:          Visualizer,        # Optional for visualization
        result_publisher:   IPublisherPort | None = None,  # None in local mode
        roi_manager:         RoiManager | None = None,         # Optional for visualization
        vis: bool = False,  # Whether to show visualization window (local mode only)
    ):
        self.detector            = detector
        self.tracker             = tracker
        self.broker              = broker
        self.repo                = repo
        self.engine              = engine
        self.result_publisher = result_publisher
        self.roi_manager         =  roi_manager 
        self.visualizer          =  visualizer
        self.vis                 =  vis
        self._violation_count = 0

    # ── Core per-frame logic ──────────────────────────────────────────────────

    def on_frame_received(
        self,
        frame: np.ndarray,
        frame_id: int = -1,
        timestamp: float | None = None,
    ):
        """
        Run the full detection pipeline on a single BGR frame.
        Returns (tracked_detections, violations, annotated_frame).

        Called directly by run_local.py (no broker).
        Called inside start() for every Kafka message in production.
        """
        # 1. Detect objects: Hand, Person, Pizza, Scooper
        raw_detections = self.detector.detect(frame)

        # 2. Temporal association — assigns stable track_id per worker
        tracked_detections = self.tracker.update(raw_detections)

        # logger.info("Received payload keys: %s", payload.keys())
        # 3. Violation logic
        violations = self.engine.process_frame(tracked_detections, frame_id, timestamp)

        # logger.info("violations = %s", violations)
        
        violation = violations[0] if violations else None

        # logger.info("violation = %s", violation)

        if violation is not None:
            self._violation_count += 1
            logger.info(
                "Violation #%d detected | frame_id=%d  track_id=%s  roi=%s",
                self._violation_count, frame_id, violation.track_id, violation.roi_name,
            )
            # Save violation frame to disk
            cv2.imwrite(violation.frame_path, frame)
            # Persist to Postgres
            self.repo.save_violation(violation)

        if self.visualizer is not None:
            annotated_frame = self.visualizer.draw_frame(
                frame.copy(), tracked_detections, self.roi_manager, violation, self._violation_count, 
            )

        # 5. Publish to detection-results (every frame, not just violations)
        if self.result_publisher is not None:
            self.result_publisher.publish(
            frame=annotated_frame,
            frame_id=frame_id,
            timestamp=timestamp,
            # detections=tracked_detections,
            violation=violation,
            violation_count=self._violation_count,
        )


        return  annotated_frame

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
            with self.result_publisher:      # connect() / disconnect()
                for msg in self.broker.subscribe():
                    try:
                        payload   = msg.payload
                        frame     = payload["frame"]        # BGR numpy array
                        frame_id  = payload["frame_id"]
                        timestamp = payload["timestamp"]
 
                        logger.debug("Processing frame_id=%d", frame_id)

                        display =  self.on_frame_received(frame, frame_id=frame_id, timestamp=timestamp)

                        # Overlay running violation count
                        if self.vis:

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