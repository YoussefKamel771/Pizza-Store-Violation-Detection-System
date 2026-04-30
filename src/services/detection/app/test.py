"""
detection_manager.py
=====================
Synchronous detection manager.
Consumes frames from the broker, runs detection, publishes violations.
"""

from datetime import datetime, timezone
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


class DetectionManagerTest:
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

    def on_frame_received(self, frame: np.ndarray):
        """
        Run the full detection pipeline on a single BGR frame.
        Returns (tracked_detections, violations, annotated_frame).

        Called directly by run_local.py (no broker).
        Called inside start() for every Kafka message in production.
        """
        # 1. Detect objects: Hand, Person, Pizza, Scooper
        raw_detections = self.detector.detect(frame)

        # 2. Temporal association — assigns stable track_id per worker
        tracked_detections = self.tracker.update(raw_detections, frame)

        # 3. Violation logic engine
        violations = self.engine.process_frame(tracked_detections, 
                                               0, 
                                               datetime.now(timezone.utc), 
                                               "unkown")

        # 4. Persist + publish each violation
        for v in violations:
            logger.info(
                "Violation detected → frame_id=%d  track_id=%s",
                0, v.track_id,
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

        return tracked_detections, violations, None