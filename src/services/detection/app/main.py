import logging

import cv2

from core.interfaces import IDetector, ITracker, IMessageBroker, IViolationRepository
from domain.engine import ScooperViolationEngine
from typing import List, Dict, Any
import numpy as np

logger = logging.getLogger(__name__)


class DetectionManager:
    def __init__(
        self, 
        detector: IDetector, 
        tracker: ITracker,
        broker: IMessageBroker, 
        repo: IViolationRepository,
        engine: ScooperViolationEngine
    ):
        self.detector = detector
        self.tracker = tracker
        self.broker = broker
        self.repo = repo
        self.engine = engine

    def on_frame_received(self, frame: np.ndarray):

        # 1. Detect objects (Hand, Person, Pizza, Scooper) 
        raw_detections = self.detector.detect(frame)

        # 2. Temporal Association (DeepSORT)
        # Adds 'track_id' to each detection to distinguish between workers 
        tracked_detections = self.tracker.update(raw_detections, frame) 
        
        # 3. Violation Logic Engine
        # Pass the whole list so the engine can check Hand vs. Scooper vs. Pizza
        violations = self.engine.process_frame(tracked_detections)
        
        # 4. Reporting
        for v in violations:
            logger.info(f"Violation detected: {v}")
            self.repo.save_violation(v)  
            cv2.imwrite(v.frame_path, frame)  # Save frame for reference 
            # self.broker.publish("alerts", v)
        
        # self.broker.publish("streaming_service", {"violations": violations})
        
        return tracked_detections, violations 

    def start(self):
        self.broker.subscribe(self.on_frame_received)