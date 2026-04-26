from abc import ABC, abstractmethod
import numpy as np
from typing import List, Dict, Any

from models.db_schemas.violation import ViolationModel

class IDetector(ABC):
    """Interface for object detection models (e.g., YOLO 12)"""
    @abstractmethod
    def detect(self, frame: np.ndarray) -> List[Dict[str, Any]]:
        pass

class ITracker(ABC):
    """Interface for multi-object tracking logic."""
    @abstractmethod
    def update(self, detections: List[Dict[str, Any]], frame: Any) -> List[Dict[str, Any]]:
        """
        Updates the tracker with new detections.
        Returns detections updated with persistent 'track_id'.
        """
        pass

class IMessageBroker(ABC):
    """Interface for communication between microservices"""
    @abstractmethod
    def subscribe(self, callback_fn):
        pass

    @abstractmethod
    def publish(self, topic: str, message: Dict[str, Any]):
        pass

class IViolationRepository(ABC):
    """Interface for persisting violation data to a database or storage system."""
    @abstractmethod
    def save_violation(self, violation_data: ViolationModel):
        pass
