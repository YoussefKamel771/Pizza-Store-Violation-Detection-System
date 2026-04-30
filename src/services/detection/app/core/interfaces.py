from abc import ABC, abstractmethod
import numpy as np
from typing import AsyncIterator, Iterator, List, Dict, Any

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
 
class IViolationRepository(ABC):
    @abstractmethod
    def save_violation(self, violation: Any) -> None:
        """Persist a violation record to the database."""
 
    @abstractmethod
    def get_violations(self, limit: int = 100) -> List[Any]:
        """Retrieve recent violations from the database."""

class BrokerMessage:
    """
    Thin wrapper so the domain never knows about aio_pika IncomingMessage
    or aiokafka ConsumerRecord internals.
    """
    __slots__ = ("message_id", "payload", "_ack_fn")

    def __init__(self, message_id: str, payload: bytes, ack_fn) -> None:
        self.message_id = message_id
        self.payload = payload
        self._ack_fn = ack_fn

    def acknowledge(self) -> None:
        self._ack_fn()


# ── Frame Consumer (reads frames FROM broker) ─────────────────────────────────
 
class IFrameConsumer(ABC):
    """
    Synchronous context-manager-compatible frame consumer.
 
    Usage:
        with consumer:
            for msg in consumer.subscribe():
                frame = msg.payload["frame"]
                ...
                msg.acknowledge()
    """
 
    @abstractmethod
    def connect(self) -> None:
        """Establish connection and subscribe to the frames topic."""
 
    @abstractmethod
    def disconnect(self) -> None:
        """Gracefully close the connection."""
 
    @abstractmethod
    def subscribe(self) -> Iterator[BrokerMessage]:
        """
        Yield one BrokerMessage per frame.
        Implementations must NOT auto-ack; the caller acks after processing.
        """
 
    def __enter__(self) -> "IFrameConsumer":
        self.connect()
        return self
 
    def __exit__(self, *_) -> None:
        self.disconnect()


# ── Violation Publisher (writes violations TO broker) ─────────────────────────
 
class IViolationPublisher(ABC):
    """
    Synchronous violation publisher.
 
    Usage:
        with publisher:
            publisher.publish(violation, frame_id=frame_id)
    """
 
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the broker."""
 
    @abstractmethod
    def disconnect(self) -> None:
        """Flush pending messages and close the connection."""
 
    @abstractmethod
    def publish(self, violation: Any) -> None:
        """Serialize and send a single violation event to the broker."""
 
    def __enter__(self) -> "IViolationPublisher":
        self.connect()
        return self
 
    def __exit__(self, *_) -> None:
        self.disconnect()