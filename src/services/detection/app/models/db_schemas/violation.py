from sqlalchemy import Column, Integer, String, DateTime, JSON, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class ViolationModel(Base):
    """SQLAlchemy model for hygiene violations."""
    __tablename__ = 'violations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    track_id = Column(Integer, nullable=False)
    violation_type = Column(String(50), default='SCOOPER_VIOLATION')
    timestamp = Column(DateTime, default=datetime.utcnow)
    frame_path = Column(String(255), nullable=True)
    detections = Column(JSON, nullable=True)  # Stores the list of tracked objects
    is_resolved = Column(Boolean, default=False)

    def __repr__(self):
        return f"<Violation(id={self.id}, track_id={self.track_id}, type='{self.violation_type}')>"