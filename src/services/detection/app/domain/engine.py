import numpy as np
from typing import List, Optional
from infrastructure.roi_manager import RoiManager, ROI
from models.db_schemas.violation import ViolationModel
from datetime import datetime, timezone

class ScooperViolationEngine:
    def __init__(self, rois: List[ROI] = None, roi_manager: RoiManager = None, proximity_threshold=50):
        # Support both direct ROI list or RoiManager instance
        if roi_manager is not None:
            self.rois = roi_manager.rois
        elif rois is not None:
            self.rois = rois
        else:
            self.rois = []
            
        self.proximity_threshold = proximity_threshold
        self.worker_states = {}  # {worker_id: {"has_grabbed": False, "with_scooper": False}}

    def _is_in_roi(self, centroid) -> bool:
        """Check if centroid is inside any defined ROI using polygon containment."""
        # ROI.contains_point uses cv2.pointPolygonTest (handles any polygon shape)
        point = (int(centroid[0]), int(centroid[1]))
        for roi in self.rois:
            if roi.contains_point(point):
                return True
        return False

    def _get_centroid(self, bbox):
        """Compute centroid from bounding box."""
        x1, y1, x2, y2 = bbox
        return ((x1 + x2) / 2, (y1 + y2) / 2)

    def check_scooper_proximity(self, hand_bbox, detections):
        """Checks if a scooper is being held by the hand using Euclidean distance."""
        hand_center = self._get_centroid(hand_bbox)
        for d in detections:
            if d['label'] == 'scooper':
                dist = np.linalg.norm(np.array(hand_center) - np.array(d['centroid']))
                if dist < self.proximity_threshold:
                    return True
        return False

    def update(self, worker_id, hand_detection, all_detections):
        state = self.worker_states.get(worker_id, {"has_grabbed": False, "with_scooper": False})
        
        centroid = hand_detection['centroid']
        in_roi = self._is_in_roi(centroid)

        # 1. Hand enters ROI to grab ingredients
        if in_roi:
            state["has_grabbed"] = True
            if self.check_scooper_proximity(hand_detection['bbox'], all_detections):
                state["with_scooper"] = True
        
        # 2. Hand moves to Pizza area
        for d in all_detections:
            if d['label'] == 'pizza':
                dist_to_pizza = np.linalg.norm(np.array(centroid) - np.array(d['centroid']))
                if dist_to_pizza < 100 and state["has_grabbed"]:
                    if not state["with_scooper"]:
                        return "VIOLATION"  # Flagged as violation
                    else:
                        # Reset after successful compliant action
                        state = {"has_grabbed": False, "with_scooper": False}
        
        self.worker_states[worker_id] = state
        return "CLEAR"

    def process_frame(self, tracked_objects: list, frame_id: int, timestamp: float, video_source: str="unknown", rois: List[ROI] = None) -> tuple:
        """
        Process full frame and return all violations.
        Optional rois override for per-frame dynamic ROI updates.
        """
        # Temporarily use overridden ROIs if provided
        original_rois = self.rois
        if rois is not None:
            self.rois = rois
            
        violations = []
        
        
        for obj in tracked_objects:
            if obj['label'] == 'hand':
                status = self.update(obj['track_id'], obj, tracked_objects)
                if status == "VIOLATION":
                    violations.append(
                        ViolationModel(
                            track_id=obj['track_id'],
                            violation_type='SCOOPER_VIOLATION',
                            frame_path=f"./alerts/violation_{obj['track_id']}.jpg", # Can be set to actual frame path if needed
                            frame_id=frame_id,  # Include frame_id if available
                            timestamp=datetime.fromtimestamp(timestamp, tz=timezone.utc),  # Include timestamp if available
                            video_source=video_source,  # Include video source if available
                            roi_name=self.rois[0].name if self.rois else "unknown",  # Assuming single ROI for simplicity
                            detections=obj  # Store the detection info for reference
                    ))
        
        # Restore original ROIs
        self.rois = original_rois
        return violations

