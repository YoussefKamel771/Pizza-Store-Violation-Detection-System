import numpy as np
import supervision as sv
from core.interfaces import ITracker

class ByteTrackTracker(ITracker):
    """
    ByteTrack implementation using the supervision library.
    ByteTrack associates high-confidence detections first, then associates 
    low-confidence detections in a second pass to prevent lost tracks.
    """
    def __init__(self, track_thresh: float = 0.25, match_thresh: float = 0.8, track_buffer: int = 30):
        """
        Args:
            track_thresh: Minimum confidence for a detection to be considered for tracking.
            match_thresh: IoU threshold for matching detections to existing tracks.
            track_buffer: Number of frames to keep a track alive without detections.
        """
        self.tracker = sv.ByteTrack(
            track_activation_threshold=track_thresh, # Lower this to catch blurry hands
            minimum_matching_threshold=match_thresh, # Keep the ID alive for 2 seconds of occlusion
            lost_track_buffer=track_buffer           # Be strict about spatial overlap
        )
        
        # ByteTrack (via supervision) requires integer class IDs, not strings.
        # We must map the YOLO string labels to integers internally.
        self.classes = ['hand', 'person', 'pizza', 'scooper']
        self.label_to_id = {label: idx for idx, label in enumerate(self.classes)}
        self.id_to_label = {idx: label for label, idx in self.label_to_id.items()}

    def update(self, detections: list) -> list:
        if not detections:
            # Supervision requires an empty Detections object if no objects are found
            sv_dets = sv.Detections.empty()
        else:
            # 1. Extract data from our standard detection format
            xyxy = np.array([d['bbox'] for d in detections])
            confidence = np.array([d['conf'] for d in detections])
            
            # 2. Map string labels to integer class IDs
            class_id = np.array([self.label_to_id.get(d['label'], -1) for d in detections])
            
            # 3. Create Supervision Detections object
            sv_dets = sv.Detections(
                xyxy=xyxy,
                confidence=confidence,
                class_id=class_id
            )

        # 4. Update tracks (ByteTrack is motion-based, so the frame is not strictly needed,
        # but we pass it to maintain the same interface signature as DeepSort)
        tracked_sv_dets = self.tracker.update_with_detections(sv_dets)

        # 5. Format output to match the exact structure expected by the ViolationEngine
        tracked_objects = []
        
        # Ensure tracker_id exists (supervision returns None if empty)
        if tracked_sv_dets.tracker_id is None:
            return tracked_objects

        for i in range(len(tracked_sv_dets)):
            track_id = tracked_sv_dets.tracker_id[i]
            
            # Supervision uses -1 for unconfirmed/unmatched objects. Skip them.
            if track_id == -1:
                continue
            
            bbox = tracked_sv_dets.xyxy[i].tolist()
            class_id = tracked_sv_dets.class_id[i]
            confidence = tracked_sv_dets.confidence[i]
            
            # Map integer ID back to string label
            label = self.id_to_label.get(class_id, "unknown")
            
            tracked_objects.append({
                "track_id": int(track_id),
                "bbox": bbox, # [x1, y1, x2, y2]
                "label": label,
                "confidence": confidence,
                "centroid": self._get_centroid(bbox)
            })
            
        return tracked_objects

    def _get_centroid(self, bbox):
        x1, y1, x2, y2 = bbox
        return ((x1 + x2) / 2, (y1 + y2) / 2)


