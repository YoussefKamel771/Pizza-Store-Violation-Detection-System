import logging

import cv2
import numpy as np

from infrastructure.roi_manager import RoiManager

logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self):
        # Store last N positions to draw trails
        self.history = {} 
        self.max_history = 20
        # Generate stable colors for IDs
        self.colors = np.random.randint(0, 255, size=(100, 3), dtype=np.uint8)

    def draw_frame(self, frame, tracked_objects, roi_manager: RoiManager, violations=None):
        """
        Main drawing function.
        """
        # 1. Draw ROIs (Regions of Interest)
        for roi in roi_manager.rois:
            # logger.debug("Drawing ROI %s with color %s", roi.name, roi.color)
            roi.draw(frame)

        # 2. Draw Tracked Objects
        for obj in tracked_objects:
            try:
                tid = int(obj['track_id'])
            except (ValueError, TypeError):
                # Fallback if track_id is somehow non-numeric
                tid = 0
            color = [int(c) for c in self.colors[tid % 100]]
            x1, y1, x2, y2 = map(int, obj['bbox'])
            label = obj['label']

            # Draw Bounding Box
            cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
            
            # Draw ID and Label
            header = f"{label.upper()} #{tid}"
            cv2.putText(frame, header, (x1, y1 - 10), 
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)

            # 3. Draw Movement Trails (for Hand tracking)
            if label == 'hand':
                self._draw_trails(frame, tid, obj['centroid'], color)

        # 4. Global Violation Overlay
        if violations:
            cv2.rectangle(frame, (0, 0), (frame.shape[1], 40), (0, 0, 255), -1)
            cv2.putText(frame, f"VIOLATION ALERT: {len(violations)} ACTIVE", (10, 30), 
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)

        return frame

    def _draw_trails(self, frame, tid, centroid, color):
        if tid not in self.history:
            self.history[tid] = []
        
        self.history[tid].append(centroid)
        if len(self.history[tid]) > self.max_history:
            self.history[tid].pop(0)

        for i in range(1, len(self.history[tid])):
            pt1 = tuple(map(int, self.history[tid][i-1]))
            pt2 = tuple(map(int, self.history[tid][i]))
            thickness = int(np.sqrt(self.max_history / float(i + 1)) * 2)
            cv2.line(frame, pt1, pt2, color, thickness)