import cv2
import json
from services.detection.app.infrastructure.detector import YOLO11Detector
# from services.detection.app.infrastructure.deepsort_tracker import DeepSortTracker
from services.detection.app.infrastructure.byteTrack_tracker import ByteTrackTracker
from services.detection.app.domain.engine import ScooperViolationEngine
from services.detection.app.infrastructure.visualization import Visualizer
from services.detection.app.infrastructure.roi_manager import RoiManager   
from services.detection.app.core.interfaces import IDetector, ITracker, IMessageBroker, IViolationRepository
from services.detection.app.domain.engine import ScooperViolationEngine
import numpy as np
from typing import List, Dict, Any
import os

class DetectionManager:
    def __init__(
        self, 
        detector: IDetector, 
        tracker: ITracker,
        engine: ScooperViolationEngine,
    ):
        self.detector = detector
        self.tracker = tracker
        self.engine = engine

    def on_frame_received(self, frame: np.ndarray):

        # 1. Detect objects (Hand, Person, Pizza, Scooper) 
        raw_detections = self.detector.detect(frame)

        # 2. Temporal Association (DeepSORT)
        # Adds 'track_id' to each detection to distinguish between workers 
        tracked_detections = self.tracker.update(raw_detections, frame) 
        
        # # 3. Violation Logic Engine
        # # Pass the whole list so the engine can check Hand vs. Scooper vs. Pizza
        violations = self.engine.process_frame(tracked_detections)
        
        # # 4. Reporting
        for v in violations:
            print(f"Violation Detected: {v}")   

        return tracked_detections, violations     

    # def start(self):
    #     self.broker.subscribe(self.on_frame_received)

ROI_CONFIG_PATH = "config/rois.json"

def initialize_roi_manager(frame: np.ndarray, roi_manager: RoiManager) -> bool:
    """
    Load ROIs from JSON if exists, otherwise launch interactive drawing.
    Returns True if ROIs are ready.
    """
    # Try loading existing config first
    if os.path.exists(ROI_CONFIG_PATH):
        print(f"Found existing ROI config at {ROI_CONFIG_PATH}")
        success = roi_manager.load_rois_from_file(ROI_CONFIG_PATH)
        if success and len(roi_manager.rois) > 0:
            return True
    
    # No config found or empty - launch interactive drawing
    print("No ROI config found. Entering interactive drawing mode...")
    success = roi_manager.draw_rois_interactive(frame)
    
    if success:
        # Immediately save for next run
        roi_manager.save_rois_to_file(ROI_CONFIG_PATH)
        print(f"ROIs saved to {ROI_CONFIG_PATH}")
        return True
    
    return False
        
def run_local_test(video_path: str, model_path: str):
    detector = YOLO11Detector(model_path)
    # Switch to ByteTrack
    tracker = ByteTrackTracker(
        track_thresh=0.1,    # Lower this to catch blurry hands
        track_buffer=60,     # Keep the ID alive for 2 seconds of occlusion
        match_thresh=0.9     # Be strict about spatial overlap
    )
    visualizer = Visualizer()
    roi_manager = RoiManager()


    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Cannot open video: {video_path}")
    
    # Read first frame for ROI setup
    ret, first_frame = cap.read()
    if not ret:
        raise ValueError("Cannot read first frame")
    
    # Initialize ROIs (load from JSON or draw interactively)
    if not initialize_roi_manager(first_frame, roi_manager):
        print("Failed to initialize ROIs. Exiting.")
        cap.release()
        return
    
    # Pass loaded ROIs to engine
    engine = ScooperViolationEngine(roi_manager=roi_manager)
 

    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            manager = DetectionManager(detector, tracker, engine)
            
            tracked_objs, violations = manager.on_frame_received(frame)

            # Draw all ROIs from manager
            frame = visualizer.draw_frame(frame.copy(), tracked_objs, roi_manager, violations)

            cv2.imshow("Pizza Store Debugger", frame)
            key = cv2.waitKey(1) & 0xFF
            if key == ord('q'):
                break
            elif key == ord('s'):
                # Manual save trigger
                roi_manager.save_rois_to_file(ROI_CONFIG_PATH)
                print("ROIs manually saved")
    finally:
        # Ensure ROIs are saved even if user closes window
        roi_manager.save_rois_to_file(ROI_CONFIG_PATH)
        cap.release()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    run_local_test(video_path='test_data/Sah w b3dha ghalt (3).mp4', model_path='weights/best.pt')

