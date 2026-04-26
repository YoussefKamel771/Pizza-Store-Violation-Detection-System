import cv2
import json
# from services.detection.app.infrastructure.deepsort_tracker import DeepSortTracker
from infrastructure.detector import YOLO11Detector
from infrastructure.byteTrack_tracker import ByteTrackTracker
from domain.engine import ScooperViolationEngine
from infrastructure.visualization import Visualizer
from infrastructure.roi_manager import RoiManager   
from core.interfaces import IDetector, ITracker, IMessageBroker, IViolationRepository
from domain.engine import ScooperViolationEngine
from infrastructure.postgress_repo import PostgresRepository
from helpers.config import get_settings, Settings
from core.logging_config import setup_logger
from main import DetectionManager
import numpy as np
from typing import List, Dict, Any
import os
import logging
import cv2

setup_logger()

logger = logging.getLogger(__name__)

settings = get_settings()
logger.info(f"Loaded settings: {settings.model_dump()}")


def initialize_roi_manager(frame: np.ndarray, roi_manager: RoiManager) -> bool:
    """
    Load ROIs from JSON if exists, otherwise launch interactive drawing.
    Returns True if ROIs are ready.
    """
    # Try loading existing config first
    if os.path.exists(settings.roi_config_path):
        print(f"Found existing ROI config at {settings.roi_config_path}")
        success = roi_manager.load_rois_from_file(settings.roi_config_path)
        if success and len(roi_manager.rois) > 0:
            return True
    
    # No config found or empty - launch interactive drawing
    print("No ROI config found. Entering interactive drawing mode...")
    success = roi_manager.draw_rois_interactive(frame)
    
    if success:
        # Immediately save for next run
        roi_manager.save_rois_to_file(settings.roi_config_path)
        print(f"ROIs saved to {settings.roi_config_path}")
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
    repo = PostgresRepository(settings.conn_str)


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

    manager = DetectionManager(detector, tracker, engine, repo)

 

    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            
            tracked_objs, violations = manager.on_frame_received(frame)

            # Draw all ROIs from manager
            frame = visualizer.draw_frame(frame.copy(), tracked_objs, roi_manager, violations)

            cv2.imshow("Pizza Store Debugger", frame)
            key = cv2.waitKey(1) & 0xFF
            if key == ord('q'):
                break
            elif key == ord('s'):
                # Manual save trigger
                roi_manager.save_rois_to_file(settings.roi_config_path)
                print("ROIs manually saved")
    finally:
        # Ensure ROIs are saved even if user closes window
        roi_manager.save_rois_to_file(settings.roi_config_path)
        cap.release()
        cv2.destroyAllWindows()


if __name__ == "__main__":
    run_local_test(video_path=settings.test_video_path, model_path=settings.model_path)

