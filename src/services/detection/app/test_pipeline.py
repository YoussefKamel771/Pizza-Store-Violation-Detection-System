"""
run_local.py — Local Test Script
==================================
Use this to:
  1. Draw ROIs interactively (saved to rois.json for production reuse)
  2. Test detection + violation logic on a local video file
  3. Visually verify everything before connecting Kafka

No broker, no Postgres required — just OpenCV and your model.

Run:
    python run_local.py

Controls while playing:
    Q  → quit and save ROIs
    S  → force-save ROIs mid-session
"""

import logging
import os

import cv2
import numpy as np

from core.logging_config import setup_logger
from helpers.config import get_settings
from infrastructure.detector import YOLO11Detector
from infrastructure.byteTrack_tracker import ByteTrackTracker
from infrastructure.visualization import Visualizer
from infrastructure.roi_manager import RoiManager
from infrastructure.postgress_repo import PostgresRepository
from domain.engine import ScooperViolationEngine
from detection_manager import DetectionManager
from test import DetectionManagerTest
from core.interfaces import IViolationPublisher

setup_logger()
logger = logging.getLogger(__name__)


# ── No-op publisher for local runs ────────────────────────────────────────────

class _NoOpPublisher(IViolationPublisher):
    """Satisfies the IViolationPublisher interface without touching Kafka."""

    def connect(self)    -> None: pass
    def disconnect(self) -> None: pass

    def publish(self, violation) -> None:
        logger.debug("[LOCAL] Skipping Kafka publish for violation: %s", violation)


# ── ROI initialisation ────────────────────────────────────────────────────────

def initialize_roi_manager(
    frame: np.ndarray, roi_manager: RoiManager, path: str
) -> bool:
    """
    Load ROIs from JSON if the file exists, otherwise open the interactive
    drawing tool.  Saves automatically after drawing.
    """
    if os.path.exists(path):
        logger.info("Found existing ROI config at %s — loading…", path)
        success = roi_manager.load_rois_from_file(path)
        if success and len(roi_manager.rois) > 0:
            logger.info("Loaded %d ROI(s).", len(roi_manager.rois))
            return True
        logger.warning("ROI file exists but is empty/corrupt — re-drawing.")

    print("\n" + "=" * 60)
    print("  No ROIs found. Entering interactive ROI drawing mode.")
    print("  Draw polygons around the protein container zones.")
    print("=" * 60 + "\n")

    success = roi_manager.draw_rois_interactive(frame)
    if success:
        roi_manager.save_rois_to_file(path)
        logger.info("ROIs saved to %s — will be reused by main.py in production.", path)
        return True

    logger.error("ROI drawing was cancelled or failed.")
    return False


def resize_frame(frame, max_width: int = 960):
    """
    Downscale so width <= max_width (keeps aspect ratio).
    1692px -> 960px saves ~43% RAM per frame.
    Raise max_width if your detection model needs full resolution.
    """
    h, w = frame.shape[:2]
    if w <= max_width:
        return frame
    scale = max_width / w
    return cv2.resize(frame, (max_width, int(h * scale)), interpolation=cv2.INTER_AREA)

# ── Main test loop ────────────────────────────────────────────────────────────

def run_local(video_path: str, model_path: str, roi_config_path: str) -> None:
    detector    = YOLO11Detector(model_path)
    tracker     = ByteTrackTracker(track_thresh=0.1, track_buffer=60, match_thresh=0.9)
    visualizer  = Visualizer()
    roi_manager = RoiManager()
    publisher   = _NoOpPublisher()

    settings = get_settings()
    repo     = PostgresRepository(settings.conn_str)

    # Open video
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Cannot open video: {video_path}")

    ret, first_frame = cap.read()
    first_frame = resize_frame(first_frame.copy())
    if not ret:
        raise ValueError("Cannot read the first frame.")

    # Draw / load ROIs
    if not initialize_roi_manager(first_frame, roi_manager, roi_config_path):
        logger.error("ROI setup failed. Exiting.")
        cap.release()
        return

    engine  = ScooperViolationEngine(roi_manager=roi_manager)
    manager = DetectionManagerTest(
        detector=detector,
        tracker=tracker,
        broker=None,            # no broker in local mode
        repo=repo,
        engine=engine,
        violation_publisher=publisher,
    )

    # Rewind so we process from frame 0
    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)

    total_violations = 0
    frame_id = 0

    logger.info("Starting local playback — press Q to quit, S to save ROIs.")

    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                logger.info("End of video.")
                break

            frame = resize_frame(frame)
            tracked_objs, violations, _ = manager.on_frame_received(frame)
            total_violations += len(violations)
            frame_id += 1

            # Draw bounding boxes, ROI outlines, and violation highlights
            display = visualizer.draw_frame(frame.copy(), tracked_objs, roi_manager, violations)

            # Overlay running violation count
            cv2.putText(
                display,
                f"Violations: {total_violations}",
                (15, 35),
                cv2.FONT_HERSHEY_SIMPLEX,
                1.0,
                (0, 0, 255),
                2,
                cv2.LINE_AA,
            )

            cv2.imshow("Pizza Store — Local Debug", display)
            key = cv2.waitKey(1) & 0xFF

            if key == ord("q"):
                break
            elif key == ord("s"):
                roi_manager.save_rois_to_file(roi_config_path)
                logger.info("ROIs manually saved.")

    finally:
        roi_manager.save_rois_to_file(roi_config_path)
        cap.release()
        cv2.destroyAllWindows()
        logger.info("Session ended. Total violations: %d", total_violations)


if __name__ == "__main__":
    settings = get_settings()
    run_local(
        video_path=settings.test_video_path,
        model_path=settings.model_path,
        roi_config_path=settings.roi_config_path,
    )