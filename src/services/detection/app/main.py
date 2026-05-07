"""
main.py — Production Entry Point
==================================
Starts the detection service in production mode:
  - Loads ROIs from rois.json  (created by run_local.py)
  - Consumes frames from Kafka [video-frames]
  - Publishes violations to Kafka [violations]
  - Saves violation records to Postgres

Run:
    python main.py

NOTE: Run run_local.py FIRST to draw and save your ROIs.
      This script will refuse to start without a valid rois.json.
"""

import logging
import signal
import sys

from core.logging_config import setup_logger
from helpers.config import get_settings
from infrastructure.detector import YOLO11Detector
from infrastructure.byteTrack_tracker import ByteTrackTracker
from infrastructure.kafka_consumer import KafkaFrameConsumer
from infrastructure.kafka_streamer import KafkaStreamer
from infrastructure.postgress_repo import PostgresRepository
from infrastructure.roi_manager import RoiManager
from domain.engine import ScooperViolationEngine
from detection_manager import DetectionManager
from infrastructure.visualization import Visualizer

setup_logger()
logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    logger.info("Settings loaded: %s", settings.model_dump())

    # ── Load ROIs ─────────────────────────────────────────────────────────────
    roi_manager = RoiManager()
    loaded = roi_manager.load_rois_from_file(settings.roi_config_path)
    if not loaded or len(roi_manager.rois) == 0:
        logger.error(
            "No ROIs found at '%s'. "
            "Run  python run_local.py  first to draw and save your ROIs.",
            settings.roi_config_path,
        )
        sys.exit(1)

    logger.info("Loaded %d ROI(s) from %s", len(roi_manager.rois), settings.roi_config_path)

    # ── Build components ──────────────────────────────────────────────────────
    detector  = YOLO11Detector(settings.model_path)
    tracker   = ByteTrackTracker(track_thresh=0.1, track_buffer=60, match_thresh=0.9)
    engine    = ScooperViolationEngine(roi_manager=roi_manager)
    repo      = PostgresRepository(settings.conn_str)

    broker    = KafkaFrameConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_frames_topic,
        group_id=settings.kafka_group_id,
    )
    publisher = KafkaStreamer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_detection_results_topic,
    )

    visualizer = Visualizer()

    manager = DetectionManager(
        detector=detector,
        tracker=tracker,
        broker=broker,
        repo=repo,
        engine=engine,
        result_publisher=publisher,
        roi_manager=roi_manager,
        visualizer=visualizer,
        vis=True,  # Visualization not needed in production mode
    )

    # ── Handle Ctrl+C gracefully ──────────────────────────────────────────────
    def _shutdown(signum, _frame):
        logger.info("Signal %d received — shutting down…", signum)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ── Blocking loop ─────────────────────────────────────────────────────────
    manager.start()


if __name__ == "__main__":
    main()