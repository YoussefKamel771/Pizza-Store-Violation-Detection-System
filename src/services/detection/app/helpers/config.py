from functools import lru_cache
import os

from pydantic_settings import BaseSettings 

class Settings(BaseSettings):

    model_path: str = os.getenv("MODEL_PATH", "../weights/best.pt")
    test_video_path: str = os.getenv("TEST_VIDEO_PATH", "../test_data/Sah w b3dha ghalt (3).mp4")
    roi_config_path: str = os.getenv("ROI_CONFIG_PATH", "../config/rois.json")

    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_frames_topic: str      = os.getenv("KAFKA_FRAMES_TOPIC",      "video-frames")
    kafka_violations_topic: str  = os.getenv("KAFKA_VIOLATIONS_TOPIC",  "violations")
    kafka_group_id: str          = os.getenv("KAFKA_GROUP_ID",          "detection-service")

    # ── Database ──────────────────────────────────────────────────────────────
    conn_str: str = os.getenv(
        "DB_CONN_STR",
        "postgresql://postgres:password@localhost:5432/scooper_db",
    )

    class Config:
        env_file = "../.env"
        env_file_encoding = "utf-8"

@lru_cache
def get_settings() -> Settings:
    return Settings()