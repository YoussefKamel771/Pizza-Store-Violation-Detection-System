import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Kafka ─────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str       = os.getenv("KAFKA_BOOTSTRAP_SERVERS",        "localhost:29092")
    kafka_detection_results_topic: str = os.getenv("KAFKA_DETECTION_RESULTS_TOPIC",  "detection-results")
    kafka_results_group: str           = os.getenv("KAFKA_RESULTS_GROUP",            "streaming-results")

    # ── Sync buffer ───────────────────────────────────────────────────────────
    # How many frame_ids to keep in memory while waiting for the matching
    # detection result. Detection takes a few ms so the result arrives
    # shortly after the frame. 300 frames of buffer ≈ 20 seconds at 15fps.
    sync_buffer_max_size: int = int(os.getenv("SYNC_BUFFER_MAX_SIZE", "600"))

    # ── Annotation ────────────────────────────────────────────────────────────
    jpeg_quality: int = int(os.getenv("JPEG_QUALITY", "80"))

    # ── Server ────────────────────────────────────────────────────────────────
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()