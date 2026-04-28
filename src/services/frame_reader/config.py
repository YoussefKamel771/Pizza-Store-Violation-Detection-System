import os
from pydantic_settings import BaseSettings , SettingsConfigDict
from typing import List

class Settings(BaseSettings):

    # ── Kafka ────────────────────────────────────────────────────────────────────
    # Inside Docker use "kafka:9092" (INTERNAL listener)
    # On your host machine use "localhost:29092" (EXTERNAL listener)
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "video-frames")

    # ── Video Source ─────────────────────────────────────────────────────────────
    # "0"           → default webcam
    # "/videos/x.mp4" → local file (mount a volume in docker-compose)
    # "rtsp://user:pass@ip:port/stream" → IP camera
    VIDEO_SOURCE: str = os.getenv("VIDEO_SOURCE", "../detection/test_data/Sah w b3dha ghalt (3).mp4")

    # ── Frame Throttling ─────────────────────────────────────────────────────────
    # Limit how many frames per second are sent to Kafka.
    # High FPS = more accurate but heavier load on broker + detection service.
    MAX_FPS: int = int(os.getenv("MAX_FPS", "15"))

    # ── JPEG Encoding ────────────────────────────────────────────────────────────
    # Lower quality = smaller Kafka messages = faster throughput.
    # 70-85 is a good balance for computer vision tasks.
    JPEG_QUALITY: int = int(os.getenv("JPEG_QUALITY", "80"))
    

def get_settings():
    return Settings()


