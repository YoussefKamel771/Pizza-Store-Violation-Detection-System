"""
Frame Reader Service
====================
Reads video frames from a file, webcam, or RTSP stream and publishes
each frame to a Kafka topic for downstream processing.

Message schema (JSON):
{
    "frame_id":  int,     # monotonically increasing frame counter
    "timestamp": float,   # Unix epoch at the moment the frame was captured
    "source":    str,     # original VIDEO_SOURCE value
    "width":     int,     # frame width  in pixels
    "height":    int,     # frame height in pixels
    "frame":     str,     # base64-encoded JPEG image
}
"""

import base64
import logging
import signal
import sys
import time
import gc
import cv2

from config import get_settings
from producer import FrameProducer

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("frame-reader")

# ── Load config from environment variables ───────────────────────────────────
settings = get_settings()

# ── Helpers ───────────────────────────────────────────────────────────────────
def open_capture(source: str) -> cv2.VideoCapture:
    """
    Open a VideoCapture from:
      - An integer string ("0", "1") → webcam index
      - A file path               → local video file
      - An RTSP URL               → network IP camera
    """
    src = int(source) if source.isdigit() else source
    cap = cv2.VideoCapture(src)
    if not cap.isOpened():
        logger.error("Cannot open video source: %s", source)
        sys.exit(1)
    fps = cap.get(cv2.CAP_PROP_FPS) or 0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    logger.info(
        "Opened source '%s' -> %dx%d @ %.1f fps (native)  throttled to %d fps",
        source, width, height, fps, settings.MAX_FPS,
    )
    return cap

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

def encode_frame(frame, quality: int = settings.JPEG_QUALITY) -> str:
    """JPEG-compress and base64-encode a frame. Frees intermediate buffer immediately."""
    success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not success:
        raise RuntimeError("cv2.imencode failed")
    encoded = base64.b64encode(buffer).decode("utf-8")
    del buffer      # free raw JPEG bytes right away
    return encoded


# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True


def _handle_signal(signum, _frame):
    global _running
    logger.info("Received signal %d – shutting down…", signum)
    _running = False


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    producer = FrameProducer(settings.KAFKA_BOOTSTRAP_SERVERS, settings.KAFKA_TOPIC)
    cap = open_capture(settings.VIDEO_SOURCE)

    frame_interval = 1.0 / settings.MAX_FPS  # seconds between published frames
    frame_id = 0
    frames_published = 0
    start_time = time.time()

    logger.info("Frame reader started. Publishing to topic '%s'.", settings.KAFKA_TOPIC)

    try:
        while _running:
            loop_start = time.time()

            # Catch OpenCV OOM before it crashes the whole process
            try:
                ret, frame = cap.read()
            except cv2.error as exc:
                if "Insufficient memory" in str(exc) or "-4:" in str(exc):
                    logger.warning(
                        "OpenCV OOM on frame %d - running GC and skipping frame.", frame_id
                    )
                    gc.collect()
                    frame_id += 1
                    time.sleep(0.1)
                    continue
                raise

            if not ret:
                # End of file – rewind for video files; reconnect for streams
                if isinstance(settings.VIDEO_SOURCE, str) and not settings.VIDEO_SOURCE.isdigit():
                    # Try to reconnect once for RTSP sources
                    logger.warning("Frame read failed – attempting reconnect…")
                    cap.release()
                    time.sleep(2)
                    cap = open_capture(settings.VIDEO_SOURCE)
                    continue
                else:
                    logger.info("End of video file reached.")
                    break

            # Resize to cap peak memory (1692px -> 960px = ~43% less RAM)
            frame = resize_frame(frame)
            h, w = frame.shape[:2]

            try:
                encoded = encode_frame(frame)
            except RuntimeError as exc:
                logger.error("Encoding error: %s - skipping frame %d", exc, frame_id)
                frame_id += 1
                continue
            finally:
                del frame       # release BGR numpy array immediately

            message = {
                "frame_id": frame_id,
                "timestamp": time.time(),
                "source": settings.VIDEO_SOURCE,
                "width": w,
                "height": h,
                "frame": encoded,
            }

            producer.publish(message)
            del encoded         # free base64 string after handing off to producer
            frames_published += 1
            frame_id += 1

            # Poll every 10 frames to drain Kafka's internal send buffer
            # Without this, encoded frames pile up in RAM until a batch fills
            if frame_id % 10 == 0:
                producer.poll_events()

            if frame_id % 100 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    "Progress: %d frames published (%.1f fps avg)",
                    frames_published,
                    frames_published / elapsed,
                )

            # Throttle to MAX_FPS
            elapsed = time.time() - loop_start
            sleep_time = frame_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    finally:
        logger.info("Releasing resources…")
        cap.release()
        producer.flush()
        elapsed = time.time() - start_time
        logger.info(
            "Frame reader stopped. Published %d frames in %.1fs (%.1f fps avg).",
            frames_published, elapsed, frames_published / max(elapsed, 0.001),
        )


if __name__ == "__main__":
    main()