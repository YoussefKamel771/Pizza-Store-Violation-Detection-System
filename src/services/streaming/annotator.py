"""
annotator.py
=============
Draws detection results onto a JPEG frame using OpenCV.
Called by the synchronizer callback once a frame_id is matched.

Kept purely functional (no state) so it can be called from any thread.
"""

import base64
import logging

import cv2
import numpy as np

logger = logging.getLogger(__name__)

# ── Colours (BGR) ─────────────────────────────────────────────────────────────
_COLOURS = {
    "Hand":    (  0, 200, 255),   # orange
    "Person":  ( 50, 205,  50),   # green
    "Pizza":   ( 30, 144, 255),   # blue
    "Scooper": (148,   0, 211),   # purple
}
_DEFAULT_COLOUR    = (200, 200, 200)
_VIOLATION_COLOUR  = (  0,   0, 255)   # red
_ROI_COLOUR        = (  0, 255, 255)   # yellow
_IN_ROI_COLOUR     = (  0,   0, 255)   # red when hand is in ROI

COLOURS = np.random.randint(0, 255, size=(100, 3), dtype=np.uint8)

def annotate(
    jpeg_bytes: bytes,
    detections: list,
    violation: dict | None,
    jpeg_quality: int = 80,
) -> bytes:
    """
    Decode JPEG → draw boxes → re-encode JPEG.

    Parameters
    ----------
    jpeg_bytes  : raw JPEG bytes from the video-frames Kafka topic
    detections  : list of detection dicts (label, confidence, x1,y1,x2,y2, in_roi)
    violation   : violation dict or None — triggers red overlay flash
    jpeg_quality: re-encode quality (0-100)

    Returns
    -------
    Annotated JPEG bytes
    """
    # Decode
    arr   = np.frombuffer(jpeg_bytes, dtype=np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        logger.error("Failed to decode JPEG frame — returning original.")
        return jpeg_bytes

    h, w = frame.shape[:2]

    # Red semi-transparent overlay on violation frames
    if violation is not None:
        overlay = frame.copy()
        cv2.rectangle(overlay, (0, 0), (w, h), _VIOLATION_COLOUR, -1)
        cv2.addWeighted(overlay, 0.15, frame, 0.85, 0, frame)

    # Draw each detection box
    for det in detections:
        x1, y1, x2, y2 = det["x1"], det["y1"], det["x2"], det["y2"]
        label      = det.get("label", "")
        confidence = det.get("confidence", 0.0)
        track_id   = det.get("track_id", -1)
        in_roi     = det.get("in_roi", False)
        # logger.info(f"detections annotated = {det}")

        tid = int(det['track_id'])
        colour = [int(c) for c in COLOURS[tid % 100]]
        # colour = _IN_ROI_COLOUR if in_roi else _COLOURS.get(label, _DEFAULT_COLOUR)

        # Bounding box
        thickness = 3 if in_roi else 2
        cv2.rectangle(frame, (x1, y1), (x2, y2), colour, thickness)

        # Label background
        tag = f"{label} #{track_id} {confidence:.0%}"
        (tw, th), _ = cv2.getTextSize(tag, cv2.FONT_HERSHEY_SIMPLEX, 0.55, 1)
        cv2.rectangle(frame, (x1, y1 - th - 8), (x1 + tw + 4, y1), colour, -1)

        # Label text
        cv2.putText(
            frame, tag,
            (x1 + 2, y1 - 4),
            cv2.FONT_HERSHEY_SIMPLEX, 0.55,
            (255, 255, 255), 1, cv2.LINE_AA,
        )

    # Violation banner at top of frame
    if violation is not None:
        banner = f"  VIOLATION  ROI: {violation['roi_id']}  Worker: #{violation['track_id']}"
        cv2.rectangle(frame, (0, 0), (w, 36), _VIOLATION_COLOUR, -1)
        cv2.putText(
            frame, banner,
            (8, 24),
            cv2.FONT_HERSHEY_SIMPLEX, 0.7,
            (255, 255, 255), 2, cv2.LINE_AA,
        )

    # Re-encode
    success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, jpeg_quality])
    if not success:
        logger.error("cv2.imencode failed — returning original.")
        return jpeg_bytes

    result = bytes(buffer)
    del buffer
    return result


def jpeg_to_base64(jpeg_bytes: bytes) -> str:
    return base64.b64encode(jpeg_bytes).decode("utf-8")