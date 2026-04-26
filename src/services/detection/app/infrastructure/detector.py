import cv2
import torch
from ultralytics import YOLO
from core.interfaces import IDetector

class YOLO11Detector(IDetector):
    def __init__(self, model_path: str = '../best.pt'):
        # Load the pretrained YOLO 11 medium model 
        torch.cuda.empty_cache()
        self.model = YOLO(model_path)
        # Expected classes: Hand, Person, Pizza, Scooper 
        self.classes = ['hand', 'person', 'pizza', 'scooper']

    def detect(self, frame) -> list:
        results = self.model(frame)
        detections = []
        for r in results:
            for box in r.boxes:
                detections.append({
                    "bbox": box.xyxy[0].tolist(),
                    "conf": float(box.conf),
                    "label": self.classes[int(box.cls)],
                    "cls_id": int(box.cls),
                    "centroid": self._get_centroid(box.xyxy[0])
                })
        return detections

    def _get_centroid(self, bbox):
        x1, y1, x2, y2 = bbox
        return ((x1 + x2) / 2, (y1 + y2) / 2)