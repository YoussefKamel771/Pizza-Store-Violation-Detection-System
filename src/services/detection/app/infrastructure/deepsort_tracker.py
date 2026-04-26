# from deep_sort_realtime.deepsort_tracker import DeepSort
from src.services.detection.app.core.interfaces import ITracker
from deep_sort_pytorch.deep_sort import DeepSort
import torch

from deep_sort_pytorch.utils.parser import get_config


class DeepSortTracker(ITracker):
    def __init__(self, config_path: str = "app/deep_sort_pytorch/configs/deep_sort.yaml"):
        # max_age: frames to keep a track alive without detections
        # n_init: frames to 'confirm' a track
        self.tracker = self._initialize_deepsort(config_path)
        self.classes = ['hand', 'person', 'pizza', 'scooper']
        
    def _initialize_deepsort(self, config_path: str) -> DeepSort:
        """Initialize DeepSort tracker with configuration."""
        cfg_deep = get_config()
        cfg_deep.merge_from_file(config_path)
        
        return DeepSort(
            cfg_deep.DEEPSORT.REID_CKPT,
            max_dist=cfg_deep.DEEPSORT.MAX_DIST,
            min_confidence=cfg_deep.DEEPSORT.MIN_CONFIDENCE,
            nms_max_overlap=cfg_deep.DEEPSORT.NMS_MAX_OVERLAP,
            max_iou_distance=cfg_deep.DEEPSORT.MAX_IOU_DISTANCE,
            max_age=cfg_deep.DEEPSORT.MAX_AGE,
            n_init=cfg_deep.DEEPSORT.N_INIT,
            nn_budget=cfg_deep.DEEPSORT.NN_BUDGET,
            use_cuda=torch.cuda.is_available()
        )

    def update(self, detections: list, frame) -> list:
        # Reformat detections for DeepSORT: [([x,y,w,h], conf, label), ...]
        raw_dets = []
        for d in detections:
            x1, y1, x2, y2 = d['bbox']
            raw_dets.append(([x1, y1, x2 - x1, y2 - y1], d['conf'], d["label"], d['cls_id']))

         # Convert to tensors for DeepSort (it expects tensors but will convert internally)
        xywh_tensor = torch.tensor([d[0] for d in raw_dets], dtype=torch.float32)
        confs_tensor = torch.tensor([d[1] for d in raw_dets], dtype=torch.float32)
        cls_ids_tensor = torch.tensor([d[3] for d in raw_dets], dtype=torch.int64)  # Assuming labels are already mapped to integers
        # confs_tensor = torch.from_numpy(confs).float()

        # Update tracks
        tracks = self.tracker.update(xywh_tensor, confs_tensor, cls_ids_tensor, frame)
        
        # tracked_objects = []
        # for track in tracks:
        #     if not track.is_confirmed():
        #         continue
            
        #     tracked_objects.append({
        #         "track_id": track.track_id,
        #         "bbox": track.to_tlbr().tolist(), # [x1, y1, x2, y2]
        #         "label": track.get_det_class(),
        #         "centroid": self._get_centroid(track.to_tlbr())
        #     })

        tracked_objects = []
        if len(tracks) > 0:
            bbox_xyxy = tracks[:, :4]
            identities = tracks[:, -2]
            categories = tracks[:, -1]

            
            for i in range(len(tracks)):
                tracked_objects.append({
                    "track_id": int(identities[i]),
                    "bbox": bbox_xyxy[i].tolist(), # [x1, y1, x2, y2]
                    "label": self.classes[int(categories[i])],
                    "centroid": self._get_centroid(bbox_xyxy[i])
                })


        return tracked_objects

    def _get_centroid(self, tlbr):
        return ((tlbr[0] + tlbr[2]) / 2, (tlbr[1] + tlbr[3]) / 2)