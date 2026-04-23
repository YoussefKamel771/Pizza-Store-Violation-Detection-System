from pathlib import Path
import numpy as np
import cv2
from typing import List, Tuple, Dict, Optional, Set
import json
from datetime import datetime

class ROIDrawer:
    """Interactive ROI drawing class using mouse callbacks."""
    
    def __init__(self, window_name: str = "ROI Drawing"):
        self.window_name = window_name
        self.drawing = False
        self.current_roi_points = []
        self.temp_point = None
        self.completed_rois = []
        self.frame_copy = None
        self.original_frame = None
        self.roi_names = []
        self.roi_types = []
        self.current_roi_name = "ROI_1"
        self.current_roi_type = "protein_container"
        self.roi_counter = 1
        
    def mouse_callback(self, event, x, y, flags, param):
        """Handle mouse events for ROI drawing."""
        if self.frame_copy is None:
            return
            
        if event == cv2.EVENT_LBUTTONDOWN:
            # Add point to current ROI
            self.current_roi_points.append((x, y))
            print(f"Added point: ({x}, {y}) to {self.current_roi_name}")
            
        elif event == cv2.EVENT_MOUSEMOVE:
            # Update temporary point for preview
            self.temp_point = (x, y)
            
        elif event == cv2.EVENT_RBUTTONDOWN:
            # Finish current ROI (need at least 3 points)
            if len(self.current_roi_points) >= 3:
                self.completed_rois.append(self.current_roi_points.copy())
                self.roi_names.append(self.current_roi_name)
                self.roi_types.append(self.current_roi_type)
                print(f"Completed ROI: {self.current_roi_name} with {len(self.current_roi_points)} points")
                
                # Prepare for next ROI
                self.roi_counter += 1
                self.current_roi_name = f"ROI_{self.roi_counter}"
                self.current_roi_points = []
                self.temp_point = None
            else:
                print("Need at least 3 points to complete ROI. Continue clicking or press 'c' to clear.")
        
        # Update display
        self._update_display()
    
    def _update_display(self):
        """Update the display with current ROI drawing state."""
        if self.original_frame is None:
            return
            
        # Start with fresh copy
        self.frame_copy = self.original_frame.copy()
        
        # Draw completed ROIs
        for i, roi_points in enumerate(self.completed_rois):
            if len(roi_points) >= 3:
                pts = np.array(roi_points, dtype=np.int32)
                cv2.polylines(self.frame_copy, [pts], True, (0, 255, 0), 2)
                cv2.fillPoly(self.frame_copy, [pts], (0, 255, 0, 30))  # Semi-transparent fill
                
                # Draw ROI name
                if i < len(self.roi_names):
                    cv2.putText(self.frame_copy, self.roi_names[i], tuple(roi_points[0]), 
                               cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
        
        # Draw current ROI being drawn
        if len(self.current_roi_points) > 0:
            # Draw completed segments
            for i in range(len(self.current_roi_points)):
                cv2.circle(self.frame_copy, self.current_roi_points[i], 3, (0, 0, 255), -1)
                if i > 0:
                    cv2.line(self.frame_copy, self.current_roi_points[i-1], 
                            self.current_roi_points[i], (0, 0, 255), 2)
            
            # Draw line to temporary point
            if self.temp_point and len(self.current_roi_points) > 0:
                cv2.line(self.frame_copy, self.current_roi_points[-1], 
                        self.temp_point, (255, 0, 0), 1)
            
            # Draw closing line preview
            if len(self.current_roi_points) > 2 and self.temp_point:
                cv2.line(self.frame_copy, self.temp_point, 
                        self.current_roi_points[0], (255, 255, 0), 1)
        
        # Draw instructions
        instructions = [
            "Left click: Add point",
            "Right click: Complete ROI",
            "Press 'c': Clear current ROI",
            "Press 'r': Reset all ROIs",
            "Press 'n': Change ROI name",
            "Press 'q': Finish drawing",
            f"Current ROI: {self.current_roi_name}"
        ]
        
        for i, instruction in enumerate(instructions):
            cv2.putText(self.frame_copy, instruction, (10, 30 + i * 25), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
        
        cv2.imshow(self.window_name, self.frame_copy)
    
    def set_roi_name(self, name: str):
        """Set the name for the current ROI being drawn."""
        self.current_roi_name = name
        print(f"Current ROI name set to: {name}")
    
    def set_roi_type(self, roi_type: str):
        """Set the type for the current ROI being drawn."""
        self.current_roi_type = roi_type
        print(f"Current ROI type set to: {roi_type}")
    
    def clear_current_roi(self):
        """Clear the current ROI being drawn."""
        self.current_roi_points = []
        self.temp_point = None
        print("Cleared current ROI")
        self._update_display()
    
    def reset_all_rois(self):
        """Reset all ROIs."""
        self.completed_rois = []
        self.roi_names = []
        self.roi_types = []
        self.current_roi_points = []
        self.temp_point = None
        self.roi_counter = 1
        self.current_roi_name = "ROI_1"
        print("Reset all ROIs")
        self._update_display()
    
    def draw_rois(self, frame: np.ndarray) -> List[Tuple[str, List[Tuple[int, int]], str]]:
        """Interactive ROI drawing interface."""
        self.original_frame = frame.copy()
        self.frame_copy = frame.copy()
        
        cv2.namedWindow(self.window_name)
        cv2.setMouseCallback(self.window_name, self.mouse_callback)
        
        self._update_display()
        
        print("\n=== ROI Drawing Mode ===")
        print("Instructions:")
        print("- Left click to add points")
        print("- Right click to complete ROI (minimum 3 points)")
        print("- Press 'c' to clear current ROI")
        print("- Press 'r' to reset all ROIs")
        print("- Press 'n' to change ROI name")
        print("- Press 'q' to finish and return ROIs")
        print("========================\n")
        
        while True:
            key = cv2.waitKey(1) & 0xFF
            
            if key == ord('q'):
                break
            elif key == ord('c'):
                self.clear_current_roi()
            elif key == ord('r'):
                self.reset_all_rois()
            elif key == ord('n'):
                # Change ROI name
                print(f"Current ROI name: {self.current_roi_name}")
                new_name = input("Enter new ROI name: ").strip()
                if new_name:
                    self.set_roi_name(new_name)
                    self._update_display()
            elif key == 27:  # ESC key
                break
        
        cv2.destroyWindow(self.window_name)
        
        # Return completed ROIs in the format expected by ObjectTracker
        roi_data = []
        for i, roi_points in enumerate(self.completed_rois):
            name = self.roi_names[i] if i < len(self.roi_names) else f"ROI_{i+1}"
            roi_type = self.roi_types[i] if i < len(self.roi_types) else "protein_container"
            roi_data.append((name, roi_points, roi_type))
        
        return roi_data


class ROI:
    """Region of Interest class for defining protein container areas."""
    
    def __init__(self, name: str, points: List[Tuple[int, int]], roi_type: str = "protein_container"):
        self.name = name
        self.points = np.array(points, dtype=np.int32)
        self.roi_type = roi_type
        self.color = (0, 255, 255)  # Yellow for ROI
    
    def contains_point(self, point: Tuple[int, int]) -> bool:
        """Check if a point is inside the ROI using OpenCV pointPolygonTest."""
        return cv2.pointPolygonTest(self.points, point, False) >= 0
    
    def draw(self, frame: np.ndarray) -> None:
        """Draw the ROI on the frame."""
        cv2.polylines(frame, [self.points], True, self.color, 2)
        cv2.putText(frame, self.name, tuple(self.points[0]), cv2.FONT_HERSHEY_SIMPLEX, 
                   0.6, self.color, 2, cv2.LINE_AA)

class RoiManager:
    def __init__(self):
        self.rois: List[ROI] = []
        self.roi_drawer = ROIDrawer()

    def draw_rois_interactive(self, frame: np.ndarray) -> bool:
        """Launch interactive ROI drawing interface."""
        print("Launching interactive ROI drawing...")
        roi_data = self.roi_drawer.draw_rois(frame)
        
        if roi_data:
            # Clear existing ROIs
            self.rois.clear()
            
            # Add new ROIs
            for name, points, roi_type in roi_data:
                self.add_roi(name, points, roi_type)
            
            print(f"Added {len(roi_data)} ROIs")
            return True
        else:
            print("No ROIs were created")
            return False
    
    def add_roi(self, name: str, points: List[Tuple[int, int]], roi_type: str = "protein_container"):
        """Add a region of interest."""
        roi = ROI(name, points, roi_type)
        self.rois.append(roi)
        print(f"Added ROI: {name} with {len(points)} points")
    

    def add_default_rois(self):
        """Add default ROIs for demonstration. Modify coordinates for your specific use case."""
        # Example protein container ROI (modify coordinates based on your video)
        protein_container_1 = [(300, 200), (500, 200), (500, 400), (300, 400)]
        self.add_roi("Protein_Container_1", protein_container_1, "protein_container")
        
        # Add more ROIs as needed
        protein_container_2 = [(600, 150), (800, 150), (800, 350), (600, 350)]
        self.add_roi("Protein_Container_2", protein_container_2, "protein_container")

    def save_rois_to_file(self, filename: str = "rois.json"):
        """Save current ROIs to a JSON file."""
        roi_data = {
            'timestamp': datetime.now().isoformat(),
            'total_rois': len(self.rois),
            'rois': []
        }
        
        for roi in self.rois:
            roi_info = {
                'name': roi.name,
                'points': roi.points.tolist(),
                'roi_type': roi.roi_type
            }
            roi_data['rois'].append(roi_info)
        
        try:
            with open(filename, 'w') as f:
                json.dump(roi_data, f, indent=2)
            print(f"Saved {len(self.rois)} ROIs to {filename}")
        except Exception as e:
            print(f"Error saving ROIs: {e}")
    
    def load_rois_from_file(self, filename: str = "rois.json"):
        """Load ROIs from a JSON file."""
        try:
            with open(filename, 'r') as f:
                roi_data = json.load(f)
            
            self.rois.clear()
            for roi_info in roi_data.get('rois', []):
                name = roi_info['name']
                points = [(int(p[0]), int(p[1])) for p in roi_info['points']]
                roi_type = roi_info.get('roi_type', 'protein_container')
                self.add_roi(name, points, roi_type)
            
            print(f"Loaded {len(self.rois)} ROIs from {filename}")
            return True
        except Exception as e:
            print(f"Error loading ROIs: {e}")
            return False
