#!/usr/bin/env python3
"""
Camera Server Module - Giả lập camera gửi frames đến processing server
Chức năng:
- Đọc video từ webcam hoặc file video
- Chuyển đổi frames thành gói tin JSON
- Gửi frames qua TCP socket đến processing server
"""

import cv2
import socket
import json
import base64
import time
import threading
import queue
import argparse
import sys
from typing import Optional, Tuple

class CameraServer:
    def __init__(self, 
                 server_host: str = "localhost", 
                 server_port: int = 6100,
                 video_source: Optional[str] = None,
                 fps_limit: int = 5):
        """
        Khởi tạo camera server
        
        Args:
            server_host: Địa chỉ IP của processing server
            server_port: Port của processing server
            video_source: Đường dẫn video file, None để sử dụng webcam
            fps_limit: Giới hạn FPS gửi frames
        """
        self.server_host = server_host
        self.server_port = server_port
        self.video_source = video_source
        self.fps_limit = fps_limit
        self.frame_delay = 1.0 / fps_limit
        
        # Queue để buffer frames
        self.frame_queue = queue.Queue(maxsize=10)
        self.running = False
        
        # Socket connection
        self.sock = None
        self.frame_id = 0
        
    def connect_to_server(self) -> bool:
        """Kết nối đến processing server"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.server_host, self.server_port))
            print(f"[CAMERA] Đã kết nối thành công đến {self.server_host}:{self.server_port}")
            return True
        except Exception as e:
            print(f"[CAMERA] Lỗi kết nối: {e}")
            return False
    
    def capture_frames(self):
        """Thread để capture frames từ video source"""
        # Khởi tạo video capture
        if self.video_source is None:
            cap = cv2.VideoCapture(0)  # Webcam
            print("[CAMERA] Sử dụng webcam")
        else:
            cap = cv2.VideoCapture(self.video_source)  # Video file
            print(f"[CAMERA] Sử dụng video file: {self.video_source}")
        
        if not cap.isOpened():
            print("[CAMERA] Không thể mở video source")
            return
        
        # Thiết lập độ phân giải
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
        
        while self.running:
            ret, frame = cap.read()
            
            if not ret:
                if self.video_source is not None:
                    # Lặp lại video nếu là file
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    continue
                else:
                    print("[CAMERA] Không thể đọc frame từ webcam")
                    break
            
            # Resize frame để giảm kích thước gói tin
            frame = cv2.resize(frame, (320, 240))
            
            # Đưa frame vào queue
            if not self.frame_queue.full():
                self.frame_queue.put(frame.copy())
            
            # Hiển thị preview (optional)
            cv2.imshow('Camera Preview', frame)
            if cv2.waitKey(1) & 0xFF == ord('q'):
                self.running = False
                break
        
        cap.release()
        cv2.destroyAllWindows()
    
    def send_frames(self):
        """Thread để gửi frames đến processing server"""
        last_send_time = time.time()
        
        while self.running:
            try:
                # Lấy frame từ queue
                frame = self.frame_queue.get(timeout=1.0)
                
                # Kiểm tra thời gian để giới hạn FPS
                current_time = time.time()
                time_diff = current_time - last_send_time
                if time_diff < self.frame_delay:
                    time.sleep(self.frame_delay - time_diff)
                
                # Chuyển đổi frame thành base64
                _, buffer = cv2.imencode('.jpg', frame, 
                                        [cv2.IMWRITE_JPEG_QUALITY, 70])
                frame_base64 = base64.b64encode(buffer).decode('utf-8')
                
                # Tạo gói tin JSON
                packet = {
                    "frame_id": self.frame_id,
                    "timestamp": time.time(),
                    "frame_data": frame_base64,
                    "width": frame.shape[1],
                    "height": frame.shape[0],
                    "channels": frame.shape[2]
                }
                
                # Gửi gói tin
                packet_json = json.dumps(packet) + "\n"
                self.sock.send(packet_json.encode('utf-8'))
                
                print(f"[CAMERA] Đã gửi frame {self.frame_id}")
                self.frame_id += 1
                last_send_time = time.time()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[CAMERA] Lỗi gửi frame: {e}")
                break
    
    def start(self):
        """Khởi động camera server"""
        print("[CAMERA] Đang khởi động camera server...")
        
        # Kết nối đến server
        if not self.connect_to_server():
            return
        
        self.running = True
        
        # Khởi động threads
        capture_thread = threading.Thread(target=self.capture_frames, daemon=True)
        send_thread = threading.Thread(target=self.send_frames, daemon=True)
        
        capture_thread.start()
        send_thread.start()
        
        print("[CAMERA] Camera server đã khởi động")
        print("Nhấn Ctrl+C để dừng...")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[CAMERA] Đang dừng camera server...")
            self.stop()
    
    def stop(self):
        """Dừng camera server"""
        self.running = False
        if self.sock:
            self.sock.close()
        print("[CAMERA] Camera server đã dừng")

def main():
    parser = argparse.ArgumentParser(description='Camera Server cho hệ thống xóa phông nền')
    parser.add_argument('--host', default='localhost', 
                        help='Địa chỉ IP của processing server')
    parser.add_argument('--port', type=int, default=6100, 
                        help='Port của processing server')
    parser.add_argument('--video', default=None, 
                        help='Đường dẫn video file (None để sử dụng webcam)')
    parser.add_argument('--fps', type=int, default=5, 
                        help='Giới hạn FPS gửi frames')
    
    args = parser.parse_args()
    
    # Tạo và khởi động camera server
    camera_server = CameraServer(
        server_host=args.host,
        server_port=args.port,
        video_source=args.video,
        fps_limit=args.fps
    )
    
    camera_server.start()

if __name__ == "__main__":
    main()