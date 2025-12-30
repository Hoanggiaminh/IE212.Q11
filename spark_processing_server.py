#!/usr/bin/env python3
"""
Spark Processing Server Module - Xử lý và xóa phông nền frames
Chức năng:
- Nhận frames từ camera server qua TCP
- Sử dụng Spark để xử lý streaming data
- Xóa phông nền cho từng frame
- Lưu kết quả thành file ảnh
"""

import socket
import json
import base64
import numpy as np
import cv2
import os
import time
from datetime import datetime
from typing import Iterator, Tuple
import threading
import queue

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext

# Import background remover
# from background_remover import remove_background

# Static function để xử lý frame - tránh serialization issues
def process_frame_static(packet_data):
    """Static function để xử lý frame trong Spark worker"""
    try:
        packet, output_dir = packet_data
        
        # Import local để tránh serialization
        import numpy as np
        import cv2
        import base64
        import os
        from datetime import datetime
        import mediapipe as mp
        from mediapipe.tasks import python
        from mediapipe.tasks.python import vision
        
        # Decode frame từ base64
        frame_bytes = base64.b64decode(packet['frame_data'])
        nparr = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if frame is None:
            return None
        
        # Khởi tạo background remover local
        BG_COLOR = (192, 192, 192)
        MASK_COLOR = (255, 255, 255)
        
        model_path = "models/selfie_segmenter.tflite"
        base_options = python.BaseOptions(model_asset_path=model_path)
        options = vision.ImageSegmenterOptions(base_options=base_options, output_category_mask=True)
        segmenter = vision.ImageSegmenter.create_from_options(options)
        
        # Xử lý background removal
        mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame)
        segmentation_result = segmenter.segment(mp_image)
        category_mask = segmentation_result.category_mask
        
        # Remove background
        image_data = mp_image.numpy_view()
        fg_image = np.zeros(image_data.shape, dtype=np.uint8)
        fg_image[:] = MASK_COLOR
        bg_image = np.zeros(image_data.shape, dtype=np.uint8)
        bg_image[:] = BG_COLOR
        
        # Fix shape issue với category mask
        mask_data = category_mask.numpy_view()
        if len(mask_data.shape) == 2:
            # Mask is 2D (H, W), expand to 3D (H, W, 3)
            condition = np.stack((mask_data,) * 3, axis=-1) > 0.2
        else:
            # Mask is already 3D, squeeze if needed và expand
            mask_data = np.squeeze(mask_data)
            if len(mask_data.shape) == 2:
                condition = np.stack((mask_data,) * 3, axis=-1) > 0.2
            else:
                condition = mask_data > 0.2
        
        processed_frame = np.where(condition, bg_image, image_data)
        
        # Tạo tên file
        timestamp = datetime.fromtimestamp(packet['timestamp']).strftime("%Y%m%d_%H%M%S")
        filename = f"frame_{packet['frame_id']}_{timestamp}.jpg"
        filepath = os.path.join(output_dir, filename)
        
        # Lưu file
        cv2.imwrite(filepath, processed_frame)
        
        return filepath
        
    except Exception as e:
        return f"ERROR: {str(e)}"

class SparkProcessingServer:
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 6100,
                 output_dir: str = "processed_frames",
                 batch_size: int = 5,
                 use_spark_rdd: bool = True):
        """
        Khởi tạo Spark processing server
        
        Args:
            host: Địa chỉ IP để listen
            port: Port để listen
            output_dir: Thư mục lưu frames đã xử lý
            batch_size: Số frames xử lý trong mỗi batch
            use_spark_rdd: Sử dụng Spark RDD (False = fallback to local processing)
        """
        self.host = host
        self.port = port
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.use_spark_rdd = use_spark_rdd
        
        # Tạo thư mục output
        os.makedirs(output_dir, exist_ok=True)
        
        # Queue để buffer frames
        self.frame_queue = queue.Queue()
        self.running = False
        
        # Socket
        self.sock = None
        self.conn = None
        
        # Khởi tạo Spark
        self.init_spark()
    
    def init_spark(self):
        """Khởi tạo Spark Session với cấu hình tối ưu cho Big Data"""
        print("[BIG DATA] Đang khởi tạo Spark Session cho xử lý phân tán...")
        
        # Tìm Python executable path
        import sys
        import os
        python_path = sys.executable
        print(f"[SPARK] Sử dụng Python tại: {python_path}")
        
        # Thiết lập environment variables để đảm bảo Spark hoạt động
        os.environ["PYSPARK_PYTHON"] = python_path
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
        
        self.spark = SparkSession.builder \
            .appName("BigDataBackgroundRemovalSystem") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.pyspark.python", python_path) \
            .config("spark.pyspark.driver.python", python_path) \
            .config("spark.executorEnv.PYSPARK_PYTHON", python_path) \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.python.worker.reuse", "false") \
            .config("spark.task.maxFailures", "1") \
            .getOrCreate()
        
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel("WARN")
        
        print(f"[✅ BIG DATA] Spark Session khởi tạo thành công: {self.spark.version}")
        print(f"[📊 SPARK] Số cores có sẵn: {self.sc.defaultParallelism}")
        print(f"[🔧 SPARK] Master: {self.sc.master}")
        print(f"[⚡ READY] Hệ thống sẵn sàng xử lý Big Data với Spark RDD!")
        
        # Test RDD functionality
        try:
            test_rdd = self.sc.parallelize([1, 2, 3, 4])
            test_result = test_rdd.map(lambda x: x * 2).collect()
            print(f"[✅ RDD TEST] Spark RDD hoạt động bình thường: {test_result}")
        except Exception as e:
            print(f"[❌ RDD TEST] Spark RDD có vấn đề: {e}")
            print("[CRITICAL] Cần kiểm tra cấu hình Spark!")
    
    def setup_server(self):
        """Thiết lập TCP server"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.listen(1)
        
        print(f"[SERVER] Đang chờ kết nối trên {self.host}:{self.port}...")
        self.conn, addr = self.sock.accept()
        print(f"[SERVER] Đã kết nối từ {addr}")
    
    def receive_frames(self):
        """Thread để nhận frames từ camera server"""
        buffer = ""
        
        while self.running:
            try:
                # Nhận data
                data = self.conn.recv(4096).decode('utf-8')
                if not data:
                    print("[SERVER] Mất kết nối từ client")
                    break
                
                buffer += data
                
                # Xử lý các gói tin hoàn chỉnh (kết thúc bằng \n)
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    
                    if line.strip():
                        try:
                            packet = json.loads(line)
                            self.frame_queue.put(packet)
                            print(f"[SERVER] Nhận frame {packet['frame_id']}")
                        except json.JSONDecodeError as e:
                            print(f"[SERVER] Lỗi decode JSON: {e}")
                
            except Exception as e:
                print(f"[SERVER] Lỗi nhận frame: {e}")
                break
    
    def process_frames_batch(self, frames_batch: list) -> list:
        """Xử lý một batch frames với Spark RDD - BẮT BUỘC để đáp ứng yêu cầu Big Data"""
        print(f"[BIG DATA SPARK] Đang xử lý batch {len(frames_batch)} frames với RDD...")
        
        if self.use_spark_rdd:
            try:
                # BẮT BUỘC sử dụng Spark RDD cho Big Data
                return self._process_with_spark_rdd_enhanced(frames_batch)
            except Exception as e:
                print(f"[SPARK RDD] Lỗi RDD processing: {e}")
                print(f"[SPARK RDD] Chi tiết lỗi: {str(e)}")
                # Thử lại với cấu hình khác thay vì fallback
                print("[SPARK RDD] Đang thử lại với cấu hình tối ưu...")
                return self._retry_spark_rdd(frames_batch)
        else:
            print("[WARNING] Chế độ no-rdd được bật - không đáp ứng yêu cầu Big Data!")
            return self._process_locally(frames_batch)
    
    def _process_with_spark_rdd_enhanced(self, frames_batch: list) -> list:
        """Enhanced Spark RDD processing với error handling tốt hơn"""
        print(f"[SPARK RDD] Khởi tạo RDD với {len(frames_batch)} frames...")
        
        # Tạo tuple (packet, output_dir) để pass vào RDD
        frames_with_output = [(packet, self.output_dir) for packet in frames_batch]
        
        # Tạo RDD từ frames batch với số partitions phù hợp
        default_parallelism = getattr(self.sc, 'defaultParallelism', 2) or 2
        num_partitions = min(len(frames_batch), default_parallelism)
        frames_rdd = self.sc.parallelize(frames_with_output, numSlices=num_partitions)
        
        print(f"[SPARK RDD] Đã tạo RDD với {frames_rdd.getNumPartitions()} partitions")
        print(f"[SPARK RDD] Bắt đầu distributed processing...")
        
        # Xử lý với map operation - sử dụng static function
        processed_rdd = frames_rdd.map(process_frame_static)
        
        print(f"[SPARK RDD] Đang collect kết quả từ {num_partitions} workers...")
        
        # Collect kết quả
        results = processed_rdd.collect()
        
        # Filter out errors và None results
        successful_results = [r for r in results if r is not None and not str(r).startswith("ERROR:")]
        
        # Log errors if any
        errors = [r for r in results if str(r).startswith("ERROR:")]
        for error in errors:
            print(f"[SPARK RDD] Worker Error: {error}")
        
        print(f"[✅ BIG DATA SUCCESS] RDD Distributed processing hoàn thành: {len(successful_results)}/{len(frames_batch)} frames")
        print(f"[📊 SPARK STATS] Partitions: {num_partitions}, Workers: {self.sc.defaultParallelism}")
        
        return successful_results
    
    def _retry_spark_rdd(self, frames_batch: list) -> list:
        """Retry Spark RDD với cấu hình tối ưu"""
        print("[SPARK RETRY] Đang retry với cấu hình tối ưu...")
        
        try:
            # Tạo RDD với 1 partition để giảm complexity
            frames_with_output = [(packet, self.output_dir) for packet in frames_batch]
            frames_rdd = self.sc.parallelize(frames_with_output, numSlices=1)
            
            print("[SPARK RETRY] Sử dụng 1 partition để stability...")
            
            # Process với map
            processed_rdd = frames_rdd.map(process_frame_static)
            results = processed_rdd.collect()
            
            successful_results = [r for r in results if r is not None and not str(r).startswith("ERROR:")]
            
            if len(successful_results) > 0:
                print(f"[✅ SPARK RETRY SUCCESS] {len(successful_results)}/{len(frames_batch)} frames processed")
                return successful_results
            else:
                raise Exception("No successful results from retry")
                
        except Exception as e:
            print(f"[❌ SPARK RETRY FAILED] {e}")
            print("[CRITICAL] KHÔNG THỂ SỬ DỤNG SPARK RDD - HỆ THỐNG KHÔNG ĐẠT YÊU CẦU BIG DATA!")
            # Return empty để không fallback
            return []
    
    def _process_locally(self, frames_batch: list) -> list:
        """Fallback: Local processing - CẢNH BÁO không đáp ứng yêu cầu Big Data!"""
        print("[⚠️  WARNING] ĐANG SỬ DỤNG LOCAL PROCESSING - KHÔNG ĐẠT YÊU CẦU BIG DATA!")
        print("[⚠️  WARNING] Cần phải sử dụng Spark RDD để đáp ứng yêu cầu đề bài!")
        
        successful_results = []
        
        for packet in frames_batch:
            try:
                result = process_frame_static((packet, self.output_dir))
                if result and not str(result).startswith("ERROR:"):
                    successful_results.append(result)
                else:
                    print(f"[LOCAL] Lỗi xử lý frame {packet['frame_id']}: {result}")
            except Exception as e:
                print(f"[LOCAL] Exception xử lý frame {packet['frame_id']}: {e}")
        
        print(f"[⚠️  LOCAL FALLBACK] hoàn thành xử lý: {len(successful_results)}/{len(frames_batch)} frames thành công")
        print("[⚠️  CRITICAL] HỆ THỐNG KHÔNG ĐẠT YÊU CẦU BIG DATA DO SỬ DỤNG LOCAL PROCESSING!")
        
        return successful_results
    
    def process_frames_worker(self):
        """Worker thread để xử lý frames với Spark"""
        frames_batch = []
        last_process_time = time.time()
        batch_timeout = 3.0  # Xử lý batch sau 3 giây nếu chưa đủ
        
        while self.running:
            try:
                # Lấy frame từ queue
                if not self.frame_queue.empty():
                    packet = self.frame_queue.get(timeout=1.0)
                    frames_batch.append(packet)
                
                current_time = time.time()
                
                # Xử lý batch khi đủ số lượng hoặc timeout
                if (len(frames_batch) >= self.batch_size or 
                    (len(frames_batch) > 0 and current_time - last_process_time > batch_timeout)):
                    
                    # Xử lý batch với Spark
                    self.process_frames_batch(frames_batch)
                    
                    # Reset batch
                    frames_batch = []
                    last_process_time = current_time
                
                else:
                    time.sleep(0.1)  # Ngắt nhỏ để không chiếm CPU
                    
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[SPARK] Lỗi trong worker: {e}")
                continue
        
        # Xử lý frames còn lại trong batch cuối
        if frames_batch:
            print("[SPARK] Xử lý batch cuối cùng...")
            self.process_frames_batch(frames_batch)
    
    def start(self):
        """Khởi động processing server"""
        print("[SERVER] Đang khởi động Spark processing server...")
        
        self.running = True
        
        # Thiết lập server
        self.setup_server()
        
        # Khởi động worker threads
        receive_thread = threading.Thread(target=self.receive_frames, daemon=True)
        process_thread = threading.Thread(target=self.process_frames_worker, daemon=True)
        
        receive_thread.start()
        process_thread.start()
        
        print(f"[SERVER] Processing server đã khởi động")
        print(f"[SERVER] Frames sẽ được lưu trong: {self.output_dir}")
        print("Nhấn Ctrl+C để dừng...")
        
        try:
            while self.running:
                time.sleep(1)
                
                # Hiển thị stats
                queue_size = self.frame_queue.qsize()
                if queue_size > 0:
                    print(f"[STATS] Frames trong queue: {queue_size}")
                    
        except KeyboardInterrupt:
            print("\n[SERVER] Đang dừng processing server...")
            self.stop()
    
    def stop(self):
        """Dừng processing server"""
        self.running = False
        
        if self.conn:
            self.conn.close()
        if self.sock:
            self.sock.close()
        
        if self.spark:
            self.spark.stop()
        
        print("[SERVER] Processing server đã dừng")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Spark Processing Server cho hệ thống xóa phông nền')
    parser.add_argument('--host', default='localhost', 
                        help='Địa chỉ IP để listen')
    parser.add_argument('--port', type=int, default=6100, 
                        help='Port để listen')
    parser.add_argument('--output', default='processed_frames', 
                        help='Thư mục lưu frames đã xử lý')
    parser.add_argument('--batch-size', type=int, default=5, 
                        help='Số frames xử lý trong mỗi batch')
    parser.add_argument('--no-rdd', action='store_true', 
                        help='Không sử dụng Spark RDD (fallback mode)')
    
    args = parser.parse_args()
    
    # Tạo và khởi động processing server
    processing_server = SparkProcessingServer(
        host=args.host,
        port=args.port,
        output_dir=args.output,
        batch_size=args.batch_size,
        use_spark_rdd=not args.no_rdd
    )
    
    processing_server.start()

if __name__ == "__main__":
    main()