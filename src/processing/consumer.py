import os
import json
import time
import logging
from datetime import datetime
from collections import Counter
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s] %(message)s')

# ─────────────────────────────────────────────
# CẤU HÌNH KẾT NỐI (Lấy từ biến môi trường)
# ─────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092,localhost:9093").split(',')
# Tạm thời để MongoDB là local, khi dựng file docker-compose MongoDB ta sẽ dùng URI của Replica Set sau
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0")
GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "video-analytics-group")

TOPICS = ["play_events", "quality_metrics", "user_actions"]
BATCH_WINDOW_SECONDS = 5  # Xử lý theo batch mỗi 5 giây (Mô phỏng Spark Streaming micro-batch)

# ─────────────────────────────────────────────
# LOGIC XỬ LÝ (PROCESSING & AGGREGATION)
# ─────────────────────────────────────────────
def process_batch(events, db_collection):
    """Tính toán thống kê cho 1 batch dữ liệu và lưu xuống Database"""
    if not events:
        return

    # 1. Thống kê số lượng theo khu vực xem (Region)
    regions = [e.get("region") for e in events if "region" in e]
    region_counts = dict(Counter(regions))

    # 2. Thống kê chất lượng (Bitrate & Buffering)
    bitrates = [e.get("bitrate_kbps") for e in events if "bitrate_kbps" in e]
    avg_bitrate = sum(bitrates) / len(bitrates) if bitrates else 0

    buffer_ratios = [e.get("buffer_ratio") for e in events if "buffer_ratio" in e]
    avg_buffer = sum(buffer_ratios) / len(buffer_ratios) if buffer_ratios else 0

    # 3. Lọc ra danh sách các video có tình trạng lag (buffer > 0.05)
    lagging_videos = [e.get("video_id") for e in events if e.get("buffer_ratio", 0) > 0.05]
    lag_counts = dict(Counter(lagging_videos))

    # 4. Định dạng Document lưu xuống Distributed Storage (MongoDB)
    metrics_doc = {
        "timestamp": datetime.now(),
        "total_events_processed": len(events),
        "region_counts": region_counts,
        "avg_bitrate_kbps": round(avg_bitrate, 2),
        "avg_buffer_ratio": round(avg_buffer, 4),
        "lagging_videos": lag_counts
    }

    try:
        # Nếu có DB thì ghi vào, nếu chưa bật DB thì bỏ qua để tránh sập code (Chịu lỗi mềm)
        if db_collection is not None:
            db_collection.insert_one(metrics_doc)
        logging.info(f"✅ Đã xử lý {len(events)} events | Avg Bitrate: {avg_bitrate:.0f} | Avg Buffer: {avg_buffer:.3f}")
    except Exception as e:
        logging.error(f"❌ Lỗi ghi vào MongoDB: {e}")

# ─────────────────────────────────────────────
# VÒNG LẶP TIÊU THỤ (CONSUMER LOOP)
# ─────────────────────────────────────────────
def main():
    logging.info(f"Khởi động Processing Node (Consumer Group: {GROUP_ID})...")
    
    # 1. Kết nối Lưu trữ phân tán (MongoDB)
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        mongo_client.admin.command('ping') # Lệnh Ping kiểm tra kết nối
        db = mongo_client["video_analytics"]
        collection = db["streaming_metrics"]
        logging.info(f"Đã kết nối thành công tới MongoDB: {MONGO_URI}")
    except ConnectionFailure:
        logging.warning("⚠️ Không thể kết nối MongoDB. System vẫn chạy nhưng KHÔNG lưu data.")
        collection = None

    # 2. Kết nối Cụm thu thập dữ liệu (Kafka)
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=GROUP_ID, # Các node cùng group sẽ chia sẻ việc đọc (Load balancing)
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        logging.info(f"Đã kết nối Kafka Brokers: {KAFKA_BROKERS}")
    except Exception as e:
        logging.error(f"Lỗi kết nối Kafka: {e}")
        return

    # 3. Lắng nghe và gom cụm dữ liệu
    batch_buffer = []
    last_batch_time = time.time()

    logging.info(f"Đang lắng nghe topics {TOPICS}... Bấm Ctrl+C để dừng.")

    try:
        # poll_timeout_ms=1000 giúp vòng lặp không bị chặn (block), cho phép check thời gian đóng batch
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)
            
            for tp, messages in msg_pack.items():
                for msg in messages:
                    batch_buffer.append(msg.value)

            # Kiểm tra thời gian đã đủ 5 giây để chốt sổ (đóng Batch) chưa
            current_time = time.time()
            if current_time - last_batch_time >= BATCH_WINDOW_SECONDS:
                if len(batch_buffer) > 0:
                    process_batch(batch_buffer, collection)
                    batch_buffer = [] # Reset buffer cho đợt tiếp theo
                else:
                    logging.info("Trạng thái chờ: Không có luồng dữ liệu mới...")
                
                last_batch_time = current_time

    except KeyboardInterrupt:
        logging.info("Processing Node đã được dừng.")
    finally:
        consumer.close()
        if 'mongo_client' in locals():
            mongo_client.close()

if __name__ == "__main__":
    main()