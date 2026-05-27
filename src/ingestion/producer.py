import os
import time
import json
import random
import hashlib
import logging
from datetime import datetime
from kafka import KafkaProducer

# Thiết lập logging để theo dõi luồng dữ liệu trên Terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ─────────────────────────────────────────────
# DATA MODELS (Tái sử dụng từ code mẫu)
# ─────────────────────────────────────────────
REGIONS = ["Asia-Pacific", "North-America", "Europe", "South-America"]
EVENT_TYPES = ["play", "pause", "seek", "buffer", "quality_change", "stop"]
VIDEOS = [f"VID_{i:04d}" for i in range(1, 21)]

def gen_event():
    """Tạo ra một event xem video ngẫu nhiên"""
    return {
        "event_id": hashlib.md5(str(random.random()).encode()).hexdigest()[:8],
        "event_type": random.choice(EVENT_TYPES),
        "video_id":   random.choice(VIDEOS),
        "user_id":    f"USR_{random.randint(1, 500):04d}",
        "region":     random.choice(REGIONS),
        "bitrate_kbps": random.choice([480, 720, 1080, 1440, 2160]),
        "buffer_ratio": round(random.uniform(0, 0.15), 3),
        "timestamp":  datetime.now().isoformat(),
    }

def get_topic(event_type):
    """Phân loại topic dựa trên loại sự kiện (Routing)"""
    if event_type in ("play", "pause", "stop"):
        return "play_events"
    elif event_type in ("buffer", "quality_change"):
        return "quality_metrics"
    else:
        return "user_actions"

# ─────────────────────────────────────────────
# KAFKA PRODUCER MAIN LOGIC
# ─────────────────────────────────────────────
def main():
    # Lấy địa chỉ Kafka Broker từ biến môi trường (Mặc định là localhost cho local dev)
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    logging.info(f"Đang kết nối tới Kafka broker tại: {kafka_broker}")
    
    # Khởi tạo Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            retries=5  # Cơ chế Fault Tolerance đơn giản ở phía client
        )
    except Exception as e:
        logging.error(f"Lỗi kết nối Kafka: {e}")
        return

    logging.info("Bắt đầu Data Ingestion... Bấm Ctrl+C để dừng.")
    
    try:
        total_produced = 0
        while True:
            # 1. Sinh dữ liệu
            event = gen_event()
            
            # 2. Xác định Kafka Topic & Partition Key
            topic = get_topic(event["event_type"])
            key = event["user_id"] # Dùng user_id làm key để đảm bảo cùng 1 user vào cùng 1 partition
            
            # 3. Gửi vào Kafka
            producer.send(topic, key=key, value=event)
            total_produced += 1
            
            # 4. In log (giới hạn in ra để không spam console, cứ 10 tin thì in 1 lần)
            if total_produced % 10 == 0:
                logging.info(f"[Đã gửi {total_produced} events] Mới nhất: ID={event['event_id']} -> Topic='{topic}'")
            
            # Giả lập luồng dữ liệu liên tục: 10 messages/giây
            time.sleep(0.1) 
            
    except KeyboardInterrupt:
        logging.info("Producer đã được dừng bởi người dùng.")
    finally:
        # Xả bộ đệm và đóng kết nối an toàn
        producer.flush()
        producer.close()
        logging.info(f"Đóng kết nối. Tổng số sự kiện đã sinh: {total_produced}")

if __name__ == "__main__":
    main()