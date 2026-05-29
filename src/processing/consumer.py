import os
import sys
import json
import time
import logging
from datetime import datetime
from collections import Counter
from typing import List, Dict

from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ─────────────────────────────────────────────
# LOGGING CONFIGURATION
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("StreamProcessor")

# ─────────────────────────────────────────────
# CONSTANTS & CONFIGURATION
# ─────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092,localhost:9093").split(',')
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0")
GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "video-analytics-group")
TOPICS = ["play_events", "quality_metrics", "user_actions"]
BATCH_WINDOW_SECONDS = 5

class StreamProcessor:
    """Consumes Kafka events, processes them in micro-batches, and stores metrics in MongoDB."""

    def __init__(self):
        self.mongo_client = None
        self.db_collection = self._initialize_mongodb()
        self.consumer = self._initialize_kafka()

    def _initialize_mongodb(self):
        """Initializes connection to the MongoDB Replica Set."""
        try:
            self.mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
            self.mongo_client.admin.command('ping')
            db = self.mongo_client["video_analytics"]
            logger.info(f"Successfully connected to MongoDB: {MONGO_URI}")
            return db["streaming_metrics"]
        except ConnectionFailure:
            logger.warning("⚠️ MongoDB connection failed. Processing will continue in-memory (No storage).")
            return None

    def _initialize_kafka(self) -> KafkaConsumer:
        """Initializes the Kafka Consumer with the specified group for load balancing."""
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            logger.info(f"Kafka Consumer initialized for topics: {TOPICS}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka Brokers: {e}")
            raise

    def process_batch(self, events: List[Dict]):
        """Aggregates metrics for a given batch of events and persists to storage."""
        if not events:
            return

        # 1. Aggregate Regions
        regions = [e.get("region") for e in events if "region" in e]
        region_counts = dict(Counter(regions))

        # 2. Aggregate Quality Metrics
        bitrates = [e.get("bitrate_kbps") for e in events if "bitrate_kbps" in e]
        avg_bitrate = sum(bitrates) / len(bitrates) if bitrates else 0

        buffer_ratios = [e.get("buffer_ratio") for e in events if "buffer_ratio" in e]
        avg_buffer = sum(buffer_ratios) / len(buffer_ratios) if buffer_ratios else 0

        # 3. Identify Problematic Streams
        lagging_videos = [e.get("video_id") for e in events if e.get("buffer_ratio", 0) > 0.05]
        lag_counts = dict(Counter(lagging_videos))

        # 4. Construct Document
        metrics_doc = {
            "timestamp": datetime.now(),
            "total_events_processed": len(events),
            "region_counts": region_counts,
            "avg_bitrate_kbps": round(avg_bitrate, 2),
            "avg_buffer_ratio": round(avg_buffer, 4),
            "lagging_videos": lag_counts
        }

        # 5. Persist to Distributed Storage
        try:
            if self.db_collection is not None:
                self.db_collection.insert_one(metrics_doc)
            logger.info(f"✅ Processed {len(events)} events | Avg Bitrate: {avg_bitrate:.0f} kbps | Avg Buffer: {avg_buffer:.3f}")
        except Exception as e:
            logger.error(f"❌ Failed to persist to MongoDB: {e}")

    def run(self):
        """Main polling loop for micro-batch stream processing."""
        logger.info(f"Starting Processing Worker (Group: {GROUP_ID}). Press Ctrl+C to stop.")
        batch_buffer = []
        last_batch_time = time.time()

        try:
            while True:
                # Non-blocking poll
                msg_pack = self.consumer.poll(timeout_ms=1000)
                
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        batch_buffer.append(msg.value)

                # Check micro-batch tumbling window
                current_time = time.time()
                if current_time - last_batch_time >= BATCH_WINDOW_SECONDS:
                    if len(batch_buffer) > 0:
                        self.process_batch(batch_buffer)
                        batch_buffer.clear()
                    else:
                        logger.info("Idle state: Awaiting telemetry data...")
                    
                    last_batch_time = current_time

        except KeyboardInterrupt:
            logger.info("Processing Worker interrupted by user.")
        finally:
            self.shutdown()

    def shutdown(self):
        """Gracefully closes all distributed connections."""
        logger.info("Initiating graceful shutdown...")
        if hasattr(self, 'consumer') and self.consumer:
            self.consumer.close()
        if hasattr(self, 'mongo_client') and self.mongo_client:
            self.mongo_client.close()
        logger.info("Shutdown complete.")

def main():
    max_retries = 20
    retry_delay = 5
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempting to start Processing Service (Attempt {attempt}/{max_retries})...")
            processor = StreamProcessor()
            processor.run()
            break
        except Exception as e:
            logger.error(f"Failed to start Processing Service: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.critical("Max retries reached. Exiting with error.")
                sys.exit(1)

if __name__ == "__main__":
    main()
