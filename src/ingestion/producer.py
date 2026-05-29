import os
import sys
import time
import json
import random
import hashlib
import logging
from datetime import datetime
from kafka import KafkaProducer

# ─────────────────────────────────────────────
# LOGGING CONFIGURATION
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TelemetryProducer")

# ─────────────────────────────────────────────
# CONSTANTS & MODELS
# ─────────────────────────────────────────────
REGIONS = ["Asia-Pacific", "North-America", "Europe", "South-America"]
EVENT_TYPES = ["play", "pause", "seek", "buffer", "quality_change", "stop"]
VIDEOS = [f"VID_{i:04d}" for i in range(1, 21)]
BITRATES = [480, 720, 1080, 1440, 2160]

class TelemetryProducer:
    """Simulates high-throughput client telemetry events and produces them to Kafka."""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = self._initialize_producer()
        self.total_produced = 0

    def _initialize_producer(self) -> KafkaProducer:
        logger.info(f"Connecting to Kafka broker(s) at: {self.bootstrap_servers}")
        try:
            return KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8'),
                retries=5
            )
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    @staticmethod
    def generate_event() -> dict:
        """Generates a mock video streaming event."""
        return {
            "event_id": hashlib.md5(str(random.random()).encode()).hexdigest()[:8],
            "event_type": random.choice(EVENT_TYPES),
            "video_id": random.choice(VIDEOS),
            "user_id": f"USR_{random.randint(1, 500):04d}",
            "region": random.choice(REGIONS),
            "bitrate_kbps": random.choice(BITRATES),
            "buffer_ratio": round(random.uniform(0, 0.15), 3),
            "timestamp": datetime.now().isoformat(),
        }

    @staticmethod
    def get_routing_topic(event_type: str) -> str:
        """Determines the correct Kafka topic based on the event type."""
        if event_type in ("play", "pause", "stop"):
            return "play_events"
        elif event_type in ("buffer", "quality_change"):
            return "quality_metrics"
        else:
            return "user_actions"

    def run(self, events_per_second: int = 10):
        """Starts the infinite loop to produce telemetry events."""
        logger.info("Starting Data Ingestion... Press Ctrl+C to stop.")
        sleep_interval = 1.0 / events_per_second

        try:
            while True:
                event = self.generate_event()
                topic = self.get_routing_topic(event["event_type"])
                partition_key = event["user_id"]

                self.producer.send(topic, key=partition_key, value=event)
                self.total_produced += 1

                if self.total_produced % 10 == 0:
                    logger.info(f"[Produced: {self.total_produced}] Latest ID={event['event_id']} -> Topic='{topic}'")

                time.sleep(sleep_interval)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user.")
        finally:
            self.shutdown()

    def shutdown(self):
        """Safely flushes and closes the Kafka producer."""
        if hasattr(self, 'producer') and self.producer:
            logger.info("Flushing buffers and closing Kafka connection...")
            self.producer.flush()
            self.producer.close()
            logger.info(f"Shutdown complete. Total events produced: {self.total_produced}")

def main():
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    max_retries = 20
    retry_delay = 5
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempting to start Ingestion Service (Attempt {attempt}/{max_retries})...")
            producer_service = TelemetryProducer(bootstrap_servers=kafka_broker)
            producer_service.run(events_per_second=10)
            break
        except Exception as e:
            logger.error(f"Failed to start Ingestion Service: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.critical("Max retries reached. Exiting with error.")
                sys.exit(1)

if __name__ == "__main__":
    main()
