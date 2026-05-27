# ingestion/base_connector.py

import json
import logging
import time
from abc import ABC, abstractmethod
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

class BaseConnector(ABC):
    """
    Base class for all GTM data source connectors.
    Every connector (Apollo, HubSpot, Salesforce) extends this.
    """

    def __init__(self, topic: str):
        self.topic = topic
        self.logger = logging.getLogger(self.__class__.__name__)
        self.producer = self._create_producer()

    def _create_producer(self) -> KafkaProducer:
        """Create and return a Kafka producer with retry logic."""
        retries = 5
        for attempt in range(retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=os.getenv(
                        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
                    ),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",              # wait for all replicas
                    retries=3,              # retry failed sends
                    max_block_ms=10000,     # 10s timeout
                )
                self.logger.info(f"Kafka producer connected on attempt {attempt + 1}")
                return producer
            except Exception as e:
                self.logger.warning(f"Kafka connection attempt {attempt + 1} failed: {e}")
                time.sleep(3)
        raise ConnectionError("Could not connect to Kafka after 5 attempts")

    def send_to_kafka(self, record: dict, key: str = None):
        """Send a single record to the Kafka topic."""
        try:
            future = self.producer.send(self.topic, value=record, key=key)
            future.get(timeout=10)  # wait for confirmation
            self.logger.info(f"Sent record to topic '{self.topic}' | key={key}")
        except Exception as e:
            self.logger.error(f"Failed to send record: {e}")

    def send_batch(self, records: list, key_field: str = None):
        """Send a list of records to Kafka."""
        success, failed = 0, 0
        for record in records:
            try:
                key = str(record.get(key_field)) if key_field else None
                self.send_to_kafka(record, key=key)
                success += 1
            except Exception as e:
                self.logger.error(f"Failed record: {e}")
                failed += 1
        self.logger.info(f"Batch complete — sent: {success} | failed: {failed}")

    def flush(self):
        """Flush all pending messages."""
        self.producer.flush()
        self.logger.info("Producer flushed")

    @abstractmethod
    def fetch_data(self) -> list:
        """Each connector must implement this to pull from its API."""
        pass

    @abstractmethod
    def run(self):
        """Each connector must implement the main run loop."""
        pass