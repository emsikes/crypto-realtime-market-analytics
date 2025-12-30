import os
import logging
from typing import Optional
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaError

load_dotenv(override=True)

logger = logging.getLogger(__name__)


class CryptoProducer:
    """
    Kafka producer configured for reliable message delivery
    """

    def __init__(self, topic: str = "crypto-prices-raw"):
        """
        Initialize producer with Confluent Cloud configuration

        Args:
            topic: Target Kafka topic name
        """
        self.topic = topic
        self.producer = self._create_producer()

        # Track delivery metrics
        self.messages_sent = 0
        self.messages_failed = 0

    def _create_producer(self) -> Producer:
        """
        Create producer
        """
        config = {
            # Connection settings
            'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVERS'),
            'sasl.username':     os.getenv('CONFLUENT_API_KEY'),
            'sasl.password':     os.getenv('CONFLEUNT_API_SECRET'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms':   'PLAIN',

            # Reliability settings
            'enable.idempotence': True,     # Exactly once delivery
            'acks': 'all',                  # Wait for all replicas to confirm
            'retries': 3,

            # Performance
            'compression.type': 'snappy',
            'linger.ms': 5,     # Small batching window
        }

        return Producer(config)

    def _delivery_callback(self, err: Optional[KafkaError], msg) -> None:
        """
        Callback invoked when message dlivery is consumed or fails.
        Kafka produce() is asynchronous and retuens immeidately - we get the outcome later
        """
        if err is not None:
            logger.error(f"Delivery failed for {msg.key()}: {err}")
            self.messages_failed += 1
        else:
            logger.debug(
                f"Delivered: {msg.key().decode('utf-8')}"
                f"[partition={msg.partition()}, offset={msg.offset()}]"
            )
            self.messages_sent += 1

    def produce(self, key: str, value: str) -> None:
        """
        Send message to Kafka

        Args:
            key: Message key (coin_id) - determins partition 
                - e.g. all Bitcoin go to the same parition
                - Guaranteed ordering and preserves sequence
            value: JSON serialized message body
        """
        self.producer.produce(
            topic=self.topic,
            key=key.encode('utf-8'),
            value=value.encode('utf-8'),
            callback=self._delivery_callback
        )
        # Trigger any pending callbacks
        self.producer.poll(0)

    def flush(self, timeout: float = 10) -> int:
        """
        Wait for all pending messages to be delivered.
        Critical before shutdown or messages could be lost.

        Returns:
            Number of messages still in queue (0 = all delivered)
        """
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush.")
        return remaining

    def get_stats(self) -> dict:
        """
        Return delivery statistics
        """
        total = self.messages_sent + self.messages_failed
        return {
            "sent": self.messages_sent,
            "failed": self.messages_failed,
            "success_rate": f"{(self.messages_sent / total * 100):.1f}%" if total > 0 else "N/A"
        }


if __name__ == "__main__":
    from schemas import CryptoPriceMessage

    producer = CryptoProducer()

    # Generate a test message
    test_msg = CryptoPriceMessage(
        coin_id="bitcoin",
        symbol="btc",
        price_usd=99000.00,
        market_cap_usd=850000000000,
        volume_24h_usd=25000000000,
        change_24h_pct=2.5,
        change_7d_pct=-1.2,
        last_updated_at="2025-12-20T10:00:00Z",
        ingestion_timestamp="2025-12-20T10:00:05Z"
    )

    # Send to Confluent Cloud
    producer.produce(key=test_msg.coin_id, value=test_msg.to_json())
    producer.flush()

    print(f"Stats: {producer.get_stats()}")
