import signal
import sys
import logging
from datetime import datetime

from coingecko_client import CoinGeckoClient
from kafka_producer import CryptoProducer
from schemas import TRACKED_COINS

"""
Main Kafka Prducer

Continuously fetches crypto prices and publishes to Kafka.
Press Ctrl+C to gracefully shutdown (caught by signal)
"""


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CryptoPriceProducerApp:
    """
    Main application coordinating API fetches and Kafka production.
    """

    def __init__(self, interval_seconds: int = 10):
        """
        Initialize the producer application.

        Args:
            interval_seconds: How often to fetch and puhblish prices
        """
        self.interval = interval_seconds
        self.api_client = CoinGeckoClient()
        self.producer = CryptoProducer()
        self.running = True

        # Metrics
        self.fetch_count = 0
        self.start_time = None

    def fetch_and_publish(self) -> None:
        """
        Single iteration: fetch prices from API, publish to Kafka.
        """
        try:
            # Fetch current prices
            prices = self.api_client.get_prices()

            # Publish each to Kafka
            for price in prices:
                self.producer.produce(
                    key=price.coin_id,
                    value=price.to_json()
                )

            # Wait for delivery confirmation
            self.producer.flush(timeout=5)

            self.fetch_count += 1

        except Exception as e:
            logger.error(f"Error in fetch_and_publish: {e}")

    def shutdown(self, signum=None, frame=None) -> None:
        """
        Handle graceful shutdown on Ctrl+C
        Ctrl+C triggers SIGINT, we catch it and flush remaining messages, print stats, exit
        """
        logger.info("Shutdown signal revieved...")
        self.running = False
        self.producer.flush(timeout=30)

        # Print summary
        runtime = (datetime.now() - self.start_time).total_seconds()
        stats = self.producer.get_stats()

        logger.info(
            f"\n{'='*50}\n"
            f"SHUTDOWN SUMMARY\n"
            f"{'='*50}\n"
            f"Runtime: {runtime:}"
            f"Fetch cycles: {self.fetch_count}\n"
            f"Messages: {stats}\n"
            f"{'='*50}"
        )

        sys.exit(0)

    def run(self) -> None:
        """
        Start the producer application.
        """
        logger.info(
            f"\n{'='*50}\n"
            f"CRYPTO PRICE PRODUCER STARTING\n"
            f"{'='*50}\n"
            f"Coins: {len(TRACKED_COINS)}\n"
            f"Interval: {self.interval} seconds\n"
            f"Topic: crypto-prices-raw\n"
            f"{'='*50}\n"
        )

        # Register shutdown handler
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        self.start_time = datetime.now()

        # Main loop
        while self.running:
            self.fetch_and_publish()

            # Log progress every minute for 6 iterations
            if self.fetch_count % 6 == 0:
                logger.info(
                    f"Progress: {self.fetch_count} cycles | {self.producer.get_stats()}")

            # Wait fot next interval
            if self.running:
                import time
                time.sleep(self.interval)


if __name__ == "__main__":
    app = CryptoPriceProducerApp(interval_seconds=10)
    app.run()
