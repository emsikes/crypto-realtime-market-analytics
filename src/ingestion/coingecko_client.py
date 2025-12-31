import logging
from typing import List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from schemas import CryptoPriceMessage, TRACKED_COINS

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CoinGeckoClient:
    """
    Client for CoinGecko API 
    """

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, timeout: int = 10):
        """
        Initialize the client.

        Args:
            timeout: Request timeout in seconds
            session: Used for connection pooling (reuse TCP conn = faster)
        """
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Creates a requests session with retry strategy - APIs are known to have transient failures

        Retries on these status codes:
        - 429: Rate limited
        - 500, 502, 503, 504: Server side errors
        """
        session = requests.Session()

        retry_strategy = Retry(
            total=3,                # Max 3 retries
            backoff_factor=1,       # Wait 1s -> 2s -> 4s to avoid crashing producer
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        return session

    def get_prices(self, coin_ids: List[str] = None) -> List[CryptoPriceMessage]:
        """
        Fetch current prices for specified coins.

        Args:
            coin_ids: List of coin IDs.  Defaults to TRACKED_COINS

        Returns:
            List of CryptoPriceMessage objects
        """
        if coin_ids is None:
            coin_ids = TRACKED_COINS

        url = f"{self.BASE_URL}/simple/price"
        params = {
            "ids": ",".join(coin_ids),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24h_vol": "true",
            "include_24h_change": "true",
            "include_last_updated_at": "true"
        }

        logger.debug(f"Fetching prices for {len(coin_ids)} coins")

        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()  # Raises exception for 4xx/5xx responses

        data = response.json()

        # Log any missing coins
        missing = set(coin_ids) - set(data.keys())
        if missing:
            logger.warning(f"Coins not returned by API: {missing}")

        # Transform each coin's data into our message format
        messages = []
        for coin_id, coin_data in data.items():
            message = CryptoPriceMessage.from_coingecko(coin_data, coin_id)
            messages.append(message)

        logger.info(f"Successfully fetched {len(messages)} prices")
        return messages


if __name__ == "__main__":
    client = CoinGeckoClient()
    prices = client.get_prices()

    print(f"\n{'Coin':<15} {'Price':>12} {'24h Change':>12}")
    print("-" * 40)

    for p in prices:
        print(f"{p.coin_id:<15} ${p.price_usd:>10,.2f} {p.change_24h_pct:>11.2f}%")
