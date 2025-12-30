from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import json


TRACKED_COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "polygon",
    "chainlink",
    "avalanche-2",
    "polkadot",
    "uniswap",
    "litecoin"
]


@dataclass
# Using the @dataclass decorator will auto-create __init__, __repr__, __eq__
# Also easily converts to JSON or dict format
# Using type hints will serve as documentation
class CryptoPriceMessage:
    """
    Schema for cryptocurrency price data.
    Each field maps to data we'll get from CoinGecko.
    """
    # Identifiers
    coin_id: str        # e.g. "bitcoin"
    symbol: str         # e.g. "btc"

    # Price data
    price_usd: float
    market_cap_usd: float
    volume_24h_usd: float

    # Change metrics
    change_24h_pct: float
    change_7d_pct: Optional[float]      # May not always be available

    # Timestamp
    last_updated_at: str        # When CoinGecko last updated (ISO format)
    ingestion_timestamp: str    # When we captured (ISO format)

    def to_json(self) -> str:
        """Serialize to JSON string for Kafka."""
        return json.dumps(asdict(self))

    @classmethod
    # Use @classmethod to isolate the API specific parsing logic
    def from_coingecko(cls, coin_data: dict, coin_id: str) -> "CryptoPriceMessage":
        """
        Factory method to create messages from the CoinGecko API response.

        Args:
            coin_data: The nested dict for a single coin from CoinGecko
            coin_id: the coin identifier (e.g. "bitcoin")
        """
        return cls(
            coin_id=coin_id,
            symbol=coin_data.get("symbol", coin_id[:3]),
            price_usd=coin_data.get("usd", 0.0),
            market_cap_usd=coin_data.get("usd_market_cap", 0.0),
            volume_24h_usd=coin_data.get("usd_24h_vol", 0.0),
            change_24h_pct=coin_data.get("usd_24h_change", 0.0),
            change_7d_pct=coin_data.get("usd_7d_change"),
            last_updated_at=datetime.utcfromtimestamp(
                coin_data.get("last_updated_at", datetime.utcnow().timestamp())
            ).isoformat() + "Z",
            ingestion_timestamp=datetime.utcnow().isoformat() + "Z"
        )


if __name__ == "__main__":
    # test creating a message manually
    msg = CryptoPriceMessage(
        coin_id="bitcoin",
        symbol="btc",
        price_usd=78500.00,
        market_cap_usd=850000000000,
        volume_24h_usd=25000000000,
        change_24h_pct=2.5,
        change_7d_pct=-1.2,
        last_updated_at="2025-12-20T10:00:00Z",
        ingestion_timestamp="2025-12-20T10:00:05Z"
    )

    print(msg)
    print(msg.to_json())
