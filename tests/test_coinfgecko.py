import requests


def test_api():
    response = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            'ids': 'bitcoin,ethereum,solana,xrp',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true',
            'include_last_update_at': 'true'
        }
    )

    print("\n\n----- CoinGecko API Working -----\n\n")
    print("Live Prices: ")
    for coin, data in response.json().items():
        print(
            f" {coin}: ${data['usd']:.2f} (24hr: {data.get('usd_24h_change', 0):.2f}%)")


if __name__ == "__main__":
    test_api()
