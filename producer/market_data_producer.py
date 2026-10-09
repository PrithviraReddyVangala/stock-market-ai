import os
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("TWELVE_DATA_API_KEY")

SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "TSLA",
    "AMZN"
]


def get_stock_quote(symbol):
    url = "https://api.twelvedata.com/quote"

    params = {
        "symbol": symbol,
        "apikey": API_KEY
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    if "code" in data:
        print(f"API error for {symbol}: {data}")
        return None

    stock_data = {
        "symbol": data.get("symbol"),
        "timestamp": data.get("datetime"),
        "price": float(data.get("close", 0)),
        "open": float(data.get("open", 0)),
        "high": float(data.get("high", 0)),
        "low": float(data.get("low", 0)),
        "volume": int(float(data.get("volume", 0))),
        "previous_close": float(data.get("previous_close", 0)),
        "change": float(data.get("change", 0)),
        "percent_change": float(data.get("percent_change", 0)),
        "received_at": datetime.now().isoformat()
    }

    return stock_data


print("\n===== STOCK MARKET DATA =====\n")

for symbol in SYMBOLS:

    stock = get_stock_quote(symbol)

    if stock:
        print(stock)