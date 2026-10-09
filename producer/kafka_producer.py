import os
import json
import time
import requests

from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.getenv("TWELVE_DATA_API_KEY")

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "stock-market"

SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "TSLA",
    "AMZN"
]


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def get_stock_quote(symbol):

    url = "https://api.twelvedata.com/quote"

    params = {
        "symbol": symbol,
        "apikey": API_KEY
    }

    response = requests.get(
        url,
        params=params,
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    if "code" in data:
        print(f"API error for {symbol}: {data}")
        return None

    # Generate timestamp when the data is received
    current_time = datetime.now().isoformat()

    stock_data = {
        "symbol": data.get("symbol"),

        # Timestamp of when our pipeline received the quote
        "timestamp": current_time,

        "price": float(data.get("close", 0)),
        "open": float(data.get("open", 0)),
        "high": float(data.get("high", 0)),
        "low": float(data.get("low", 0)),
        "volume": int(float(data.get("volume", 0))),
        "previous_close": float(data.get("previous_close", 0)),
        "change": float(data.get("change", 0)),
        "percent_change": float(data.get("percent_change", 0)),

        # Timestamp for ingestion
        "received_at": current_time
    }

    return stock_data


print("Kafka Producer started...")
print("Sending stock data to Kafka...\n")


while True:

    for symbol in SYMBOLS:

        try:

            stock_data = get_stock_quote(symbol)

            if stock_data:

                producer.send(
                    KAFKA_TOPIC,
                    value=stock_data
                )

                producer.flush()

                print(f"Sent: {stock_data}")

        except Exception as e:

            print(f"Error processing {symbol}: {e}")

    print("\nWaiting 5 minutes...\n")

    time.sleep(300)