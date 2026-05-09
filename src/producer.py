# producer.py
from kafka import KafkaProducer
import json
from ingestion import (
    fetch_new_data,
    fetch_futures_ohlcv,
    fetch_funding_rates,
    fetch_open_interest
)

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",  # Changed from host.docker.internal
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    api_version=(0, 10, 1)  # Added to prevent Python 3.12 handshake errors
)

def send_rows(topic, rows):
    if not rows:
        print(f"⚠️ No data fetched for topic: {topic}")
        return

    for row in rows:
        producer.send(topic, row)

    producer.flush()
    print(f"✅ Sent {len(rows)} rows to Kafka topic: {topic}")


# -------------------------------
# Spot OHLCV
# -------------------------------
spot_rows = fetch_new_data("BTCUSDT", "1d")
send_rows("ohlcv", spot_rows)

# -------------------------------
# Futures OHLCV
# -------------------------------
futures_rows = fetch_futures_ohlcv("BTCUSDT", "1d")
send_rows("futures_ohlcv", futures_rows)

# -------------------------------
# Funding rates
# -------------------------------
funding_rows = fetch_funding_rates("BTCUSDT")
send_rows("funding_rates", funding_rows)

# -------------------------------
# Open Interest
# -------------------------------
oi_rows = fetch_open_interest("BTCUSDT", "1d")
send_rows("open_interest", oi_rows)
