import json
import os
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
from ingestion import upsert_rows_to_db

load_dotenv()

# --- Configuration ---
TOPIC_TABLE_MAP = {
    "ohlcv": "ohlcv",
    "futures_ohlcv": "futures_ohlcv",
    "funding_rates": "funding_rates",
    "open_interest": "open_interest"
}

# --- Consumer Initialization ---
# Using group_id=None is the most stable way to bypass WSL/Docker metadata hangs.
# It ensures the script immediately starts reading from the 'earliest' offset.
consumer = KafkaConsumer(
    *TOPIC_TABLE_MAP.keys(),
    bootstrap_servers=["127.0.0.1:9092"],  # Use explicit IP
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",          # Catch up on all backlog
    group_id=None,                         # Bypasses group join rebalance hangs
    api_version=(0, 10, 1),                # Essential for Python 3.12 compatibility
    enable_auto_commit=False,              # Not needed for group_id=None
    security_protocol="PLAINTEXT"
)

print(f"🚀 Started! Listening to Kafka: {list(TOPIC_TABLE_MAP.keys())}")

# --- Main Message Loop ---
try:
    for message in consumer:
        topic = message.topic
        row = message.value
        table = TOPIC_TABLE_MAP.get(topic)

        # 1. Basic data validation
        if not isinstance(row, dict):
            print(f"❌ DATA ERROR [{topic}] → Not a dict: {row}")
            continue

        if "symbol" not in row or "ts" not in row:
            print(f"❌ DATA ERROR [{topic}] → Missing keys in row: {row}")
            continue

        # 2. Database Upsert
        try:
            # We wrap row in a list because upsert_rows_to_db expects a list
            upsert_rows_to_db([row], table_name=table)
            print(f"✅ UPSERT [{topic}] → {row.get('symbol')} @ {row.get('ts')}")
        except Exception as e:
            print(f"❌ DB UPSERT ERROR")
            print(f"   Topic : {topic}")
            print(f"   Table : {table}")
            print(f"   Error : {repr(e)}")

except KeyboardInterrupt:
    print("\n👋 Consumer stopped manually.")
except Exception as e:
    print(f"❌ CONSUMER CRITICAL ERROR: {repr(e)}")
finally:
    consumer.close()