# ingestion.py
import psycopg2
import pandas as pd
from binance.client import Client
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
client = Client(API_KEY, API_SECRET)

PG_KW = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

# ------------------------------------------------------------------
# Generic DB helpers
# ------------------------------------------------------------------

def _get_last_timestamp(table, symbol):
    conn = psycopg2.connect(**PG_KW)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(ts) FROM {table} WHERE symbol=%s;",
                (symbol,)
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def upsert_rows_to_db(rows, table_name, pg_kw=PG_KW):
    if not rows:
        return

    columns = rows[0].keys()
    quoted_columns = [f'"{col}"' for col in columns]
    values = [[row[col] for col in columns] for row in rows]

    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(quoted_columns)})
        VALUES %s
        ON CONFLICT ("symbol", "ts") DO UPDATE SET
        {', '.join([
            f'"{col}" = EXCLUDED."{col}"'
            for col in columns if col not in ('symbol', 'ts')
        ])}
    """

    conn = psycopg2.connect(**pg_kw)
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
        conn.commit()
    finally:
        conn.close()

# ------------------------------------------------------------------
# Spot OHLCV (UNCHANGED LOGIC)
# ------------------------------------------------------------------

def get_last_timestamp(symbol):
    return _get_last_timestamp("ohlcv", symbol)


def fetch_new_data(symbol, interval="1d"):
    last_ts = get_last_timestamp(symbol)

    if last_ts is None:
        start_str = "1 Jan, 2010"
    else:
        start_str = (last_ts + pd.Timedelta(seconds=1)) \
            .strftime("%d %b, %Y %H:%M:%S")

    klines = client.get_historical_klines(
        symbol, interval, start_str=start_str
    )

    if not klines:
        return []

    df = pd.DataFrame(klines, columns=[
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close Time', 'Quote Asset Volume', 'Number of Trades',
        'Taker Buy Base Asset Volume',
        'Taker Buy Quote Asset Volume', 'Ignore'
    ])

    df['ts'] = pd.to_datetime(df['Open Time'], unit='ms')
    df['open'] = df['Open'].astype(float)
    df['high'] = df['High'].astype(float)
    df['low'] = df['Low'].astype(float)
    df['close'] = df['Close'].astype(float)
    df['volume'] = df['Volume'].astype(float)
    df['symbol'] = symbol

    return df[['symbol','ts','open','high','low','close','volume']] \
        .to_dict(orient='records')

# ------------------------------------------------------------------
# Futures OHLCV
# ------------------------------------------------------------------

def get_last_timestamp_futures(symbol):
    return _get_last_timestamp("futures_ohlcv", symbol)


def fetch_futures_ohlcv(symbol, interval="1d"):
    last_ts = get_last_timestamp_futures(symbol)

    start_str = "1 Jan, 2010" if last_ts is None else \
        (last_ts + pd.Timedelta(seconds=1)) \
        .strftime("%d %b, %Y %H:%M:%S")

    klines = client.futures_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=start_str
    )

    if not klines:
        return []

    df = pd.DataFrame(klines, columns=[
        'Open Time','Open','High','Low','Close','Volume',
        'Close Time','Quote Asset Volume',
        'Number of Trades',
        'Taker Buy Base','Taker Buy Quote','Ignore'
    ])

    df['ts'] = pd.to_datetime(df['Open Time'], unit='ms')

    return [{
        'symbol': symbol,
        'ts': row['ts'],
        'open': float(row['Open']),
        'high': float(row['High']),
        'low': float(row['Low']),
        'close': float(row['Close']),
        'volume': float(row['Volume'])
    } for _, row in df.iterrows()]

# ------------------------------------------------------------------
# Funding rates
# ------------------------------------------------------------------

def get_last_timestamp_funding(symbol):
    return _get_last_timestamp("funding_rates", symbol)


def fetch_funding_rates(symbol):
    data = client.futures_funding_rate(symbol=symbol, limit=1000)

    if not data:
        return []

    return [{
        'symbol': symbol,
        'ts': pd.to_datetime(d['fundingTime'], unit='ms'),
        'funding_rate': float(d['fundingRate'])
    } for d in data]

# ------------------------------------------------------------------
# Open Interest
# ------------------------------------------------------------------

def get_last_timestamp_open_interest(symbol):
    return _get_last_timestamp("open_interest", symbol)


def fetch_open_interest(symbol, interval="1d"):
    data = client.futures_open_interest_hist(
        symbol=symbol,
        period=interval,
        limit=500
    )

    if not data:
        return []

    return [{
        'symbol': symbol,
        'ts': pd.to_datetime(d['timestamp'], unit='ms'),
        'open_interest': float(d['sumOpenInterest'])
    } for d in data]
