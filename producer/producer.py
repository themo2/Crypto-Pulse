import json
import time
import websocket
from kafka import KafkaProducer
import sys
from datetime import datetime
import pandas as pd
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import URL



COINS = [
    "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt", 
    "adausdt", "dogeusdt", "avaxusdt", "linkusdt", "dotusdt"
]


streams = []
for coin in COINS:
    streams.append(f"{coin}@ticker")       # Real-time price and stats
    streams.append(f"{coin}@kline_1m")     # 1-minute candle


STREAM_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)


print("🔄 Connecting to Kafka Server...")
try:
    bootstrap_servers = ["kafka:29092", "cryptopulse-kafka:29092"]
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Connected to Kafka Successfully!")
except Exception as e:
    print(f"❌ Failed to connect to Kafka. Error: {e}")
    sys.exit()

KAFKA_TOPIC = "top10-crypto-live"
DB_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER", "cryptopulse"),
    password=os.getenv("DB_PASSWORD", "cryptopulse123"),
    host=os.getenv("DB_HOST", "cryptopulse-postgres"),
    port=int(os.getenv("DB_PORT", "5432")),
    database=os.getenv("DB_NAME", "cryptopulse_db"),
)
DB_ENGINE = create_engine(DB_URL)
HISTORICAL_PRICES = Table("historical_prices_1m", MetaData(), autoload_with=DB_ENGINE)
BINANCE_URL = os.getenv("BINANCE_API_URL", "https://data-api.binance.vision/api/v3/klines")
BINANCE_FALLBACK_URL = "https://api.binance.com/api/v3/klines"
BINANCE_INTERVAL = "1m"
START_FALLBACK_MS = 1672531200000

session = requests.Session()
retry_strategy = Retry(
    total=3,
    connect=3,
    read=3,
    status=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))


def upsert_historical_candle(candle):
    record = {
        "symbol": candle["symbol"],
        "timestamp": pd.to_datetime(candle["timestamp"]),
        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
        "volume": candle["volume"],
        "number_of_trades": candle["number_of_trades"],
    }

    stmt = pg_insert(HISTORICAL_PRICES).values([record])
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol", "timestamp"],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
            "number_of_trades": stmt.excluded.number_of_trades,
        },
    )

    with DB_ENGINE.begin() as conn:
        conn.execute(stmt)


def fetch_missing_rows(symbol):
    with DB_ENGINE.connect() as conn:
        last_timestamp = conn.execute(
            text("SELECT MAX(timestamp) FROM historical_prices_1m WHERE symbol = :sym"),
            {"sym": symbol},
        ).scalar()

    if last_timestamp is not None:
        start_time_ms = int(last_timestamp.timestamp() * 1000) + 60000
    else:
        start_time_ms = START_FALLBACK_MS

    params = {"symbol": symbol, "interval": BINANCE_INTERVAL, "startTime": str(start_time_ms)}
    response = None
    last_error = None

    for url in (BINANCE_URL, BINANCE_FALLBACK_URL):
        try:
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as exc:
            last_error = exc
            response = None

    if response is None:
        print(f"[{symbol}] Failed to fetch missing rows: {last_error}")
        return 0

    payload = response.json()
    if not payload:
        print(f"[{symbol}] No missing rows to add.")
        return 0

    new_data = pd.DataFrame(payload)
    new_data.columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "number_of_trades", "taker_base_vol",
        "taker_quote_vol", "ignore"
    ]
    new_data = new_data[["open_time", "open", "high", "low", "close", "volume", "number_of_trades"]].copy()
    new_data["timestamp"] = pd.to_datetime(new_data["open_time"], unit="ms")
    new_data.drop(columns=["open_time"], inplace=True)
    new_data["symbol"] = symbol
    numeric_cols = ["open", "high", "low", "close", "volume", "number_of_trades"]
    new_data[numeric_cols] = new_data[numeric_cols].apply(pd.to_numeric)

    records = new_data[["symbol", "timestamp", "open", "high", "low", "close", "volume", "number_of_trades"]].to_dict(orient="records")
    stmt = pg_insert(HISTORICAL_PRICES).values(records)

    with DB_ENGINE.begin() as conn:
        conn.execute(stmt.on_conflict_do_nothing(index_elements=["symbol", "timestamp"]))

    print(f"[{symbol}] Added {len(new_data)} missing rows to historical_prices_1m.")
    return len(new_data)

def on_message(ws, message):
    try:
        raw_data = json.loads(message)
        
        # Ensure message originates from the Multiplex stream
        if "stream" in raw_data and "data" in raw_data:
            stream_name = raw_data["stream"]
            payload = raw_data["data"]
            
            # Extract cryptocurrency symbol and stream type
            symbol = stream_name.split('@')[0].upper()
            stream_type = stream_name.split('@')[1]
            
            msg = None
            
            # 4. Format data
            if stream_type == "ticker":
                msg = {
                    "type": "ticker",
                    "symbol": symbol,
                    "price": float(payload['c']),
                    "change_pct": float(payload['P']),
                    "volume": float(payload['v'])
                }
            
            elif stream_type == "kline_1m":
                k = payload['k']
                
                # Convert candle timestamp to CSV-compatible format
                candle_time = datetime.utcfromtimestamp(k['t'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                
                msg = {
                    "type": "kline",
                    "symbol": symbol,
                    "timestamp": candle_time,
                    "open": float(k['o']),
                    "high": float(k['h']),
                    "low": float(k['l']),
                    "close": float(k['c']),
                    "volume": float(k['v']),
                    "number_of_trades": int(k['n']),
                    "is_closed": k['x']
                }
            
            # 5. Send data to Kafka silently
            if msg:
                producer.send(KAFKA_TOPIC, msg)
                print(f"📡 [Kafka Stream] Published {msg['type'].upper()} for {msg['symbol']} | Price: {msg.get('price') or msg.get('close')}")
                if msg["type"] == "kline" and msg["is_closed"]:
                    try:
                        upsert_historical_candle(msg)
                    except Exception as db_error:
                        print(f"[{msg['symbol']}] Failed to write closed candle to Postgres: {db_error}")
                
    except Exception as e:
        # Ignore parsing errors to maintain connection stability
        pass

def on_error(ws, error):
    print(f"❌ Connection error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("\n🔴 Connection closed. Reconnecting in 5 seconds...")
    time.sleep(5)
    start_ws()

def on_open(ws):
    print("🟢 Connected to Binance Multiplex Stream!")
    print(f"📡 Fetching Live Tickers and 1-Minute Klines for {len(COINS)} top coins...")
    print(f"🚀 Data is streaming silently to Kafka topic: '{KAFKA_TOPIC}'...")

def start_ws():
    ws = websocket.WebSocketApp(STREAM_URL, 
                                on_open=on_open, 
                                on_message=on_message, 
                                on_error=on_error, 
                                on_close=on_close)
    ws.run_forever()

if __name__ == "__main__":
    import threading
    def run_initial_gap_fill():
        for coin in [coin.upper() for coin in COINS]:
            try:
                fetch_missing_rows(coin)
            except Exception as e:
                print(f"Initial gap fill note for {coin}: {e}")

    threading.Thread(target=run_initial_gap_fill, daemon=True).start()
    start_ws()
