import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import json
import threading
import uuid
import os
from kafka import KafkaConsumer

st.set_page_config(
    page_title="CryptoPulse Candlestick",
    page_icon="🕯️",
    layout="wide"
)

st.title("🕯️ Live Candlestick Market Stream")
st.caption("Real-time 1-minute streaming candles directly from Kafka")

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "DOTUSDT"
]

@st.cache_resource
def start_kline_consumer():
    # Stores candle history directly from Kafka stream per symbol
    # Dict[symbol, Dict[timestamp_str, candle_dict]]
    kline_history = {s: {} for s in SYMBOLS}
    latest_klines = {}
    lock = threading.Lock()

    def consume():
        # Dynamic group ID ensures instant offset read from Kafka
        group_id = f"streamlit-kline-{uuid.uuid4().hex[:8]}"
        
        kafka_hosts = ["kafka:29092", "cryptopulse-kafka:9092", "kafka:9092", "127.0.0.1:9092"]
        
        consumer = None
        for host in kafka_hosts:
            try:
                consumer = KafkaConsumer(
                    "top10-crypto-live",
                    bootstrap_servers=[host],
                    auto_offset_reset="earliest",
                    group_id=group_id,
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    consumer_timeout_ms=1000
                )
                print(f"Connected Streamlit Kline Consumer to Kafka at {host}")
                break
            except Exception as e:
                print(f"Trying Kafka host {host} failed: {e}")
                continue

        if not consumer:
            print("❌ Streamlit Kline Consumer failed to connect to any Kafka host.")
            return

        while True:
            try:
                for message in consumer:
                    data = message.value
                    if not isinstance(data, dict):
                        continue

                    # Filter for kline events
                    if data.get("type") != "kline":
                        continue

                    symbol = data.get("symbol", "").upper()
                    if symbol not in SYMBOLS:
                        continue

                    ts_str = str(data["timestamp"])
                    candle = {
                        "timestamp": pd.to_datetime(ts_str),
                        "open": float(data["open"]),
                        "high": float(data["high"]),
                        "low": float(data["low"]),
                        "close": float(data["close"]),
                        "volume": float(data["volume"]),
                        "number_of_trades": int(data["number_of_trades"]),
                        "is_closed": bool(data["is_closed"])
                    }

                    with lock:
                        if symbol not in kline_history:
                            kline_history[symbol] = {}
                        
                        kline_history[symbol][ts_str] = candle
                        latest_klines[symbol] = candle

                        if len(kline_history[symbol]) > 300:
                            sorted_keys = sorted(kline_history[symbol].keys())
                            for old_k in sorted_keys[:-300]:
                                del kline_history[symbol][old_k]

            except Exception as loop_err:
                pass

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()

    return kline_history, latest_klines, lock

kline_history, latest_klines, kline_lock = start_kline_consumer()

@st.cache_data(ttl=5)
def load_historical_candles(symbol, limit):
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "cryptopulse_db"),
            user=os.getenv("POSTGRES_USER", "cryptopulse"),
            password=os.getenv("POSTGRES_PASSWORD", "cryptopulse123")
        )

        query = """
            SELECT timestamp, open, high, low, close, volume, number_of_trades
            FROM historical_prices_1m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """

        df = pd.read_sql_query(query, conn, params=(symbol, limit))
        conn.close()

        df = df.sort_values("timestamp")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        print(f"PostgreSQL seed load note: {e}")
        return pd.DataFrame()


col1, col2 = st.columns([2, 1])

with col1:
    symbol = st.selectbox("Cryptocurrency", SYMBOLS, index=0)

with col2:
    candle_count = st.selectbox("Candles Displayed", [30, 50, 100, 200], index=2)

@st.fragment(run_every="1s")
def candlestick_dashboard():
    try:
        with kline_lock:
            symbol_candles_dict = kline_history.get(symbol, {})
            current_live = latest_klines.get(symbol)
            if current_live:
                current_live = current_live.copy()
            candles_list = list(symbol_candles_dict.values())

        if candles_list:
            df_kafka = pd.DataFrame(candles_list)
            df_kafka["timestamp"] = pd.to_datetime(df_kafka["timestamp"])
            df_kafka = df_kafka.sort_values("timestamp")
        else:
            df_kafka = pd.DataFrame()

        # 2. Merge with PostgreSQL historical candles to seed older chart history
        df_db = load_historical_candles(symbol, candle_count)

        if not df_db.empty and not df_kafka.empty:
            # Drop overlapping timestamps in DB, prefer direct Kafka streaming rows
            kafka_timestamps = set(df_kafka["timestamp"])
            df_db_filtered = df_db[~df_db["timestamp"].isin(kafka_timestamps)]
            df = pd.concat([df_db_filtered, df_kafka], ignore_index=True)
        elif not df_kafka.empty:
            df = df_kafka
        else:
            df = df_db

        if df.empty:
            st.warning(f"📡 Waiting for live Kafka stream for {symbol}...")
            return

        df = df.sort_values("timestamp").tail(candle_count)

        # METRICS
        latest = df.iloc[-1]
        previous_close = df.iloc[-2]["close"] if len(df) > 1 else latest["open"]
        difference = latest["close"] - previous_close
        percentage = (difference / previous_close) * 100 if previous_close else 0

        m1, m2, m3, m4, m5 = st.columns(5)

        m1.metric("Close", f"${latest['close']:,.4f}", f"{percentage:+.3f}%")
        m2.metric("Open", f"${latest['open']:,.4f}")
        m3.metric("High", f"${latest['high']:,.4f}")
        m4.metric("Low", f"${latest['low']:,.4f}")

        if current_live is not None:
            status = "CLOSED" if current_live.get("is_closed", False) else "LIVE STREAM 🟢"
            m5.metric("Candle Status", status)
        else:
            m5.metric("Candle Status", "Kafka Streaming...")

        # CANDLESTICK CHART DIRECTLY FROM KAFKA
        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=df["timestamp"],
                    open=df["open"],
                    high=df["high"],
                    low=df["low"],
                    close=df["close"],
                    name=symbol,
                    increasing_line_color="#00C853",
                    decreasing_line_color="#FF1744"
                )
            ]
        )

        fig.update_layout(
            title=f"{symbol} — Real-Time Kafka Stream Candlestick Chart",
            xaxis_title="Time",
            yaxis_title="Price (USDT)",
            xaxis_rangeslider_visible=False,
            height=580,
            margin=dict(l=20, r=20, t=60, b=20)
        )

        st.plotly_chart(fig, use_container_width=True)

        if current_live is not None:
            st.caption("📡 Chart is streaming directly from Kafka topic `top10-crypto-live` in real-time.")

        # DATA TABLE
        with st.expander("View Live Streamed OHLC Data"):
            display_df = df.copy().sort_values("timestamp", ascending=False)
            st.dataframe(display_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error("Error loading the candlestick streaming dashboard")
        st.exception(e)

candlestick_dashboard()