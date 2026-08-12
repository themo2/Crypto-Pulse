import logging
import os
import time
from datetime import datetime, timezone
import requests
from sqlalchemy import MetaData, Table, create_engine, select, func
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert as pg_insert


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)




db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER", "cryptopulse"),
    password=os.getenv("DB_PASSWORD", "cryptopulse123"),
    host=os.getenv("DB_HOST", "cryptopulse-postgres"),
    port=int(os.getenv("DB_PORT", "5432")),
    database=os.getenv("DB_NAME", "cryptopulse_db"),
)

engine = create_engine(db_url, pool_pre_ping=True)
metadata = MetaData()
historical_prices = Table("historical_prices_1m", metadata, autoload_with=engine)

SYMBOLS = [
    "ADAUSDT", "AVAXUSDT", "BNBUSDT", "BTCUSDT", "DOGEUSDT", 
    "DOTUSDT", "ETHUSDT", "LINKUSDT", "SOLUSDT", "XRPUSDT"
]




def upsert_records(records):
    """Inserts records into PostgreSQL, ignoring conflicts on (symbol, timestamp)."""
    if not records:
        return
    
    stmt = pg_insert(historical_prices).values(records)
    upsert_stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "timestamp"])
    
    with engine.begin() as conn:
        conn.execute(upsert_stmt)

def get_latest_timestamp(symbol):
    """Fetches the latest timestamp stored in DB for a given symbol (returns epoch ms or None)."""
    with engine.connect() as conn:
        stmt = select(func.max(historical_prices.c.timestamp)).where(
            historical_prices.c.symbol == symbol
        )
        max_ts = conn.execute(stmt).scalar()
        
    if max_ts:
        # Convert datetime object to UTC timestamp in milliseconds
        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
        return int(max_ts.timestamp() * 1000)
    return None




def backfill_gaps_for_symbol(symbol):
    """Checks for data gaps in PostgreSQL and fetches missing records from Binance API."""
    latest_ms = get_latest_timestamp(symbol)
    
    if latest_ms is None:
        logging.info(f"[{symbol}] No existing records found in DB. Please run your CSV seed script first.")
        return

    # Fetch starting 1 ms after the last saved candle
    start_time = latest_ms + 1
    current_time = int(time.time() * 1000)
    
    # Only fetch if the gap is greater than 1 minute (60,000 ms)
    if current_time - start_time < 60000:
        logging.info(f"[{symbol}] Database is already up to date.")
        return

    logging.info(f"[{symbol}] Gap detected! Backfilling missing records starting from {start_time}...")
    
    url = "https://api.binance.com/api/v3/klines"
    records_to_insert = []
    total_inserted = 0

    while start_time < current_time:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_time,
            "limit": 1000  # Max limit per request in Binance API
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                logging.error(f"[{symbol}] API error: {response.status_code}")
                break
                
            klines = response.json()
            if not klines:
                break

            for k in klines:
                # Binance kline structure: [open_time, open, high, low, close, volume, close_time, qav, trades, ...]
                open_time_ms = k[0]
                
                # Stop if we accidentally fetch a candle that hasn't fully closed yet
                if open_time_ms > current_time - 60000:
                    continue 

                dt_str = datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                
                records_to_insert.append({
                    "symbol": symbol,
                    "timestamp": dt_str,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "number_of_trades": int(k[8])
                })

            # Prepare startTime for the next batch (close_time + 1ms)
            start_time = klines[-1][6] + 1
            
            # Batch upsert to PostgreSQL every 1,000 records to save memory
            if len(records_to_insert) >= 1000:
                upsert_records(records_to_insert)
                total_inserted += len(records_to_insert)
                logging.info(f"[{symbol}] Appended {len(records_to_insert)} records to PostgreSQL...")
                records_to_insert = []
                
        except Exception as e:
            logging.error(f"[{symbol}] Error during backfill: {e}")
            break

    # Insert any remaining records that didn't hit the 1,000 threshold
    if records_to_insert:
        upsert_records(records_to_insert)
        total_inserted += len(records_to_insert)

    logging.info(f"[{symbol}] Completed gap backfill. Total rows added: {total_inserted}")

def run_all_gap_checks():
    """Runs gap checking sequentially across all target symbols."""
    logging.info("--- Starting Database Gap Check ---")
    for symbol in SYMBOLS:
        backfill_gaps_for_symbol(symbol)
    logging.info("--- Gap Check Complete ---")




if __name__ == "__main__":
    run_all_gap_checks()