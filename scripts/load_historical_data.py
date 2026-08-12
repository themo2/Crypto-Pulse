import pandas as pd
import os
from pathlib import Path
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert as pg_insert

db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER", "cryptopulse"),
    password=os.getenv("DB_PASSWORD", "cryptopulse123"),
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    database=os.getenv("DB_NAME", "cryptopulse_db"),
)

engine = create_engine(db_url)
data_dir = Path(__file__).resolve().parent.parent / "data" / "historical_data"
historical_prices = Table("historical_prices_1m", MetaData(), autoload_with=engine)
files = [
    "ADAUSDT_1m_1month.csv", "AVAXUSDT_1m_1month.csv", "BNBUSDT_1m_1month.csv", 
    "BTCUSDT_1m_1month.csv", "DOGEUSDT_1m_1month.csv", "DOTUSDT_1m_1month.csv", 
    "ETHUSDT_1m_1month.csv", "LINKUSDT_1m_1month.csv", "SOLUSDT_1m_1month.csv", 
    "XRPUSDT_1m_1month.csv"
]

for file in files:
    symbol = file.split('_')[0] 
    
    df = pd.read_csv(data_dir / file)
    
    df['symbol'] = symbol
    
    df = df[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'number_of_trades']]

    records = df.to_dict(orient="records")
    stmt = pg_insert(historical_prices).values(records)

    with engine.begin() as conn:
        conn.execute(
            stmt.on_conflict_do_nothing(index_elements=["symbol", "timestamp"])
        )

    print(f"Successfully loaded {file} into the database.")