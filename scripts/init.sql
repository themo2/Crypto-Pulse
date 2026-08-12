CREATE TABLE IF NOT EXISTS historical_prices_1m (
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    number_of_trades INTEGER,
    PRIMARY KEY (symbol, timestamp)
);

CREATE INDEX idx_symbol_time_1m ON historical_prices_1m (symbol, timestamp DESC);