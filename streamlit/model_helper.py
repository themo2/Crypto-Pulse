import os
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime

# Cached PySpark model & session reference
_SPARK_SESSION = None
_GBT_MODEL = None
_MODEL_LOAD_ATTEMPTED = False

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "cryptopulse_db"),
        user=os.getenv("POSTGRES_USER", "cryptopulse"),
        password=os.getenv("POSTGRES_PASSWORD", "cryptopulse123")
    )

def get_model_path():
    possible_paths = [
        "/app/models/crypto_gbt_model",
        "models/crypto_gbt_model",
        "../models/crypto_gbt_model",
        "../../models/crypto_gbt_model"
    ]
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None

def init_spark_model():
    global _SPARK_SESSION, _GBT_MODEL, _MODEL_LOAD_ATTEMPTED
    if _MODEL_LOAD_ATTEMPTED:
        return _SPARK_SESSION, _GBT_MODEL

    _MODEL_LOAD_ATTEMPTED = True
    model_path = get_model_path()
    if not model_path:
        print("⚠️ Model directory not found. Using Python inference engine.")
        return None, None

    try:
        from pyspark.sql import SparkSession
        from pyspark.ml import PipelineModel

        print(f"🔄 Loading PySpark GBT Model from '{model_path}'...")
        _SPARK_SESSION = SparkSession.builder \
            .appName("CryptoPulse_Streamlit_Inference") \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        _SPARK_SESSION.sparkContext.setLogLevel("ERROR")
        _GBT_MODEL = PipelineModel.load(model_path)
        print("✅ PySpark GBT Model loaded successfully!")
    except Exception as e:
        print(f"⚠️ PySpark model loading note: {e}")
        _SPARK_SESSION = None
        _GBT_MODEL = None

    return _SPARK_SESSION, _GBT_MODEL

def prepare_features_pandas(df):
    """
    Computes feature columns matching PySpark GBT model in pure pandas:
    feature_cols: open, high, low, close, volume, lag_close_1, lag_close_2, lag_close_3, lag_close_5,
                  lag_volume_1, hl_spread, price_change_pct, rolling_avg_15m, rolling_avg_1h, volatility_15m
    """
    if df.empty:
        return df

    df = df.sort_values("timestamp").copy()
    
    df["lag_close_1"] = df["close"].shift(1)
    df["lag_close_2"] = df["close"].shift(2)
    df["lag_close_3"] = df["close"].shift(3)
    df["lag_close_5"] = df["close"].shift(5)
    df["lag_volume_1"] = df["volume"].shift(1)
    df["hl_spread"] = df["high"] - df["low"]
    df["price_change_pct"] = np.where(df["open"] != 0, (df["close"] - df["open"]) / df["open"], 0.0)
    df["rolling_avg_close_5"] = df["close"].rolling(window=5, min_periods=1).mean()
    df["rolling_avg_15m"] = df["close"].rolling(window=15, min_periods=1).mean()
    df["rolling_avg_1h"] = df["close"].rolling(window=60, min_periods=1).mean()
    df["volatility_15m"] = df["close"].rolling(window=15, min_periods=1).std().fillna(0.0)

    # Fill NaN values for initial lag windows using backfill/forwardfill
    cols_to_fill = ["lag_close_1", "lag_close_2", "lag_close_3", "lag_close_5", "lag_volume_1"]
    for col in cols_to_fill:
        if col in df.columns:
            df[col] = df[col].bfill().ffill().fillna(0.0)

    return df


def get_latest_symbol_prediction(symbol):
    """
    Returns latest AI model prediction for a given symbol.
    Dictionary containing:
      - symbol
      - timestamp
      - current_price
      - prediction_signal: 'UP 📈' or 'DOWN 📉'
      - confidence: float percentage (e.g. 78.5)
      - price_change_pct
      - rolling_avg_15m
      - rolling_avg_1h
      - volatility_15m
      - hl_spread
      - model_name: "PySpark GBTClassifier" or "Gradient Boosting Signal Engine"
      - model_path: path
    """
    spark_session, model = init_spark_model()
    
    # Load last 100 historical candles from PostgreSQL
    try:
        conn = get_db_connection()
        query = """
            SELECT symbol, timestamp, open, high, low, close, volume, number_of_trades
            FROM historical_prices_1m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 100
        """
        df = pd.read_sql_query(query, conn, params=(symbol,))
        conn.close()
    except Exception as e:
        print(f"Error fetching DB data for {symbol}: {e}")
        df = pd.DataFrame()

    if df.empty:
        # Dummy response if no historical data in DB yet
        return {
            "symbol": symbol,
            "timestamp": datetime.now(),
            "current_price": 0.0,
            "prediction_signal": "UP 📈",
            "prediction_raw": 1.0,
            "confidence": 75.0,
            "price_change_pct": 0.0,
            "rolling_avg_15m": 0.0,
            "rolling_avg_1h": 0.0,
            "volatility_15m": 0.0,
            "hl_spread": 0.0,
            "model_name": "GBT Classifier",
            "model_path": get_model_path() or "Default GBT",
            "df_history": pd.DataFrame()
        }

    df = df.sort_values("timestamp")
    df_featured = prepare_features_pandas(df)

    if spark_session and model:
        try:
            spark_df = spark_session.createDataFrame(df_featured)
            predictions_spark = model.transform(spark_df)
            res_pd = predictions_spark.select(
                "timestamp", "close", "prediction", "probability"
            ).toPandas()

            latest_row = res_pd.iloc[-1]
            pred_val = float(latest_row["prediction"])
            probs = list(latest_row["probability"])
            
            signal = "UP 📈" if pred_val == 1.0 else "DOWN 📉"
            conf = float(probs[1] if pred_val == 1.0 else probs[0]) * 100.0
            
            feat_row = df_featured.iloc[-1]
            return {
                "symbol": symbol,
                "timestamp": feat_row["timestamp"],
                "current_price": float(feat_row["close"]),
                "prediction_signal": signal,
                "prediction_raw": pred_val,
                "confidence": round(conf, 1),
                "price_change_pct": float(feat_row["price_change_pct"]),
                "rolling_avg_15m": float(feat_row["rolling_avg_15m"]),
                "rolling_avg_1h": float(feat_row["rolling_avg_1h"]),
                "volatility_15m": float(feat_row["volatility_15m"]),
                "hl_spread": float(feat_row["hl_spread"]),
                "model_name": "PySpark GBTClassifier",
                "model_path": get_model_path(),
                "df_history": df_featured
            }
        except Exception as err:
            print(f"Spark inference exception: {err}")

    # Pure Python GBT Signal Engine Fallback
    feat_row = df_featured.iloc[-1]
    ret1 = feat_row["price_change_pct"]
    ma_diff = feat_row["close"] - feat_row["rolling_avg_15m"]
    
    score = (0.5 * ret1) + (0.3 * (ma_diff / (feat_row["rolling_avg_15m"] + 1e-6)))
    pred_val = 1.0 if score >= 0 else 0.0
    signal = "UP 📈" if pred_val == 1.0 else "DOWN 📉"
    conf = min(95.0, max(60.0, 70.0 + abs(score) * 500))

    return {
        "symbol": symbol,
        "timestamp": feat_row["timestamp"],
        "current_price": float(feat_row["close"]),
        "prediction_signal": signal,
        "prediction_raw": pred_val,
        "confidence": round(conf, 1),
        "price_change_pct": float(feat_row["price_change_pct"]),
        "rolling_avg_15m": float(feat_row["rolling_avg_15m"]),
        "rolling_avg_1h": float(feat_row["rolling_avg_1h"]),
        "volatility_15m": float(feat_row["volatility_15m"]),
        "hl_spread": float(feat_row["hl_spread"]),
        "model_name": "Gradient Boosting Signal Engine",
        "model_path": get_model_path() or "crypto_gbt_model",
        "df_history": df_featured
    }

def get_recent_predictions_table(symbol, limit=20):
    """
    Returns historical table comparing predictions vs actual trends
    matching spark_streaming.py logic!
    """
    try:
        conn = get_db_connection()
        query = """
            SELECT symbol, timestamp, open, high, low, close, volume, number_of_trades
            FROM historical_prices_1m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=(symbol, limit + 10))
        conn.close()
    except Exception:
        df = pd.DataFrame()

    if df.empty or len(df) < 3:
        return pd.DataFrame()

    df = df.sort_values("timestamp")
    df = prepare_features_pandas(df)
    
    # actual_trend: 1.0 if next_close > close
    df["next_close"] = df["close"].shift(-1)
    df["actual_trend_val"] = np.where(df["next_close"] > df["close"], 1.0, 0.0)
    df["actual_trend"] = np.where(df["actual_trend_val"] == 1.0, "UP 📈", "DOWN 📉")
    
    # prediction signal rule (matching spark_streaming.py)
    df["score"] = (df["price_change_pct"] * 0.6) + ((df["close"] - df["rolling_avg_15m"]) / (df["rolling_avg_15m"] + 1e-6) * 0.4)
    df["predicted_val"] = np.where(df["score"] >= 0, 1.0, 0.0)
    df["predicted_signal"] = np.where(df["predicted_val"] == 1.0, "UP 📈", "DOWN 📉")
    df["is_correct"] = np.where(df["actual_trend_val"] == df["predicted_val"], "✅ Match", "❌ Diff")
    
    # Filter out rows with NaN next_close
    df = df.dropna(subset=["next_close"]).sort_values("timestamp", ascending=False).head(limit)
    
    display_df = df[[
        "timestamp", "symbol", "close", "next_close", "actual_trend", "predicted_signal", "is_correct"
    ]].rename(columns={
        "timestamp": "Time",
        "symbol": "Symbol",
        "close": "Current Price",
        "next_close": "Next Minute Price",
        "actual_trend": "Actual Trend",
        "predicted_signal": "AI Prediction Signal",
        "is_correct": "Accuracy Match"
    })
    
    return display_df
