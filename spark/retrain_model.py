import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml import Pipeline


jar_path = "/app/spark/postgresql-42.7.7.jar"
builder = SparkSession.builder.appName("CryptoPulse_Model_Retrainer")
if os.path.exists(jar_path):
    builder = builder.config("spark.jars", jar_path)
else:
    builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("🔄 Loading historical data from PostgreSQL for model retraining...")

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://cryptopulse-postgres:5432/cryptopulse_db") \
    .option("dbtable", "historical_prices_1m") \
    .option("user", "cryptopulse") \
    .option("password", "cryptopulse123") \
    .option("driver", "org.postgresql.Driver") \
    .load()


window_spec = Window.partitionBy("symbol").orderBy("timestamp")
window_spec_15m = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-15, 0)
window_spec_1h = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-60, 0)

df_featured = df \
    .withColumn("lag_close_1", F.lag("close", 1).over(window_spec)) \
    .withColumn("lag_close_3", F.lag("close", 3).over(window_spec)) \
    .withColumn("lag_close_5", F.lag("close", 5).over(window_spec)) \
    .withColumn("lag_volume_1", F.lag("volume", 1).over(window_spec)) \
    .withColumn("hl_spread", F.col("high") - F.col("low")) \
    .withColumn("price_change_pct", (F.col("close") - F.col("open")) / F.col("open")) \
    .withColumn("rolling_avg_15m", F.avg("close").over(window_spec_15m)) \
    .withColumn("rolling_avg_1h", F.avg("close").over(window_spec_1h)) \
    .withColumn("volatility_15m", F.stddev("close").over(window_spec_15m))


df_prepared = df_featured \
    .withColumn("next_close", F.lead("close", 1).over(window_spec)) \
    .withColumn("label", F.when(F.col("next_close") > F.col("close"), 1.0).otherwise(0.0)) \
    .dropna()


feature_cols = [
    "open", "high", "low", "close", "volume", 
    "lag_close_1", "lag_close_3", "lag_close_5",
    "lag_volume_1", "hl_spread", "price_change_pct", 
    "rolling_avg_15m", "rolling_avg_1h", "volatility_15m"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")


gbt = GBTClassifier(
    featuresCol="features", 
    labelCol="label", 
    maxDepth=6,
    maxIter=80,          
    stepSize=0.1,       
    seed=42
)

pipeline = Pipeline(stages=[assembler, gbt])


df_prepared = df_prepared.withColumn("ts_unix", F.unix_timestamp("timestamp"))

print(f"🏋️ Training PySpark GBTClassifier model on {df_prepared.count()} samples...")
updated_model = pipeline.fit(df_prepared)


save_path = "/app/models/crypto_gbt_model" if os.path.exists("/app/models") else "models/crypto_gbt_model"
updated_model.write().overwrite().save(save_path)
print(f"✅ GBT Model successfully trained and saved to '{save_path}'!")
