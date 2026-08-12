from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline


spark = SparkSession.builder \
    .appName("CryptoPulse_Direction_Classifier") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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
    .withColumn("volatility_15m", F.stddev("close").over(window_spec_15m)) # Volatility over the past 15 minutes


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
    maxDepth=6,          # Suitable depth to prevent overfitting on 1-minute noise
    maxIter=80,          
    stepSize=0.1,       
    seed=42
)

pipeline = Pipeline(stages=[assembler, gbt])


df_prepared = df_prepared.withColumn("ts_unix", F.unix_timestamp("timestamp"))
split_ts = df_prepared.stat.approxQuantile("ts_unix", [0.8], 0.01)[0]

train_data = df_prepared.filter(F.col("ts_unix") <= split_ts)
test_data = df_prepared.filter(F.col("ts_unix") > split_ts)

model = pipeline.fit(train_data)


predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)













model.write().overwrite().save("models/crypto_direction_model")
