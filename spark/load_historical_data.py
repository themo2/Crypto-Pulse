from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    to_timestamp
)
from pyspark.sql.types import DoubleType, LongType





INPUT_PATH = "/app/data/historical_data/*.csv"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/cryptopulse_db"
POSTGRES_TABLE = "historical_prices_1m"
POSTGRES_USER = "cryptopulse"
POSTGRES_PASSWORD = "cryptopulse123"

spark = (
    SparkSession.builder
    .appName("CryptoPulse-Historical-Loader")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Reading historical CSV files...")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

print("Detected columns:")
df.printSchema()





df = df.withColumn(
    "symbol",
    regexp_extract(
        input_file_name(),
        r"([^/\\]+)_1m_1year\.csv$",
        1
    )
)




df = df.withColumn(
    "timestamp",
    to_timestamp(
        col("timestamp"),
        "yyyy-MM-dd HH:mm:ss"
    )
)



df = (
    df.withColumn("open", col("open").cast(DoubleType()))
    .withColumn("high", col("high").cast(DoubleType()))
    .withColumn("low", col("low").cast(DoubleType()))
    .withColumn("close", col("close").cast(DoubleType()))
    .withColumn("volume", col("volume").cast(DoubleType()))
    .withColumn(
        "number_of_trades",
        col("number_of_trades").cast(LongType())
    )
)



df = df.select(
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "number_of_trades"
)

print(f"Rows read: {df.count()}")

print("Final schema:")
df.printSchema()






print(
    f"Writing historical data to PostgreSQL table: {POSTGRES_TABLE}"
)

(
    df.write
    .format("jdbc")
    .option("url", POSTGRES_URL)
    .option("dbtable", POSTGRES_TABLE)
    .option("user", POSTGRES_USER)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", "5000")
    .mode("overwrite")
    .save()
)

print("Historical data loaded successfully.")

spark.stop()