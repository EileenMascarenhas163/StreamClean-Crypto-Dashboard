import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Add logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark session with explicit configurations
logger.info("Starting Spark session...")
spark = SparkSession.builder \
    .appName("StreamClean") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.hadoop.home.dir", "/app/hadoop") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()
logger.info("Spark session created.")

# Define schema for incoming JSON data
schema = StructType([
    StructField("coin", StringType()),
    StructField("price", DoubleType()),
    StructField("market_cap", DoubleType()),
    StructField("vol_24h", DoubleType()),
    StructField("change_24h", DoubleType()),
    StructField("sentiment", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Connect to Kafka
logger.info("Connecting to Kafka...")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "earliest") \
    .load()
logger.info("Connected to Kafka topic 'crypto_prices'.")

# Verify streaming dataframe
if not df.isStreaming:
    logger.error("Dataframe is not a streaming dataframe!")
else:
    logger.info("Dataframe is a streaming dataframe.")

# Parse JSON payload
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Debugging: Write raw stream to console
debug_query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()
logger.info("Debugging console output started.")

# Cleaning: Drop invalid prices and fill nulls
cleaned_df = parsed_df.filter(col("price").isNotNull() & (col("price") > 0))
cleaned_df = cleaned_df.withColumn("change_24h", coalesce(col("change_24h"), lit(0.0)))
cleaned_df = cleaned_df.withColumn("market_cap", coalesce(col("market_cap"), lit(0.0)))
cleaned_df = cleaned_df.withColumn("vol_24h", coalesce(col("vol_24h"), lit(0.0)))
cleaned_df = cleaned_df.withColumn("sentiment", coalesce(col("sentiment"), lit(0.0)))

# Add date for partitioning
final_df = cleaned_df.withColumn("date", to_date("timestamp"))

# Write cleaned data to Parquet
logger.info("Starting raw data streaming to Parquet...")
raw_query = final_df.writeStream \
    .format("parquet") \
    .option("path", "./data/parquet/raw") \
    .option("checkpointLocation", "./checkpoint/raw") \
    .partitionBy("date", "coin") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()
logger.info("Raw data streaming started.")

# Wait for termination (for testing, will be managed by Prefect)
raw_query.awaitTermination()