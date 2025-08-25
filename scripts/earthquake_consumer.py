from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from dotenv import load_dotenv
import os
import logging
from delta import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EarthquakeDeltaWriter")

load_dotenv()

def create_spark_session():
    """Create and configure Spark session with Delta Lake and S3 support"""
    try:
        aws_access_key = os.getenv('aws_access_key')
        aws_secret_key = os.getenv('aws_secret_key')
        
        if not aws_access_key or not aws_secret_key:
            logger.error("AWS credentials not found in environment variables")
            raise ValueError("Missing AWS credentials")

        spark = SparkSession.builder \
            .appName("EarthquakeDeltaWriter") \
            .config("spark.jars.packages", ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "org.apache.hadoop:hadoop-aws:3.3.2",
                "com.amazonaws:aws-java-sdk-bundle:1.11.901",
                "io.delta:delta-core_2.12:2.2.0",
                "io.delta:delta-storage:2.2.0"
            ])) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true") \
            .getOrCreate()

        logger.info("Spark session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def run_earthquake_consumer():
    """Main function to consume from Kafka and write to Delta Lake"""
    try:
        spark = create_spark_session()
        
        # Schema for earthquake data
        schema = StructType([
            StructField("id", StringType()),
            StructField("magnitude", DoubleType()),
            StructField("location", StringType()),
            StructField("time", LongType()),
            StructField("longitude", DoubleType()),
            StructField("latitude", DoubleType()),
            StructField("depth", DoubleType()),
            StructField("processed_at", TimestampType())
        ])
        
        # Kafka configuration
        kafka_options = {
            "kafka.bootstrap.servers": "localhost:9094",
            "subscribe": "earthquake-topic",
            "startingOffsets": "earliest",
            "failOnDataLoss": "false"
        }
        
        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load() \
            .select(
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ) \
            .select("data.*", "kafka_timestamp") \
            .withColumn("event_time", from_unixtime(col("time")/1000)) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("date", col("event_time").cast("date"))
        
        logger.info("Streaming DataFrame created successfully")
        
        # Write to console first for verification
        console_query = df.writeStream \
            .format("console") \
            .outputMode("append") \
            .start()
        
        # Wait a bit to see if console output works
        import time
        time.sleep(10)
        console_query.stop()
        
        # Now try Delta Lake
        s3_output_path = "s3a://realtime-disaster-data/earthquake_data/"
        checkpoint_path = "s3a://realtime-disaster-data/checkpoints/earthquake_delta/"
        
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", s3_output_path) \
            .trigger(processingTime="30 seconds") \
            .start() #.partitionBy("date") \
        
        logger.info("Streaming query started")
        return query

    except Exception as e:
        logger.error(f"Error in run_earthquake_consumer: {str(e)}")
        raise

if __name__ == "__main__":
    query = None
    try:
        logger.info("Starting Earthquake Delta Lake consumer")
        query = run_earthquake_consumer()
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Stopping gracefully...")
        if query:
            query.stop()
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        if query:
            query.stop()
        raise