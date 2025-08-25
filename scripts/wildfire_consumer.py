from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType
from dotenv import load_dotenv
import os
import logging
from delta import *
from earthquake_consumer import create_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WildfireDeltaWriter")

load_dotenv()

def run_wildfire_consumer():
    """Main function to consume from Kafka and write to Delta Lake"""
    try:
        spark = create_spark_session()
        
        schema = StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("bright_ti4", DoubleType()),
            StructField("acq_date", TimestampType()),
            StructField("acq_time", IntegerType()),
            StructField("confidence", StringType()),
            StructField("bright_ti5", DoubleType()),
            StructField("frp", DoubleType()),
            StructField("daynight", StringType())
        ])
        
        # Kafka configuration
        kafka_options = {
            "kafka.bootstrap.servers": "localhost:9094",
            "subscribe": "wildfire-topic",
            "startingOffsets": "earliest",
            "failOnDataLoss": "false"
        }
        
        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load() \
            .select(
                from_json(col("value").cast("string"), schema).alias("data")
            ) \
            .select("data.*") 
        
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
        s3_output_path = "s3a://realtime-disaster-data/wildfire_data/"
        checkpoint_path = "s3a://realtime-disaster-data/checkpoints/wildfire_delta/"
        
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
        logger.error(f"Error in run_wildfire_consumer: {str(e)}")
        raise

if __name__ == "__main__":
    query = None
    try:
        logger.info("Starting Wildfire Delta Lake consumer")
        query = run_wildfire_consumer()
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