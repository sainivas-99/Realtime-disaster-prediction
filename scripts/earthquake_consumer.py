from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from dotenv import load_dotenv
import os

load_dotenv()

aws_access_key = os.getenv('aws_access_key')
aws_secret_key = os.getenv('aws_secret_key')


def create_spark_session():
    return SparkSession.builder \
            .appName("EarthquakeDeltaWriter") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "io.delta:delta-core_2.12:2.2.0",
                "org.apache.hadoop:hadoop-aws:3.3.2"
            ])) \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

def run_earthquake_consumer():
    spark = create_spark_session()
    
    # Define schema for earthquake data
    schema = StructType([
        StructField("id", StringType()),
        StructField("magnitude", DoubleType()),
        StructField("location", StringType()),
        StructField("time", LongType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("depth", DoubleType())
    ])
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9094") \
        .option("subscribe", "earthquake-topic") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", from_unixtime(col("time")/1000))
    
    # Write to Delta Lake
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "data_collected/earthquake_delta_table/_checkpoints") \
        .option('path', 's3://realtime-disaster-data/earthquake_data/') \
        .start()
    
    return query

if __name__ == "__main__":
    query = run_earthquake_consumer()
    query.awaitTermination()