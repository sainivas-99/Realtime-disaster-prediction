from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_spark_session():
    return SparkSession.builder \
        .appName("WildfireDeltaWriter") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0") \
        .getOrCreate()

def run_wildfire_consumer():
    spark = create_spark_session()
    
    # Define schema for wildfire data
    schema = StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("brightness", DoubleType()),
        StructField("acq_date", TimestampType()),
        StructField("acq_time", StringType()),
        StructField("confidence", StringType())
    ])
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9094") \
        .option("subscribe", "wildfire-topic") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")
    
    # Write to Delta Lake
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "data_collected/wildfire_delta_table/_checkpoints") \
        .start("data_collected/wildfire_delta_table")
    
    return query

if __name__ == "__main__":
    query = run_wildfire_consumer()
    query.awaitTermination()