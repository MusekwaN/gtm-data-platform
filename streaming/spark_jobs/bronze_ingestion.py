# streaming/spark_jobs/bronze_ingestion.py
"""
Bronze Layer — Raw ingestion from Kafka.
No transformations. Store everything as-is with metadata.
"""

import os
from pathlib import Path

# Fix HADOOP_HOME for Windows before importing pyspark
if not os.environ.get("HADOOP_HOME"):
    hadoop_home = Path.home() / ".hadoop"
    hadoop_home.mkdir(exist_ok=True)
    (hadoop_home / "bin").mkdir(exist_ok=True)
    hadoop_home_str = str(hadoop_home).replace("\\", "/")
    os.environ["HADOOP_HOME"] = hadoop_home_str
    os.environ["hadoop.home.dir"] = hadoop_home_str
else:
    os.environ["HADOOP_HOME"] = os.environ["HADOOP_HOME"].replace("\\", "/")
    os.environ["hadoop.home.dir"] = os.environ["HADOOP_HOME"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    lit, to_json, struct
)
from dotenv import load_dotenv
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from streaming.schemas.lead_schema import BRONZE_LEAD_SCHEMA

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv()
load_dotenv(os.path.join(ROOT_DIR, 'env'))

def create_spark_session() -> SparkSession:
    """Create Spark session with Kafka package."""
    hadoop_home = os.environ.get("HADOOP_HOME", "").replace("\\", "/")
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("GTM-Bronze-Ingestion")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.home.dir", hadoop_home)
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home} -Dfile.encoding=UTF-8")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints/bronze")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topic: str):
    """Read a Kafka topic as a streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def process_bronze(spark: SparkSession, topic: str, output_path: str):
    """
    Read raw Kafka messages and write to Bronze storage.
    Adds metadata columns: ingestion_time, topic, partition, offset.
    """
    raw_stream = read_kafka_stream(spark, topic)

    # Parse JSON value from Kafka
    parsed = (
        raw_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(
                col("value").cast("string"),
                BRONZE_LEAD_SCHEMA
            ).alias("data")
        )
        .select(
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "data.*",                           # expand all data fields
            current_timestamp().alias("bronze_loaded_at"),
            lit("bronze").alias("layer")
        )
    )

    # Write to parquet files (Bronze storage)
    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", f"./checkpoints/bronze/{topic}")
        .partitionBy("source")                  # partition by data source
        .outputMode("append")
        .trigger(processingTime="30 seconds")   # micro-batch every 30s
        .start()
    )

    return query


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Bronze ingestion layer...")

    # Start streaming from both topics
    query_apollo = process_bronze(
        spark,
        topic="apollo_leads",
        output_path="./data/bronze/leads"
    )

    query_crm = process_bronze(
        spark,
        topic="crm_events",
        output_path="./data/bronze/crm_events"
    )

    print("Bronze streams running. Waiting for data...")

    # Keep running until stopped
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()