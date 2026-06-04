# streaming/spark_jobs/silver_processing.py
"""
Silver Layer — Clean, deduplicate, standardize Bronze data.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, concat_ws,
    regexp_extract, current_timestamp,
    when, coalesce, lit
)
from pyspark.sql.types import TimestampType
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from streaming.schemas.lead_schema import BRONZE_LEAD_SCHEMA

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GTM-Silver-Processing")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints/silver")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def clean_leads(df):
    """
    Apply Silver transformations:
    - Lowercase and trim emails
    - Extract email domain
    - Combine first + last name
    - Standardize nulls
    - Remove obvious duplicates
    """
    return (
        df
        # Clean email
        .withColumn("email",
            lower(trim(col("email"))))

        # Extract domain from email
        .withColumn("email_domain",
            regexp_extract(col("email"), r"@(.+)$", 1))

        # Full name
        .withColumn("full_name",
            concat_ws(" ",
                trim(col("first_name")),
                trim(col("last_name"))
            ))

        # Standardize company name
        .withColumn("company_name",
            trim(col("company_name")))

        # Normalize seniority
        .withColumn("seniority",
            when(col("seniority").isNull(), "unknown")
            .otherwise(lower(trim(col("seniority")))))

        # Normalize industry
        .withColumn("industry",
            when(col("industry").isNull(), "unknown")
            .otherwise(lower(trim(col("industry")))))

        # Add processing timestamp
        .withColumn("processed_at", current_timestamp())
        .withColumn("layer", lit("silver"))

        # Drop rows with no email AND no lead_id
        .filter(
            col("email").isNotNull() | col("lead_id").isNotNull()
        )

        # Drop exact duplicates
        .dropDuplicates(["email", "source"])

        # Select final Silver columns
        .select(
            "lead_id",
            "source",
            "full_name",
            "email",
            "email_domain",
            "job_title",
            "seniority",
            "company_name",
            "company_domain",
            "industry",
            "employee_count",
            "lead_status",
            "processed_at",
            "layer"
        )
    )


def process_silver(spark: SparkSession):
    """Read Bronze parquet files and write cleaned Silver output."""

    # Read from Bronze layer (streaming read of parquet files)
    bronze_df = (
        spark.readStream
        .format("parquet")
        .schema(BRONZE_LEAD_SCHEMA)
        .option("path", "./data/bronze/leads")
        .load()
    )

    silver_df = clean_leads(bronze_df)

    query = (
        silver_df.writeStream
        .format("parquet")
        .option("path", "./data/silver/leads")
        .option("checkpointLocation", "./checkpoints/silver/leads")
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

    return query


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Silver processing layer...")
    query = process_silver(spark)
    print("Silver stream running...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()