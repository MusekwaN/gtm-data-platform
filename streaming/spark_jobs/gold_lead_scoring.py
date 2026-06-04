# streaming/spark_jobs/gold_lead_scoring.py
"""
Gold Layer — Business metrics, lead scoring, intent signals.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum,
    when, current_timestamp, lit,
    round as spark_round
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GTM-Gold-LeadScoring")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def calculate_lead_score(df):
    """
    Score leads based on:
    - Seniority level      (0-40 points)
    - Company size         (0-30 points)
    - Industry fit         (0-20 points)
    - Data completeness    (0-10 points)
    Max score = 100
    """
    return (
        df
        # Seniority score
        .withColumn("seniority_score",
            when(col("seniority").isin("c_suite","vp"), 40)
            .when(col("seniority") == "director", 30)
            .when(col("seniority") == "manager", 20)
            .when(col("seniority") == "senior", 10)
            .otherwise(5))

        # Company size score
        .withColumn("company_size_score",
            when(col("employee_count") >= 1000, 30)
            .when(col("employee_count") >= 200, 25)
            .when(col("employee_count") >= 50, 15)
            .when(col("employee_count") >= 10, 10)
            .otherwise(5))

        # Industry fit score
        .withColumn("industry_score",
            when(col("industry").isin(
                "software", "technology", "saas",
                "fintech", "information technology"
            ), 20)
            .when(col("industry").isin(
                "finance", "banking", "healthcare"
            ), 15)
            .otherwise(5))

        # Data completeness score
        .withColumn("completeness_score",
            (
                when(col("email").isNotNull(), 3).otherwise(0) +
                when(col("company_name").isNotNull(), 2).otherwise(0) +
                when(col("job_title").isNotNull(), 2).otherwise(0) +
                when(col("company_domain").isNotNull(), 3).otherwise(0)
            ))

        # Total lead score
        .withColumn("lead_score",
            col("seniority_score") +
            col("company_size_score") +
            col("industry_score") +
            col("completeness_score"))

        # Intent classification
        .withColumn("intent_level",
            when(col("lead_score") >= 80, "hot")
            .when(col("lead_score") >= 60, "warm")
            .when(col("lead_score") >= 40, "cool")
            .otherwise("cold"))

        # Recommended action
        .withColumn("recommended_action",
            when(col("intent_level") == "hot",  "schedule_demo")
            .when(col("intent_level") == "warm", "send_case_study")
            .when(col("intent_level") == "cool", "add_to_nurture")
            .otherwise("enrich_data"))

        .withColumn("scored_at", current_timestamp())
        .withColumn("layer", lit("gold"))
    )


def process_gold(spark: SparkSession):
    """Read Silver data and produce Gold lead scores."""
    
    try:
        silver_df = spark.read.parquet("./data/silver/leads")
        
        if silver_df.count() == 0:
            print("WARNING: No silver data to score. Run silver_processing first.")
            return
            
    except Exception as e:
        print(f"ERROR: Silver data not found - {str(e)}")
        print("Please run silver_processing layer first to generate silver data.")
        return
    
    gold_df = calculate_lead_score(silver_df)

    # Write Gold output
    (
        gold_df
        .select(
            "lead_id",
            "source",
            "full_name",
            "email",
            "company_name",
            "industry",
            "seniority",
            "employee_count",
            "lead_score",
            "seniority_score",
            "company_size_score",
            "industry_score",
            "completeness_score",
            "intent_level",
            "recommended_action",
            "scored_at",
            "layer"
        )
        .write
        .mode("overwrite")
        .parquet("./data/gold/lead_scores")
    )

    print("Gold lead scores written successfully")

    # Print a sample to console
    print("\nSample Gold output:")
    gold_df.select(
        "full_name", "company_name",
        "lead_score", "intent_level", "recommended_action"
    ).show(10, truncate=False)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Running Gold lead scoring...")
    process_gold(spark)


if __name__ == "__main__":
    main()