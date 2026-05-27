# streaming/schemas/lead_schema.py

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    TimestampType, DoubleType, MapType
)

# ── Raw Kafka message schema (Bronze) ─────────────────────────
BRONZE_LEAD_SCHEMA = StructType([
    StructField("lead_id",          StringType(),  True),
    StructField("source",           StringType(),  True),
    StructField("first_name",       StringType(),  True),
    StructField("last_name",        StringType(),  True),
    StructField("email",            StringType(),  True),
    StructField("job_title",        StringType(),  True),
    StructField("seniority",        StringType(),  True),
    StructField("linkedin_url",     StringType(),  True),
    StructField("company_name",     StringType(),  True),
    StructField("company_domain",   StringType(),  True),
    StructField("industry",         StringType(),  True),
    StructField("employee_count",   IntegerType(), True),
    StructField("apollo_score",     DoubleType(),  True),
    StructField("lead_status",      StringType(),  True),
    StructField("phone",            StringType(),  True),
    StructField("ingested_at",      StringType(),  True),
    StructField("event_type",       StringType(),  True),
])

# ── Cleaned schema (Silver) ────────────────────────────────────
SILVER_LEAD_SCHEMA = StructType([
    StructField("lead_id",          StringType(),  False),
    StructField("source",           StringType(),  False),
    StructField("full_name",        StringType(),  True),
    StructField("email",            StringType(),  True),
    StructField("email_domain",     StringType(),  True),
    StructField("job_title",        StringType(),  True),
    StructField("seniority",        StringType(),  True),
    StructField("company_name",     StringType(),  True),
    StructField("company_domain",   StringType(),  True),
    StructField("industry",         StringType(),  True),
    StructField("employee_count",   IntegerType(), True),
    StructField("ingested_at",      TimestampType(),True),
    StructField("processed_at",     TimestampType(),True),
])