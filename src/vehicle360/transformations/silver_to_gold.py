from __future__ import annotations

from datetime import date, timedelta
from pyspark.sql import DataFrame, SparkSession, functions as F
from vehicle360.core.schema_utils import first_existing_col
from vehicle360.utils.hashing import hash_key


def _audit_cols(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("created_timestamp", F.current_timestamp())
          .withColumn("updated_timestamp", F.current_timestamp())
    )


def build_dim_date(spark: SparkSession, start_date: str = "2025-01-01", end_date: str = "2026-12-31") -> DataFrame:
    dates = []
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    while current <= end:
        dates.append((current,))
        current += timedelta(days=1)

    df = spark.createDataFrame(dates, ["calendar_date"])
    df = (
        df.withColumn("date_key", F.date_format(F.col("calendar_date"), "yyyyMMdd").cast("int"))
          .withColumn("day_of_month", F.dayofmonth("calendar_date"))
          .withColumn("month_number", F.month("calendar_date"))
          .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
          .withColumn("quarter_number", F.quarter("calendar_date"))
          .withColumn("year_number", F.year("calendar_date"))
          .withColumn("week_of_year", F.weekofyear("calendar_date"))
          .withColumn("day_of_week", F.dayofweek("calendar_date"))
          .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
          .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
    )
    return _audit_cols(df)


def build_dim_vehicle_model(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("vehicle_model_key", hash_key("model_id"))
          .withColumn("effective_from", F.current_timestamp())
          .withColumn("effective_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
    )
    return _audit_cols(out)


def build_dim_customer(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("customer_key", hash_key("customer_id"))
          .withColumn("effective_from", F.current_timestamp())
          .withColumn("effective_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
    )
    return _audit_cols(out)


def build_dim_dealer(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("dealer_key", hash_key("dealer_id"))
          .withColumn("effective_from", F.current_timestamp())
          .withColumn("effective_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
    )
    return _audit_cols(out)


def build_dim_part(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("part_key", hash_key(first_existing_col(df, ["part_id", "part_number"])))
          .withColumn("effective_from", F.current_timestamp())
          .withColumn("effective_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
    )
    return _audit_cols(out)


def build_dim_failure_code(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("failure_code_key", hash_key("failure_code"))
          .withColumn("effective_from", F.current_timestamp())
          .withColumn("effective_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
    )
    return _audit_cols(out)


def build_dim_vehicle(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("vehicle_key", hash_key("vin"))
          .withColumn("vehicle_model_key", hash_key("model_id"))
          .withColumn("effective_from", F.current_timestamp())
          .withColumn("effective_to", F.lit(None).cast("timestamp"))
          .withColumn("is_current", F.lit(True))
    )
    return _audit_cols(out)


def build_fact_service_order(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("service_order_key", hash_key("service_order_id"))
          .withColumn("vehicle_key", F.coalesce(first_existing_col(df, ["vehicle_key"]), hash_key("vin")))
          .withColumn("dealer_key", F.coalesce(first_existing_col(df, ["dealer_key"]), hash_key("dealer_id")))
          .withColumn("customer_key", F.coalesce(first_existing_col(df, ["customer_key"]), hash_key("customer_id")))
          .withColumn("open_date_key", F.date_format(first_existing_col(df, ["open_date", "opened_timestamp"]), "yyyyMMdd").cast("int"))
          .withColumn("close_date_key", F.date_format(first_existing_col(df, ["close_date", "closed_timestamp"]), "yyyyMMdd").cast("int"))
          .withColumn("total_service_cost", first_existing_col(df, ["total_service_cost", "total_service_amount"], 0))
          .withColumn("repeat_repair_count", first_existing_col(df, ["repeat_repair_count"], 0))
    )
    return _audit_cols(out)


def build_fact_repair_job(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("repair_job_key", hash_key("repair_job_id"))
          .withColumn("vehicle_key", F.coalesce(first_existing_col(df, ["vehicle_key"]), hash_key("vin")))
          .withColumn("dealer_key", F.coalesce(first_existing_col(df, ["dealer_key"]), hash_key("dealer_id")))
          .withColumn("failure_code_key", F.coalesce(first_existing_col(df, ["failure_code_key"]), hash_key("failure_code")))
          .withColumn("repair_date_key", F.date_format(first_existing_col(df, ["repair_date", "completed_timestamp", "started_timestamp"]), "yyyyMMdd").cast("int"))
    )
    return _audit_cols(out)


def build_fact_part_consumption(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("part_consumption_key", hash_key("part_consumption_id"))
          .withColumn("vehicle_key", F.coalesce(first_existing_col(df, ["vehicle_key"]), hash_key("vin")))
          .withColumn("dealer_key", F.coalesce(first_existing_col(df, ["dealer_key"]), hash_key("dealer_id")))
          .withColumn("part_key", F.coalesce(first_existing_col(df, ["part_key"]), hash_key(first_existing_col(df, ["part_id", "part_number"]))))
          .withColumn("consumption_date_key", F.date_format(first_existing_col(df, ["consumption_date", "consumption_timestamp"]), "yyyyMMdd").cast("int"))
          .withColumn("extended_cost", first_existing_col(df, ["extended_cost"], 0))
    )
    return _audit_cols(out)


def build_fact_warranty_claim(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("warranty_claim_key", hash_key("warranty_claim_id"))
          .withColumn("vehicle_key", F.coalesce(first_existing_col(df, ["vehicle_key"]), hash_key("vin")))
          .withColumn("dealer_key", F.coalesce(first_existing_col(df, ["dealer_key"]), hash_key("dealer_id")))
          .withColumn("submitted_date_key", F.date_format(first_existing_col(df, ["claim_submitted_date"]), "yyyyMMdd").cast("int"))
          .withColumn("decision_date_key", F.date_format(first_existing_col(df, ["decision_date", "claim_approved_date", "claim_rejected_date"]), "yyyyMMdd").cast("int"))
          .withColumn("claim_count", first_existing_col(df, ["claim_count"], 1))
    )
    return _audit_cols(out)


def build_fact_vehicle_daily_usage(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("vehicle_usage_key", hash_key("usage_id"))
          .withColumn("vehicle_key", F.coalesce(first_existing_col(df, ["vehicle_key"]), hash_key("vin")))
          .withColumn("customer_key", F.coalesce(first_existing_col(df, ["customer_key"]), hash_key("customer_id")))
          .withColumn("date_key", F.date_format(first_existing_col(df, ["usage_date"]), "yyyyMMdd").cast("int"))
    )
    return _audit_cols(out)


def build_fact_breakdown(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("breakdown_key", hash_key("breakdown_id"))
          .withColumn("vehicle_key", F.coalesce(first_existing_col(df, ["vehicle_key"]), hash_key("vin")))
          .withColumn("dealer_key", F.coalesce(first_existing_col(df, ["dealer_key"]), hash_key("dealer_id")))
          .withColumn("customer_key", F.coalesce(first_existing_col(df, ["customer_key"]), hash_key("customer_id")))
          .withColumn("failure_code_key", F.coalesce(first_existing_col(df, ["failure_code_key"]), hash_key("failure_code")))
          .withColumn("breakdown_date_key", F.date_format(first_existing_col(df, ["breakdown_date", "breakdown_timestamp"]), "yyyyMMdd").cast("int"))
          .withColumn("resolved_date_key", F.date_format(first_existing_col(df, ["resolved_date", "resolved_timestamp"]), "yyyyMMdd").cast("int"))
    )
    return _audit_cols(out)


TARGET_TRANSFORMERS = {
    "vehicle360.gold.dim_vehicle_model": build_dim_vehicle_model,
    "vehicle360.gold.dim_customer": build_dim_customer,
    "vehicle360.gold.dim_dealer": build_dim_dealer,
    "vehicle360.gold.dim_part": build_dim_part,
    "vehicle360.gold.dim_failure_code": build_dim_failure_code,
    "vehicle360.gold.dim_vehicle": build_dim_vehicle,
    "vehicle360.gold.fact_service_order": build_fact_service_order,
    "vehicle360.gold.fact_repair_job": build_fact_repair_job,
    "vehicle360.gold.fact_part_consumption": build_fact_part_consumption,
    "vehicle360.gold.fact_warranty_claim": build_fact_warranty_claim,
    "vehicle360.gold.fact_vehicle_daily_usage": build_fact_vehicle_daily_usage,
    "vehicle360.gold.fact_breakdown": build_fact_breakdown,
}
