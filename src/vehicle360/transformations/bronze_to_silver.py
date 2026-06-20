from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from vehicle360.core.schema_utils import first_existing_col
from vehicle360.utils.hashing import add_hash_columns


def _base(df: DataFrame, batch_id: str, business_keys: list[str]) -> DataFrame:
    out = df
    if "pipeline_id" in out.columns:
        out = out.withColumnRenamed("pipeline_id", "source_pipeline_id")
    else:
        out = out.withColumn("source_pipeline_id", F.lit(None).cast("string"))

    if "batch_id" in out.columns:
        out = out.withColumnRenamed("batch_id", "source_batch_id")
    else:
        out = out.withColumn("source_batch_id", F.lit(None).cast("string"))

    if "ingestion_timestamp" in out.columns:
        out = out.withColumnRenamed("ingestion_timestamp", "source_ingestion_timestamp")
    else:
        out = out.withColumn("source_ingestion_timestamp", F.current_timestamp())

    if "record_hash" in out.columns:
        out = out.withColumnRenamed("record_hash", "source_record_hash")
    else:
        out = out.withColumn("source_record_hash", F.lit(None).cast("string"))

    if "source_file_name" not in out.columns:
        out = out.withColumn("source_file_name", F.lit(None).cast("string"))

    out = (
        out.withColumn("silver_batch_id", F.lit(batch_id))
           .withColumn("silver_load_timestamp", F.current_timestamp())
           .withColumn("silver_update_timestamp", F.current_timestamp())
           .withColumn("is_valid", F.lit(True))
    )
    return add_hash_columns(out, business_keys)


def transform_vehicle(df: DataFrame, batch_id: str) -> DataFrame:
    out = df.select(
        first_existing_col(df, ["vin"]).alias("vin"),
        first_existing_col(df, ["model_id", "vehicle_model_id"]).alias("model_id"),
        first_existing_col(df, ["current_customer_id", "customer_id"]).alias("customer_id"),
        first_existing_col(df, ["selling_dealer_id", "dealer_id"]).alias("dealer_id"),
        first_existing_col(df, ["registration_number"]).alias("registration_number"),
        first_existing_col(df, ["manufacture_date", "production_date"]).cast("date").alias("production_date"),
        first_existing_col(df, ["sale_date", "delivery_date"]).cast("date").alias("delivery_date"),
        first_existing_col(df, ["warranty_start_date"]).cast("date").alias("warranty_start_date"),
        first_existing_col(df, ["warranty_end_date"]).cast("date").alias("warranty_end_date"),
        first_existing_col(df, ["current_odometer_km", "odometer_km"]).alias("current_odometer_km"),
        first_existing_col(df, ["odometer_km", "current_odometer_km"]).alias("odometer_km"),
        first_existing_col(df, ["vehicle_status"]).alias("vehicle_status"),
        first_existing_col(df, ["color"]).alias("color"),
        first_existing_col(df, ["engine_number"]).alias("engine_number"),
        first_existing_col(df, ["chassis_number"]).alias("chassis_number"),
        first_existing_col(df, ["powertrain_type"]).alias("powertrain_type"),
        first_existing_col(df, ["emission_standard"]).alias("emission_standard"),
        *[F.col(c) for c in df.columns if c in ("pipeline_id", "batch_id", "ingestion_timestamp", "source_file_name", "record_hash")]
    )
    return _base(out, batch_id, ["vin"])


def transform_service_order(df: DataFrame, batch_id: str) -> DataFrame:
    out = df
    if "opened_timestamp" not in out.columns:
        out = out.withColumn("opened_timestamp", first_existing_col(out, ["open_timestamp", "created_timestamp"]))
    if "closed_timestamp" not in out.columns:
        out = out.withColumn("closed_timestamp", first_existing_col(out, ["close_timestamp", "completed_timestamp"]))
    if "open_date" not in out.columns:
        out = out.withColumn("open_date", F.to_date(first_existing_col(out, ["opened_timestamp", "open_timestamp", "created_timestamp"])))
    if "close_date" not in out.columns:
        out = out.withColumn("close_date", F.to_date(first_existing_col(out, ["closed_timestamp", "close_timestamp", "completed_timestamp"])))
    if "total_service_cost" not in out.columns:
        out = out.withColumn("total_service_cost", F.coalesce(first_existing_col(out, ["labor_cost"], 0), F.lit(0)) + F.coalesce(first_existing_col(out, ["parts_cost"], 0), F.lit(0)) + F.coalesce(first_existing_col(out, ["other_cost"], 0), F.lit(0)))
    if "repeat_repair_count" not in out.columns:
        out = out.withColumn("repeat_repair_count", F.lit(0))
    return _base(out, batch_id, ["service_order_id"])


def transform_repair_job(df: DataFrame, batch_id: str) -> DataFrame:
    out = df
    if "repair_date" not in out.columns:
        out = out.withColumn("repair_date", F.to_date(first_existing_col(out, ["completed_timestamp", "started_timestamp"])))
    if "labor_cost" not in out.columns:
        out = out.withColumn("labor_cost", F.coalesce(first_existing_col(out, ["labor_hours"], 0), F.lit(0)) * F.coalesce(first_existing_col(out, ["labor_rate"], 0), F.lit(0)))
    return _base(out, batch_id, ["repair_job_id"])


def transform_part_consumption(df: DataFrame, batch_id: str) -> DataFrame:
    out = df
    if "extended_cost" not in out.columns:
        out = out.withColumn("extended_cost", F.coalesce(first_existing_col(out, ["quantity"], 0), F.lit(0)) * F.coalesce(first_existing_col(out, ["unit_cost"], 0), F.lit(0)))
    if "consumption_date" not in out.columns:
        out = out.withColumn("consumption_date", F.to_date(first_existing_col(out, ["consumption_timestamp", "created_timestamp"])))
    return _base(out, batch_id, ["part_consumption_id"])


def transform_warranty_claim(df: DataFrame, batch_id: str) -> DataFrame:
    out = df
    if "decision_date" not in out.columns:
        out = out.withColumn("decision_date", F.coalesce(first_existing_col(out, ["claim_approved_date"]), first_existing_col(out, ["claim_rejected_date"]), first_existing_col(out, ["claim_submitted_date"])))
    if "claim_count" not in out.columns:
        out = out.withColumn("claim_count", F.lit(1))
    if "warranty_type" not in out.columns:
        out = out.withColumn("warranty_type", first_existing_col(out, ["claim_type"]))
    return _base(out, batch_id, ["warranty_claim_id"])


def transform_vehicle_usage(df: DataFrame, batch_id: str) -> DataFrame:
    out = df
    if "usage_id" not in out.columns:
        out = out.withColumn("usage_id", F.sha2(F.concat_ws("||", first_existing_col(out, ["vin"]).cast("string"), first_existing_col(out, ["usage_date"]).cast("string")), 256))
    return _base(out, batch_id, ["usage_id"])


def transform_breakdown(df: DataFrame, batch_id: str) -> DataFrame:
    out = df
    if "breakdown_date" not in out.columns:
        out = out.withColumn("breakdown_date", F.to_date(first_existing_col(out, ["breakdown_timestamp"])))
    if "resolved_date" not in out.columns:
        out = out.withColumn("resolved_date", F.to_date(first_existing_col(out, ["resolved_timestamp"])))
    return _base(out, batch_id, ["breakdown_id"])


def generic_transform(df: DataFrame, batch_id: str, business_keys: list[str]) -> DataFrame:
    return _base(df, batch_id, business_keys)


TRANSFORMERS = {
    "vehicle": transform_vehicle,
    "service_order": transform_service_order,
    "repair_job": transform_repair_job,
    "part_consumption": transform_part_consumption,
    "warranty_claim": transform_warranty_claim,
    "vehicle_daily_usage": transform_vehicle_usage,
    "breakdown": transform_breakdown,
}


def transform(source_df: DataFrame, target_table: str, batch_id: str, business_keys: list[str]) -> DataFrame:
    target_name = target_table.split(".")[-1]
    fn = TRANSFORMERS.get(target_name)
    if fn:
        return fn(source_df, batch_id)
    return generic_transform(source_df, batch_id, business_keys)
