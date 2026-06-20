from pyspark.sql import SparkSession, functions as F


def get_watermark(spark: SparkSession, watermark_table: str, pipeline_id: str) -> str | None:
    try:
        rows = spark.table(watermark_table).filter(F.col("pipeline_id") == pipeline_id).select("current_watermark").limit(1).collect()
        return rows[0]["current_watermark"] if rows else None
    except Exception:
        return None


def update_watermark(
    spark: SparkSession,
    watermark_table: str,
    pipeline_id: str,
    watermark_column: str,
    current_watermark: str | None,
    batch_id: str,
    audit_id: str,
) -> None:
    if current_watermark is None:
        return
    spark.sql(f"""
        MERGE INTO {watermark_table} t
        USING (
            SELECT
              '{pipeline_id}' pipeline_id,
              '{watermark_column}' watermark_column,
              '{current_watermark}' current_watermark,
              '{batch_id}' last_successful_batch_id,
              '{audit_id}' last_successful_audit_id,
              current_timestamp() last_successful_run_timestamp,
              current_timestamp() updated_timestamp
        ) s
        ON t.pipeline_id = s.pipeline_id
        WHEN MATCHED THEN UPDATE SET
          previous_watermark = t.current_watermark,
          current_watermark = s.current_watermark,
          watermark_column = s.watermark_column,
          last_successful_batch_id = s.last_successful_batch_id,
          last_successful_audit_id = s.last_successful_audit_id,
          last_successful_run_timestamp = s.last_successful_run_timestamp,
          updated_timestamp = s.updated_timestamp
        WHEN NOT MATCHED THEN INSERT (
          pipeline_id, watermark_column, previous_watermark, current_watermark,
          last_successful_batch_id, last_successful_audit_id, last_successful_run_timestamp, updated_timestamp
        )
        VALUES (
          s.pipeline_id, s.watermark_column, NULL, s.current_watermark,
          s.last_successful_batch_id, s.last_successful_audit_id, s.last_successful_run_timestamp, s.updated_timestamp
        )
    """)
