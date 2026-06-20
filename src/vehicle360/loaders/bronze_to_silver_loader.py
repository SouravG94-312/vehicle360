from __future__ import annotations

from pyspark.sql import SparkSession, Window, functions as F

from vehicle360.audit.audit_manager import PipelineAuditManager
from vehicle360.audit.watermark import get_watermark, update_watermark
from vehicle360.core.merge import append_table, merge_scd1, overwrite_table
from vehicle360.core.schema_utils import align_to_target_schema
from vehicle360.dq.quality_engine import DataQualityEngine
from vehicle360.metadata.repository import MetadataRepository
from vehicle360.transformations.bronze_to_silver import transform


def _dedupe(df, keys: list[str], sequence_col: str | None):
    keys = [k for k in keys if k in df.columns]
    if not keys:
        return df, 0
    order_col = F.col(sequence_col).desc_nulls_last() if sequence_col and sequence_col in df.columns else F.monotonically_increasing_id()
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(order_col)
    staged = df.withColumn("_rn", F.row_number().over(w))
    dupes = staged.filter(F.col("_rn") > 1).count()
    return staged.filter(F.col("_rn") == 1).drop("_rn"), dupes


def run_bronze_to_silver(spark: SparkSession, pipeline_id: str, batch_id: str, workflow_audit_id: str | None = None) -> None:
    cfg = MetadataRepository(spark).get_pipeline_config(pipeline_id)
    audit = PipelineAuditManager(
        spark=spark,
        pipeline_id=cfg.pipeline_id,
        pipeline_name=cfg.pipeline_name,
        pipeline_group=cfg.pipeline_group,
        batch_id=batch_id,
        workflow_audit_id=workflow_audit_id,
    )

    rows_read = 0
    try:
        audit.start(cfg.source_object, cfg.target_object, cfg.load_type)
        source_df = spark.table(cfg.source_object)

        source_watermark = None
        if cfg.watermark_column and cfg.watermark_column in source_df.columns:
            source_watermark = get_watermark(spark, "vehicle360.control.pipeline_watermark", cfg.pipeline_id)
            if source_watermark:
                source_df = source_df.filter(F.col(cfg.watermark_column) > F.lit(source_watermark))

        rows_read = source_df.count()
        staged = transform(source_df, cfg.target_object, batch_id, cfg.primary_key_columns)
        staged, duplicated = _dedupe(staged, cfg.primary_key_columns, cfg.sequence_column)

        dq = DataQualityEngine(spark)
        valid_df, rejected_df = dq.apply_rules(staged, cfg.pipeline_id, audit.audit_id, workflow_audit_id, batch_id)
        rejected = dq.write_rejected(rejected_df, cfg.pipeline_id, audit.audit_id, workflow_audit_id, batch_id, cfg.source_object, cfg.target_object)

        final_df = align_to_target_schema(spark, valid_df, cfg.target_object)

        if cfg.load_type == "FULL":
            metrics = overwrite_table(final_df, cfg.target_object)
        elif cfg.load_type == "APPEND":
            metrics = append_table(final_df, cfg.target_object)
        else:
            metrics = merge_scd1(
                spark,
                final_df,
                cfg.target_object,
                cfg.primary_key_columns,
                update_condition="target.record_hash <> source.record_hash" if "record_hash" in final_df.columns else None,
            )

        metrics["rows_read"] = rows_read
        metrics["rows_rejected"] = rejected
        metrics["rows_duplicated"] = duplicated

        target_watermark = None
        if cfg.watermark_column and cfg.watermark_column in source_df.columns and rows_read > 0:
            target_watermark = source_df.agg(F.max(F.col(cfg.watermark_column)).cast("string")).first()[0]
            update_watermark(spark, "vehicle360.control.pipeline_watermark", cfg.pipeline_id, cfg.watermark_column, target_watermark, batch_id, audit.audit_id)

        audit.complete(metrics, source_watermark, target_watermark)

    except Exception as e:
        audit.fail(e, rows_read)
        raise
