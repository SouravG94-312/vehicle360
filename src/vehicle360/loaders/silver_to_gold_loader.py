from __future__ import annotations

from pyspark.sql import SparkSession

from vehicle360.audit.audit_manager import PipelineAuditManager
from vehicle360.core.merge import append_table, merge_scd1, overwrite_table
from vehicle360.core.schema_utils import align_to_target_schema
from vehicle360.metadata.repository import MetadataRepository
from vehicle360.transformations.silver_to_gold import TARGET_TRANSFORMERS, build_dim_date
from vehicle360.core.schema_utils import align_to_target_schema, get_identity_columns


def run_silver_to_gold(spark: SparkSession, pipeline_id: str, batch_id: str, workflow_audit_id: str | None = None) -> None:
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

        if cfg.target_object == "vehicle360.gold.dim_date":
            target_df = build_dim_date(spark)
            rows_read = target_df.count()
        else:
            source_df = spark.table(cfg.source_object)
            rows_read = source_df.count()
            transformer = TARGET_TRANSFORMERS.get(cfg.target_object)
            if not transformer:
                raise ValueError(f"No silver_to_gold transformer registered for target {cfg.target_object}")
            target_df = transformer(source_df)
        
        identity_columns = get_identity_columns(spark, cfg.target_object)

        #final_df = align_to_target_schema(spark, target_df, cfg.target_object)
        final_df = align_to_target_schema(spark=spark, df=target_df, target_table=cfg.target_object, exclude_columns=identity_columns)

        if cfg.load_type == "FULL":
            metrics = overwrite_table(final_df, cfg.target_object)
        elif cfg.load_type == "APPEND":
            metrics = append_table(final_df, cfg.target_object)
        else:
            metrics = merge_scd1(
                spark=spark,
                source_df=final_df,
                target_table=cfg.target_object,
                key_columns=cfg.primary_key_columns,
                update_condition="target.record_hash <> source.record_hash" if "record_hash" in final_df.columns else None,
                exclude_update_columns=identity_columns
            )

        metrics["rows_read"] = rows_read
        audit.complete(metrics)

    except Exception as e:
        audit.fail(e, rows_read)
        raise
