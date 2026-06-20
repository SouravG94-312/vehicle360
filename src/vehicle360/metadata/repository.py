from __future__ import annotations

from dataclasses import dataclass
from pyspark.sql import SparkSession, functions as F


@dataclass
class PipelineConfig:
    pipeline_id: str
    pipeline_name: str
    pipeline_group: str
    source_object: str
    target_object: str
    load_type: str
    primary_key_columns: list[str]
    sequence_column: str | None
    watermark_column: str | None
    transformation_mode: str


class MetadataRepository:
    def __init__(self, spark: SparkSession, table: str = "vehicle360.control.pipeline_config"):
        self.spark = spark
        self.table = table

    def get_pipeline_config(self, pipeline_id: str) -> PipelineConfig:
        rows = self.spark.table(self.table).filter((F.col("pipeline_id") == pipeline_id) & (F.col("is_active") == True)).limit(1).collect()
        if not rows:
            raise ValueError(f"No active pipeline_config found for pipeline_id={pipeline_id}")
        r = rows[0].asDict()
        return PipelineConfig(
            pipeline_id=r["pipeline_id"],
            pipeline_name=r.get("pipeline_name") or r["pipeline_id"],
            pipeline_group=r.get("pipeline_group") or "",
            source_object=f"{r['source_catalog']}.{r['source_schema']}.{r['source_object']}",
            target_object=f"{r['target_catalog']}.{r['target_schema']}.{r['target_object']}",
            load_type=r.get("load_type") or "MERGE",
            primary_key_columns=[c.strip() for c in (r.get("primary_key_columns") or "").split(",") if c.strip()],
            sequence_column=r.get("sequence_column"),
            watermark_column=r.get("watermark_column"),
            transformation_mode=r.get("transformation_mode") or "METADATA",
        )

    def list_active_pipelines(self, pipeline_group: str | None = None) -> list[PipelineConfig]:
        df = self.spark.table(self.table).filter(F.col("is_active") == True)
        if pipeline_group:
            df = df.filter(F.col("pipeline_group") == pipeline_group)
        return [self.get_pipeline_config(r["pipeline_id"]) for r in df.orderBy("execution_order").select("pipeline_id").collect()]
