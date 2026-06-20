from __future__ import annotations

from typing import Iterable, Any
from pyspark.sql import DataFrame, SparkSession, functions as F
from delta.tables import DeltaTable


def latest_delta_metrics(spark: SparkSession, table_name: str) -> dict[str, Any]:
    try:
        row = spark.sql(f"DESCRIBE HISTORY {table_name}").orderBy(F.col("version").desc()).first()
        if row is None:
            return {}
        metrics = row["operationMetrics"] or {}
        return {
            "version": row["version"],
            "operation": row["operation"],
            "operationMetrics": {str(k): str(v) for k, v in metrics.items()},
            "rows_read": int(metrics.get("numSourceRows", metrics.get("numInputRows", 0)) or 0),
            "rows_inserted": int(metrics.get("numTargetRowsInserted", 0) or 0),
            "rows_updated": int(metrics.get("numTargetRowsUpdated", 0) or 0),
            "rows_deleted": int(metrics.get("numTargetRowsDeleted", 0) or 0),
            "rows_written": int(metrics.get("numOutputRows", metrics.get("numTargetRowsInserted", 0)) or 0),
        }
    except Exception:
        return {}


def merge_scd1(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_columns: Iterable[str],
    update_condition: str | None = None,
) -> dict[str, Any]:
    keys = [c for c in key_columns if c]
    if not keys:
        raise ValueError(f"MERGE requires key columns for target {target_table}")

    for k in keys:
        if k not in source_df.columns:
            raise ValueError(f"MERGE key {k} missing in source dataframe for target {target_table}")

    target = DeltaTable.forName(spark, target_table)
    condition = " AND ".join([f"target.{k} <=> source.{k}" for k in keys])

    builder = target.alias("target").merge(source_df.alias("source"), condition)
    if update_condition:
        builder = builder.whenMatchedUpdateAll(condition=update_condition)
    else:
        builder = builder.whenMatchedUpdateAll()

    builder.whenNotMatchedInsertAll().execute()
    return latest_delta_metrics(spark, target_table)


def overwrite_table(df: DataFrame, target_table: str) -> dict[str, Any]:
    rows = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(target_table)
    return {"operation": "OVERWRITE", "rows_written": rows, "rows_inserted": rows, "rows_updated": 0, "rows_deleted": 0}


def append_table(df: DataFrame, target_table: str) -> dict[str, Any]:
    rows = df.count()
    df.write.format("delta").mode("append").saveAsTable(target_table)
    return {"operation": "APPEND", "rows_written": rows, "rows_inserted": rows, "rows_updated": 0, "rows_deleted": 0}
