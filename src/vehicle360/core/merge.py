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


def merge_scd1(spark: SparkSession, source_df: DataFrame, target_table: str, key_columns: Iterable[str], update_condition: str | None = None,
    exclude_update_columns: Iterable[str] | None = None, ) -> dict[str, Any]:

    keys = [c for c in key_columns if c]
    exclude_update_columns = set(exclude_update_columns or [])

    if not keys:
        raise ValueError(f"MERGE requires key columns for target {target_table}")

    for k in keys:
        if k not in source_df.columns:
            raise ValueError(
                f"MERGE key {k} missing in source dataframe for target {target_table}. "
                f"Available columns: {source_df.columns}"
            )

    target_columns = [f.name for f in spark.table(target_table).schema.fields]
    source_columns = source_df.columns

    merge_columns = [
        c for c in target_columns
        if c in source_columns and c not in exclude_update_columns
    ]

    update_columns = [
        c for c in merge_columns
        if c not in keys
    ]

    update_set = {
        c: f"source.{c}"
        for c in update_columns
    }

    insert_set = {
        c: f"source.{c}"
        for c in merge_columns
    }

    condition = " AND ".join([f"target.{k} <=> source.{k}" for k in keys])

    target = DeltaTable.forName(spark, target_table)

    builder = target.alias("target").merge(
        source_df.alias("source"),
        condition
    )

    if update_set:
        if update_condition:
            builder = builder.whenMatchedUpdate(
                condition=update_condition,
                set=update_set
            )
        else:
            builder = builder.whenMatchedUpdate(set=update_set)

    builder = builder.whenNotMatchedInsert(values=insert_set)

    builder.execute()

    return latest_delta_metrics(spark, target_table)

def overwrite_table(df: DataFrame, target_table: str) -> dict[str, Any]:
    rows = df.count()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(target_table)
    return {"operation": "OVERWRITE", "rows_written": rows, "rows_inserted": rows, "rows_updated": 0, "rows_deleted": 0}


def append_table(df: DataFrame, target_table: str) -> dict[str, Any]:
    rows = df.count()
    df.write.format("delta").mode("append").saveAsTable(target_table)
    return {"operation": "APPEND", "rows_written": rows, "rows_inserted": rows, "rows_updated": 0, "rows_deleted": 0}
