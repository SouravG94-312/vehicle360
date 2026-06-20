from __future__ import annotations

from typing import Iterable
from pyspark.sql import DataFrame, SparkSession, functions as F


def table_columns(spark: SparkSession, table_name: str) -> list[str]:
    return [f.name for f in spark.table(table_name).schema.fields]


def table_schema_map(spark: SparkSession, table_name: str) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in spark.table(table_name).schema.fields}


def safe_col(df: DataFrame, name: str):
    return F.col(name) if name in df.columns else F.lit(None)


def first_existing_col(df: DataFrame, candidates: Iterable[str], default=None):
    for c in candidates:
        if c in df.columns:
            return F.col(c)
    return F.lit(default)


def align_to_target_schema(spark: SparkSession, df: DataFrame, target_table: str) -> DataFrame:
    """
    Make source dataframe safe for Delta MERGE/INSERT into an existing table:
    - add missing target columns as NULL cast to target type
    - drop extra source columns
    - keep target column order
    """
    target_schema = spark.table(target_table).schema
    out = df
    for field in target_schema.fields:
        if field.name not in out.columns:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            out = out.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return out.select([F.col(f.name) for f in target_schema.fields])


def require_columns(df: DataFrame, columns: Iterable[str], context: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {context}: {missing}. Available columns: {df.columns}")
