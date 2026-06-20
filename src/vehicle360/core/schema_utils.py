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

def get_identity_columns(spark: SparkSession, table_name: str) -> list[str]:
    """
    Detect identity columns from SHOW CREATE TABLE output.
    Works for Databricks Delta tables where identity columns appear as:
    column_name BIGINT GENERATED ALWAYS AS IDENTITY
    """
    try:
        rows = spark.sql(f"SHOW CREATE TABLE {table_name}").collect()
        ddl = "\n".join([str(r[0]) for r in rows])
        identity_cols = []

        for line in ddl.splitlines():
            clean = line.strip().replace("`", "")
            if "GENERATED" in clean.upper() and "IDENTITY" in clean.upper():
                col_name = clean.split()[0].replace(",", "")
                identity_cols.append(col_name)

        return identity_cols

    except Exception:
        return []

def align_to_target_schema(spark: SparkSession, df: DataFrame, target_table: str, exclude_columns: Iterable[str] | None = None
) -> DataFrame:
    """
    Align dataframe to the existing target table schema.

    Important:
    - Identity columns must be excluded from source dataframe.
    - Databricks generates identity values automatically.
    """
    exclude_columns = set(exclude_columns or [])

    target_schema = spark.table(target_table).schema
    out = df

    selected_columns = []

    for field in target_schema.fields:
        if field.name in exclude_columns:
            continue

        if field.name not in out.columns:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            out = out.withColumn(field.name, F.col(field.name).cast(field.dataType))

        selected_columns.append(field.name)

    return out.select([F.col(c) for c in selected_columns])


def require_columns(df: DataFrame, columns: Iterable[str], context: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {context}: {missing}. Available columns: {df.columns}")
