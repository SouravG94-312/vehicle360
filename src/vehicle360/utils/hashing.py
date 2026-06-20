from typing import Iterable
from pyspark.sql import DataFrame, functions as F


def add_hash_columns(
    df: DataFrame,
    business_key_cols: Iterable[str],
    record_cols: Iterable[str] | None = None,
    business_hash_col: str = "business_key_hash",
    record_hash_col: str = "record_hash",
) -> DataFrame:
    business_key_cols = [c for c in business_key_cols if c in df.columns]
    record_cols = [c for c in (record_cols or df.columns) if c in df.columns and c not in (business_hash_col, record_hash_col)]

    if business_key_cols:
        df = df.withColumn(
            business_hash_col,
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_key_cols]), 256),
        )
    else:
        df = df.withColumn(business_hash_col, F.sha2(F.lit("NO_BUSINESS_KEY"), 256))

    if record_cols:
        df = df.withColumn(
            record_hash_col,
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in record_cols]), 256),
        )
    else:
        df = df.withColumn(record_hash_col, F.sha2(F.lit("NO_RECORD_COLS"), 256))

    return df


def hash_key(*cols):
    return F.abs(F.hash(*[F.col(c) if isinstance(c, str) else c for c in cols])).cast("bigint")
