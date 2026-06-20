from __future__ import annotations

from uuid import uuid4
from pyspark.sql import DataFrame, SparkSession, functions as F


class DataQualityEngine:
    def __init__(
        self,
        spark: SparkSession,
        rules_table: str = "vehicle360.control.data_quality_rule",
        results_table: str = "vehicle360.control.data_quality_result",
        rejected_table: str = "vehicle360.control.rejected_record",
    ):
        self.spark = spark
        self.rules_table = rules_table
        self.results_table = results_table
        self.rejected_table = rejected_table

    def apply_rules(self, df: DataFrame, pipeline_id: str, audit_id: str, workflow_audit_id: str | None, batch_id: str):
        rules = self._rules(pipeline_id)
        if not rules:
            return df.withColumn("dq_status", F.lit("PASSED")).withColumn("dq_error_message", F.lit(None).cast("string")), df.limit(0)

        good = df
        rejected = None

        for r in rules:
            expr = r["rule_expression"]
            rule_id = r["rule_id"]
            rule_name = r["rule_name"]
            action = r["failure_action"] or "WARN"
            before = good.count()

            bad = good.filter(f"NOT ({expr})")
            bad_count = bad.count()
            passed_count = before - bad_count
            self._result(audit_id, workflow_audit_id, batch_id, pipeline_id, r, before, passed_count, bad_count)

            if bad_count > 0 and action in ("QUARANTINE", "DROP_RECORD", "FAIL_PIPELINE"):
                tagged = bad.withColumn("dq_status", F.lit("FAILED")).withColumn("dq_error_message", F.lit(f"{rule_id}: {rule_name}")).withColumn("_failed_rule_id", F.lit(rule_id))
                rejected = tagged if rejected is None else rejected.unionByName(tagged, allowMissingColumns=True)
                good = good.filter(expr)
                if action == "FAIL_PIPELINE":
                    raise ValueError(f"Data quality failed: {rule_id} - {rule_name}; failed_count={bad_count}")

        good = good.withColumn("dq_status", F.lit("PASSED")).withColumn("dq_error_message", F.lit(None).cast("string"))
        if rejected is None:
            rejected = df.limit(0).withColumn("dq_status", F.lit("FAILED")).withColumn("dq_error_message", F.lit(None).cast("string"))
        return good, rejected

    def write_rejected(self, rejected_df: DataFrame, pipeline_id: str, audit_id: str, workflow_audit_id: str | None, batch_id: str, source_object: str, target_object: str) -> int:
        count = rejected_df.count()
        if count == 0:
            return 0
        payload_cols = [c for c in rejected_df.columns if not c.startswith("_")]
        out = (
            rejected_df
            .withColumn("rejection_id", F.expr("uuid()"))
            .withColumn("audit_id", F.lit(audit_id))
            .withColumn("workflow_audit_id", F.lit(workflow_audit_id))
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("pipeline_id", F.lit(pipeline_id))
            .withColumn("source_object", F.lit(source_object))
            .withColumn("target_object", F.lit(target_object))
            .withColumn("source_record", F.to_json(F.struct(*[F.col(c) for c in payload_cols])))
            .withColumn("rejection_reason", F.col("dq_error_message"))
            .withColumn("failed_rule_ids", F.col("_failed_rule_id") if "_failed_rule_id" in rejected_df.columns else F.lit(None))
            .withColumn("severity", F.lit("HIGH"))
            .withColumn("source_file_name", F.lit(None).cast("string"))
            .withColumn("source_file_path", F.lit(None).cast("string"))
            .withColumn("rejected_timestamp", F.current_timestamp())
            .select("rejection_id", "audit_id", "workflow_audit_id", "batch_id", "pipeline_id", "source_object", "target_object", "source_record", "rejection_reason", "failed_rule_ids", "severity", "source_file_name", "source_file_path", "rejected_timestamp")
        )
        out.write.format("delta").mode("append").saveAsTable(self.rejected_table)
        return count

    def _rules(self, pipeline_id: str):
        try:
            return self.spark.table(self.rules_table).filter((F.col("pipeline_id") == pipeline_id) & (F.col("is_active") == True)).collect()
        except Exception:
            return []

    def _result(self, audit_id, workflow_audit_id, batch_id, pipeline_id, rule, evaluated, passed, failed):
        from uuid import uuid4
    
        def q(value):
            if value is None:
                return "NULL"
            if isinstance(value, bool):
                return "true" if value else "false"
            if isinstance(value, (int, float)):
                return str(value)
            s = str(value).replace("'", "''")
            return f"'{s}'"
    
        failure_percentage = float((failed / evaluated * 100) if evaluated else 0)
    
        self.spark.sql(f"""
            INSERT INTO {self.results_table} (
                dq_result_id,
                audit_id,
                workflow_audit_id,
                batch_id,
                pipeline_id,
                rule_id,
                rule_name,
                rule_type,
                severity,
                evaluated_row_count,
                failed_row_count,
                passed_row_count,
                failure_percentage,
                threshold_value,
                rule_status,
                failure_action,
                evaluated_timestamp,
                sample_failed_records
            )
            VALUES (
                {q(str(uuid4()))},
                {q(audit_id)},
                {q(workflow_audit_id)},
                {q(batch_id)},
                {q(pipeline_id)},
                {q(rule["rule_id"])},
                {q(rule["rule_name"])},
                {q(rule["rule_type"])},
                {q(rule["severity"])},
                {int(evaluated)},
                {int(failed)},
                {int(passed)},
                {failure_percentage},
                {q(rule["threshold_value"])},
                {q("FAILED" if failed else "PASSED")},
                {q(rule["failure_action"])},
                current_timestamp(),
                NULL
            )
        """)
    