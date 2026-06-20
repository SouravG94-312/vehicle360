from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark.sql import SparkSession


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def q(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    s = str(value).replace("'", "''")
    return f"'{s}'"


@dataclass
class PipelineAuditManager:
    spark: SparkSession
    pipeline_id: str
    pipeline_name: str
    batch_id: str
    workflow_audit_id: str | None = None
    audit_table: str = "vehicle360.control.pipeline_run_audit"
    event_table: str = "vehicle360.control.pipeline_event_log"
    pipeline_group: str | None = None

    audit_id: str = ""
    started_at: datetime | None = None

    def __post_init__(self) -> None:
        self.audit_id = self.audit_id or str(uuid4())

    def start(self, source_object: str, target_object: str, load_type: str) -> str:
        self.started_at = utc_now()

        self.spark.sql(f"""
            INSERT INTO {self.audit_table} (
                audit_id,
                workflow_audit_id,
                batch_id,
                pipeline_id,
                pipeline_name,
                pipeline_group,
                source_object,
                target_object,
                load_type,
                run_status,
                start_timestamp,
                end_timestamp,
                duration_seconds,
                rows_read,
                rows_written,
                rows_inserted,
                rows_updated,
                rows_deleted,
                rows_rejected,
                rows_duplicated,
                rows_quarantined,
                source_watermark,
                target_watermark,
                source_file_name,
                source_file_path,
                delta_table_version,
                delta_operation,
                delta_operation_metrics,
                job_id,
                job_run_id,
                task_key,
                notebook_path,
                cluster_id,
                error_code,
                error_message,
                error_stacktrace,
                created_timestamp,
                updated_timestamp
            )
            VALUES (
                {q(self.audit_id)},
                {q(self.workflow_audit_id)},
                {q(self.batch_id)},
                {q(self.pipeline_id)},
                {q(self.pipeline_name)},
                {q(self.pipeline_group)},
                {q(source_object)},
                {q(target_object)},
                {q(load_type)},
                'RUNNING',
                {q(self.started_at)},
                NULL,
                NULL,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                {q(self.started_at)},
                {q(self.started_at)}
            )
        """)

        self.event("PIPELINE_START", "INFO", f"Started {self.pipeline_id}")
        return self.audit_id

    def complete(self, metrics: dict[str, Any] | None = None, source_watermark: str | None = None, target_watermark: str | None = None) -> None:
        metrics = metrics or {}
        now = utc_now()
        duration = int((now - self.started_at).total_seconds()) if self.started_at else 0
        updates = {
            "run_status": "SUCCESS",
            "end_timestamp": now,
            "duration_seconds": duration,
            "rows_read": int(metrics.get("rows_read", 0) or 0),
            "rows_written": int(metrics.get("rows_written", 0) or 0),
            "rows_inserted": int(metrics.get("rows_inserted", 0) or 0),
            "rows_updated": int(metrics.get("rows_updated", 0) or 0),
            "rows_deleted": int(metrics.get("rows_deleted", 0) or 0),
            "rows_rejected": int(metrics.get("rows_rejected", 0) or 0),
            "rows_duplicated": int(metrics.get("rows_duplicated", 0) or 0),
            "source_watermark": source_watermark,
            "target_watermark": target_watermark,
            "delta_table_version": metrics.get("version"),
            "delta_operation": metrics.get("operation"),
            "updated_timestamp": now,
        }
        self._update(updates)
        self.event("PIPELINE_COMPLETE", "INFO", f"Completed {self.pipeline_id}")

    def fail(self, error: Exception, rows_read: int = 0) -> None:
        now = utc_now()
        duration = int((now - self.started_at).total_seconds()) if self.started_at else 0
        self._update({
            "run_status": "FAILED",
            "end_timestamp": now,
            "duration_seconds": duration,
            "rows_read": rows_read,
            "error_code": type(error).__name__,
            "error_message": str(error)[:4000],
            "error_stacktrace": traceback.format_exc()[:8000],
            "updated_timestamp": now,
        })
        self.event("PIPELINE_FAILED", "ERROR", str(error)[:1000])

    def event(self, event_type: str, level: str, message: str) -> None:
        try:
            self.spark.sql(f"""
                INSERT INTO {self.event_table} (
                    event_id,
                    audit_id,
                    workflow_audit_id,
                    batch_id,
                    pipeline_id,
                    event_type,
                    event_level,
                    event_message,
                    event_payload,
                    event_timestamp
                )
                VALUES (
                    {q(str(uuid4()))},
                    {q(self.audit_id)},
                    {q(self.workflow_audit_id)},
                    {q(self.batch_id)},
                    {q(self.pipeline_id)},
                    {q(event_type)},
                    {q(level)},
                    {q(message)},
                    NULL,
                    {q(utc_now())}
                )
            """)
        except Exception:
            pass

    def _update(self, updates: dict[str, Any]) -> None:
        set_clause = ", ".join([f"{k} = {q(v)}" for k, v in updates.items()])
        self.spark.sql(f"UPDATE {self.audit_table} SET {set_clause} WHERE audit_id = {q(self.audit_id)}")


@dataclass
class WorkflowAuditManager:
    spark: SparkSession
    workflow_name: str
    workflow_audit_table: str = "vehicle360.control.workflow_run_audit"
    batch_id: str | None = None
    workflow_audit_id: str | None = None
    started_at: datetime | None = None

    def __post_init__(self) -> None:
        self.batch_id = self.batch_id or str(uuid4())
        self.workflow_audit_id = self.workflow_audit_id or str(uuid4())

    def start(self, trigger_type: str = "MANUAL", triggered_by: str | None = None) -> dict[str, str]:
        self.started_at = utc_now()
        row = [{
            "workflow_audit_id": self.workflow_audit_id,
            "workflow_name": self.workflow_name,
            "workflow_run_name": self.workflow_name,
            "batch_id": self.batch_id,
            "job_id": None,
            "job_run_id": None,
            "trigger_type": trigger_type,
            "triggered_by": triggered_by,
            "run_status": "RUNNING",
            "start_timestamp": self.started_at,
            "end_timestamp": None,
            "duration_seconds": None,
            "total_pipelines": 0,
            "successful_pipelines": 0,
            "failed_pipelines": 0,
            "skipped_pipelines": 0,
            "warning_pipelines": 0,
            "total_rows_read": 0,
            "total_rows_inserted": 0,
            "total_rows_updated": 0,
            "total_rows_deleted": 0,
            "total_rows_rejected": 0,
            "error_code": None,
            "error_message": None,
            "created_timestamp": self.started_at,
            "updated_timestamp": self.started_at,
        }]
        self.spark.createDataFrame(row).write.format("delta").mode("append").saveAsTable(self.workflow_audit_table)
        return {"batch_id": self.batch_id, "workflow_audit_id": self.workflow_audit_id}

    def complete(self, pipeline_audit_table: str = "vehicle360.control.pipeline_run_audit") -> None:
        now = utc_now()
        row = self.spark.sql(f"""
            SELECT
              COUNT(*) total_pipelines,
              SUM(CASE WHEN run_status='SUCCESS' THEN 1 ELSE 0 END) successful_pipelines,
              SUM(CASE WHEN run_status='FAILED' THEN 1 ELSE 0 END) failed_pipelines,
              SUM(CASE WHEN run_status='SKIPPED' THEN 1 ELSE 0 END) skipped_pipelines,
              SUM(COALESCE(rows_read,0)) total_rows_read,
              SUM(COALESCE(rows_inserted,0)) total_rows_inserted,
              SUM(COALESCE(rows_updated,0)) total_rows_updated,
              SUM(COALESCE(rows_deleted,0)) total_rows_deleted,
              SUM(COALESCE(rows_rejected,0)) total_rows_rejected
            FROM {pipeline_audit_table}
            WHERE workflow_audit_id = {q(self.workflow_audit_id)}
              AND batch_id = {q(self.batch_id)}
        """).first()
        failed = int(row["failed_pipelines"] or 0)
        status = "FAILED" if failed > 0 else "SUCCESS"
        duration = int((now - self.started_at).total_seconds()) if self.started_at else 0
        self.spark.sql(f"""
            UPDATE {self.workflow_audit_table}
            SET run_status = {q(status)},
                end_timestamp = {q(now)},
                duration_seconds = {duration},
                total_pipelines = {int(row["total_pipelines"] or 0)},
                successful_pipelines = {int(row["successful_pipelines"] or 0)},
                failed_pipelines = {failed},
                skipped_pipelines = {int(row["skipped_pipelines"] or 0)},
                total_rows_read = {int(row["total_rows_read"] or 0)},
                total_rows_inserted = {int(row["total_rows_inserted"] or 0)},
                total_rows_updated = {int(row["total_rows_updated"] or 0)},
                total_rows_deleted = {int(row["total_rows_deleted"] or 0)},
                total_rows_rejected = {int(row["total_rows_rejected"] or 0)},
                updated_timestamp = {q(now)}
            WHERE workflow_audit_id = {q(self.workflow_audit_id)}
        """)

    def fail(self, error: Exception) -> None:
        now = utc_now()
        duration = int((now - self.started_at).total_seconds()) if self.started_at else 0
        self.spark.sql(f"""
            UPDATE {self.workflow_audit_table}
            SET run_status = 'FAILED',
                end_timestamp = {q(now)},
                duration_seconds = {duration},
                error_code = {q(type(error).__name__)},
                error_message = {q(str(error)[:4000])},
                updated_timestamp = {q(now)}
            WHERE workflow_audit_id = {q(self.workflow_audit_id)}
        """)
