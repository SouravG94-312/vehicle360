import sys
sys.path.append("/Workspace/Vehicle360/vehicle360_medallion_project/src")

from vehicle360.audit.audit_manager import WorkflowAuditManager
from vehicle360.utils.databricks import get_widget

batch_id = get_widget(dbutils, "batch_id")
workflow_audit_id = get_widget(dbutils, "workflow_audit_id")

manager = WorkflowAuditManager(
    spark=spark,
    workflow_name="Vehicle360 Medallion Workflow",
    batch_id=batch_id,
    workflow_audit_id=workflow_audit_id,
)

try:
    raise RuntimeError("Vehicle360 workflow failed. Review failed upstream task and pipeline_run_audit.")
except Exception as e:
    manager.fail(e)
