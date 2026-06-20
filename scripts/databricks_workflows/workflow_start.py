import sys
# Update this path based on where you upload the project.
sys.path.append("/Workspace/Vehicle360/vehicle360_medallion_project/src")

from vehicle360.audit.audit_manager import WorkflowAuditManager
from vehicle360.utils.databricks import set_task_value

manager = WorkflowAuditManager(spark=spark, workflow_name="Vehicle360 Medallion Workflow")
ids = manager.start(trigger_type="WORKFLOW")
set_task_value(dbutils, "batch_id", ids["batch_id"])
set_task_value(dbutils, "workflow_audit_id", ids["workflow_audit_id"])
print(ids)
