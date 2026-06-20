import sys
# Update this path based on where you upload the project.
sys.path.append("/Workspace/Vehicle360/vehicle360_medallion_project/src")

from vehicle360.loaders.bronze_to_silver_loader import run_bronze_to_silver
from vehicle360.loaders.silver_to_gold_loader import run_silver_to_gold
from vehicle360.utils.databricks import get_widget

pipeline_id = get_widget(dbutils, "pipeline_id")
batch_id = get_widget(dbutils, "batch_id")
workflow_audit_id = get_widget(dbutils, "workflow_audit_id")
layer = get_widget(dbutils, "layer", "bronze_to_silver")

if layer == "bronze_to_silver":
    run_bronze_to_silver(spark, pipeline_id, batch_id, workflow_audit_id)
elif layer == "silver_to_gold":
    run_silver_to_gold(spark, pipeline_id, batch_id, workflow_audit_id)
else:
    raise ValueError(f"Unsupported layer: {layer}")
