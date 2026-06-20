# Vehicle 360 Medallion Project - Updated Full Load Codebase

## What this version includes

This version updates the codebase to support all registered loads:

### Bronze to Silver

- vehicle_model
- dealer
- customer
- vehicle
- part
- failure_code
- service_order
- repair_job
- part_consumption
- warranty_claim
- vehicle_daily_usage
- breakdown

### Silver to Gold Dimensions

- dim_date
- dim_vehicle_model
- dim_customer
- dim_dealer
- dim_part
- dim_failure_code
- dim_vehicle

### Silver to Gold Facts

- fact_service_order
- fact_repair_job
- fact_part_consumption
- fact_warranty_claim
- fact_vehicle_daily_usage
- fact_breakdown

## Key improvement

The loaders now call `align_to_target_schema()` before writing to Delta.

This makes the framework safer because it:

- reads the actual target table schema from Databricks
- adds missing columns as NULL
- drops extra dataframe columns
- casts existing columns to the target datatype
- preserves the target table column order

This is important for your project because your actual Databricks tables have shown several schema differences from the planned DDL.

## Main scripts

```text
scripts/bootstrap/01_create_metadata_entries.sql
scripts/databricks_workflows/workflow_start.py
scripts/databricks_workflows/run_pipeline_task.py
scripts/databricks_workflows/workflow_complete.py
scripts/databricks_workflows/workflow_fail.py
```

## Main modules

```text
src/vehicle360/loaders/bronze_to_silver_loader.py
src/vehicle360/loaders/silver_to_gold_loader.py
src/vehicle360/transformations/bronze_to_silver.py
src/vehicle360/transformations/silver_to_gold.py
src/vehicle360/core/schema_utils.py
src/vehicle360/core/merge.py
src/vehicle360/audit/audit_manager.py
src/vehicle360/dq/quality_engine.py
```

## First run

1. Upload the project to Databricks.
2. Update the `sys.path.append(...)` path in the workflow scripts if your workspace path is different.
3. Run:

```sql
scripts/bootstrap/01_create_metadata_entries.sql
```

4. Test one Silver pipeline:

```python
from vehicle360.loaders.bronze_to_silver_loader import run_bronze_to_silver

run_bronze_to_silver(
    spark=spark,
    pipeline_id="PL_SILVER_VEHICLE",
    batch_id="BATCH_TEST_001",
    workflow_audit_id=None
)
```

5. Test one Gold pipeline:

```python
from vehicle360.loaders.silver_to_gold_loader import run_silver_to_gold

run_silver_to_gold(
    spark=spark,
    pipeline_id="PL_GOLD_DIM_VEHICLE",
    batch_id="BATCH_TEST_001",
    workflow_audit_id=None
)
```
