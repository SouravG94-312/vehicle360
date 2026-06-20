# Vehicle 360 Full Load Runbook

## 1. Bootstrap metadata

Run:

```sql
scripts/bootstrap/01_create_metadata_entries.sql
```

Expected result:

```text
SILVER = 12
GOLD   = 13
```

Validate:

```sql
SELECT pipeline_group, execution_order, pipeline_id, source_object, target_object
FROM vehicle360.control.pipeline_config
WHERE pipeline_id LIKE 'PL_SILVER_%' OR pipeline_id LIKE 'PL_GOLD_%'
ORDER BY pipeline_group, execution_order;
```

## 2. Test imports

```python
import sys
sys.path.append("/Workspace/Vehicle360/vehicle360_medallion_project/src")

from vehicle360.loaders.bronze_to_silver_loader import run_bronze_to_silver
from vehicle360.loaders.silver_to_gold_loader import run_silver_to_gold
print("imports ok")
```

## 3. Run all Silver loads

```python
from vehicle360.loaders.bronze_to_silver_loader import run_bronze_to_silver

silver = [
    "PL_SILVER_VEHICLE_MODEL",
    "PL_SILVER_DEALER",
    "PL_SILVER_CUSTOMER",
    "PL_SILVER_VEHICLE",
    "PL_SILVER_PART",
    "PL_SILVER_FAILURE_CODE",
    "PL_SILVER_SERVICE_ORDER",
    "PL_SILVER_REPAIR_JOB",
    "PL_SILVER_PART_CONSUMPTION",
    "PL_SILVER_WARRANTY_CLAIM",
    "PL_SILVER_VEHICLE_USAGE",
    "PL_SILVER_BREAKDOWN"
]

for p in silver:
    print(f"Running {p}")
    run_bronze_to_silver(spark, p, "BATCH_SILVER_001", None)
```

## 4. Run all Gold loads

```python
from vehicle360.loaders.silver_to_gold_loader import run_silver_to_gold

gold = [
    "PL_GOLD_DIM_DATE",
    "PL_GOLD_DIM_VEHICLE_MODEL",
    "PL_GOLD_DIM_CUSTOMER",
    "PL_GOLD_DIM_DEALER",
    "PL_GOLD_DIM_PART",
    "PL_GOLD_DIM_FAILURE_CODE",
    "PL_GOLD_DIM_VEHICLE",
    "PL_GOLD_FACT_SERVICE_ORDER",
    "PL_GOLD_FACT_REPAIR_JOB",
    "PL_GOLD_FACT_PART_CONSUMPTION",
    "PL_GOLD_FACT_WARRANTY_CLAIM",
    "PL_GOLD_FACT_VEHICLE_DAILY_USAGE",
    "PL_GOLD_FACT_BREAKDOWN"
]

for p in gold:
    print(f"Running {p}")
    run_silver_to_gold(spark, p, "BATCH_GOLD_001", None)
```

## 5. Audit validation

```sql
SELECT pipeline_id, run_status, rows_read, rows_inserted, rows_updated, rows_rejected, error_message
FROM vehicle360.control.pipeline_run_audit
ORDER BY start_timestamp DESC
LIMIT 50;
```

## 6. Row-count validation

```sql
SELECT 'dim_vehicle' table_name, COUNT(*) row_count FROM vehicle360.gold.dim_vehicle
UNION ALL
SELECT 'dim_vehicle_model', COUNT(*) FROM vehicle360.gold.dim_vehicle_model
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM vehicle360.gold.dim_customer
UNION ALL
SELECT 'dim_dealer', COUNT(*) FROM vehicle360.gold.dim_dealer
UNION ALL
SELECT 'dim_part', COUNT(*) FROM vehicle360.gold.dim_part
UNION ALL
SELECT 'dim_failure_code', COUNT(*) FROM vehicle360.gold.dim_failure_code
UNION ALL
SELECT 'fact_service_order', COUNT(*) FROM vehicle360.gold.fact_service_order
UNION ALL
SELECT 'fact_repair_job', COUNT(*) FROM vehicle360.gold.fact_repair_job
UNION ALL
SELECT 'fact_part_consumption', COUNT(*) FROM vehicle360.gold.fact_part_consumption
UNION ALL
SELECT 'fact_warranty_claim', COUNT(*) FROM vehicle360.gold.fact_warranty_claim
UNION ALL
SELECT 'fact_vehicle_daily_usage', COUNT(*) FROM vehicle360.gold.fact_vehicle_daily_usage
UNION ALL
SELECT 'fact_breakdown', COUNT(*) FROM vehicle360.gold.fact_breakdown;
```

## 7. Common troubleshooting

### If a table write fails with a column mismatch

The loader already aligns dataframe columns to the target schema. If it still fails, run:

```sql
DESCRIBE TABLE vehicle360.gold.<table_name>;
DESCRIBE TABLE vehicle360.silver.<source_table>;
```

Then adjust:

```text
src/vehicle360/transformations/silver_to_gold.py
```

### If a pipeline is missing

Run the bootstrap SQL again.

### If `ModuleNotFoundError` happens

Update:

```python
sys.path.append("/Workspace/Vehicle360/vehicle360_medallion_project/src")
```
