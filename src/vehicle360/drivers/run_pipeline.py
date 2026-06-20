import argparse
from vehicle360.utils.spark import get_spark
from vehicle360.loaders.bronze_to_silver_loader import run_bronze_to_silver
from vehicle360.loaders.silver_to_gold_loader import run_silver_to_gold


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--workflow-audit-id", default=None)
    parser.add_argument("--layer", required=True, choices=["bronze_to_silver", "silver_to_gold"])
    args = parser.parse_args()

    spark = get_spark(f"vehicle360-{args.pipeline_id}")
    if args.layer == "bronze_to_silver":
        run_bronze_to_silver(spark, args.pipeline_id, args.batch_id, args.workflow_audit_id)
    else:
        run_silver_to_gold(spark, args.pipeline_id, args.batch_id, args.workflow_audit_id)


if __name__ == "__main__":
    main()
