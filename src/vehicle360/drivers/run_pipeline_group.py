import argparse
from uuid import uuid4
from vehicle360.utils.spark import get_spark
from vehicle360.metadata.repository import MetadataRepository
from vehicle360.loaders.bronze_to_silver_loader import run_bronze_to_silver
from vehicle360.loaders.silver_to_gold_loader import run_silver_to_gold


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-group", required=True, choices=["SILVER", "GOLD"])
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--workflow-audit-id", default=None)
    args = parser.parse_args()

    spark = get_spark(f"vehicle360-{args.pipeline_group}")
    batch_id = args.batch_id or str(uuid4())
    repo = MetadataRepository(spark)

    for cfg in repo.list_active_pipelines(args.pipeline_group):
        print(f"Running {cfg.pipeline_id}")
        if args.pipeline_group == "SILVER":
            run_bronze_to_silver(spark, cfg.pipeline_id, batch_id, args.workflow_audit_id)
        else:
            run_silver_to_gold(spark, cfg.pipeline_id, batch_id, args.workflow_audit_id)


if __name__ == "__main__":
    main()
