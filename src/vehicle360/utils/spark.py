from pyspark.sql import SparkSession


def get_spark(app_name: str = "vehicle360-medallion") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()
