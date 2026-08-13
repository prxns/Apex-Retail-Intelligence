"""Spark construction for local validation; Databricks supplies its own Spark session."""
from .paths import IS_DATABRICKS


def get_spark(app_name: str):
    if IS_DATABRICKS:
        return spark  # noqa: F821 - Databricks runtime global
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
