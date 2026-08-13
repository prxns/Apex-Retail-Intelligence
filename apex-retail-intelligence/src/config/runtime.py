"""Spark session construction used only outside Databricks."""
from .paths import IS_DATABRICKS


def get_spark(app_name):
    if IS_DATABRICKS:
        return spark  # noqa: F821 - supplied by the Databricks notebook runtime
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:
        raise RuntimeError("Local Delta execution requires the delta-spark dependency.") from exc
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
