"""Reusable audit reconciliation used by Landing and Silver."""
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


from .core import reconcile_counts


def reconcile_row_count(spark, df: DataFrame, audit_csv_path: str, expected_table_name: str):
    """Return a one-row reconciliation DataFrame and pass flag; fail closed on malformed audits."""
    audit = (
        spark.read.option("header", "true").option("mode", "FAILFAST").csv(audit_csv_path)
    )
    required = {"table_name", "row_count"}
    missing = required.difference(audit.columns)
    if missing:
        raise ValueError(f"Audit file {audit_csv_path} missing columns: {sorted(missing)}")

    rows = audit.filter(col("table_name") == expected_table_name).select("row_count").collect()
    if len(rows) != 1:
        raise ValueError(
            f"Expected exactly one audit row for '{expected_table_name}' in {audit_csv_path}; found {len(rows)}"
        )
    try:
        expected = int(rows[0]["row_count"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid row_count for {expected_table_name}") from exc

    actual = df.count()
    difference, status = reconcile_counts(expected, actual)
    result = spark.createDataFrame(
        [(expected_table_name, expected, actual, difference, status)],
        ["dataset", "expected", "actual", "difference", "status"],
    )
    return result, status == "PASS"


def assert_reconciliation(passed: bool, dataset: str) -> None:
    if not passed:
        raise RuntimeError(f"MANDATORY AUDIT FAILED: {dataset}")
