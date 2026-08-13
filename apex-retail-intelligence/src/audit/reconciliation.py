from pyspark.sql.functions import col


def reconcile_row_count(spark, df, audit_csv_path, expected_table_name):
    """Compare a DataFrame's actual count with the supplied audit CSV entry."""
    try:
        audit_df = spark.read.option("header", "true").option("mode", "FAILFAST").csv(audit_csv_path)
        required = {"table_name", "row_count"}
        if not required.issubset(audit_df.columns):
            raise ValueError(f"Audit file {audit_csv_path} must contain {sorted(required)}")
        expected_rows = audit_df.filter(col("table_name") == expected_table_name).select("row_count").collect()
        if len(expected_rows) != 1:
            raise ValueError(f"Expected exactly one audit record for {expected_table_name} in {audit_csv_path}")
        expected_count = int(expected_rows[0]["row_count"])
        actual_count = df.count()
        difference = actual_count - expected_count
        status = "PASS" if difference == 0 else "FAIL"
        result = spark.createDataFrame(
            [(expected_table_name, expected_count, actual_count, difference, status)],
            ["dataset", "expected", "actual", "difference", "status"],
        )
        return result, status == "PASS"
    except Exception as exc:
        print(f"Audit file read error for {expected_table_name}: {exc}")
        return None, False
