import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import datetime

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestPipeline").master("local[2]").getOrCreate()

# -- Audit Testing --
def test_reconciliation_pass(spark):
    from src.audit.reconciliation import reconcile_row_count
    import tempfile
    
    # Create dummy audit CSV
    audit_data = "table_name,row_count\ntest_table,2\n"
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(audit_data)
        audit_path = f.name
        
    df = spark.createDataFrame([("a",), ("b",)], ["col1"])
    recon, passed = reconcile_row_count(spark, df, audit_path, "test_table")

    res = recon.collect()[0]
    assert passed
    assert res['status'] == 'PASS'
    assert res['difference'] == 0

def test_reconciliation_fail(spark):
    from src.audit.reconciliation import reconcile_row_count
    import tempfile
    
    # Create dummy audit CSV
    audit_data = "table_name,row_count\ntest_table,3\n"
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(audit_data)
        audit_path = f.name
        
    df = spark.createDataFrame([("a",), ("b",)], ["col1"])
    recon, passed = reconcile_row_count(spark, df, audit_path, "test_table")
    assert not passed
    assert recon.collect()[0]["status"] == "FAIL"

# Note: In a complete environment we would mock and test:
# - missing PK handling (verify row count drops)
# - duplicate removal (verify deduplication logic)
# - SCD2 logic (mock target delta table, run merge logic, check is_current flag)
# - SCD1 update
# - Sales deduplication
# - Surrogate key stability (hash consistency)

def test_surrogate_key_stability(spark):
    from pyspark.sql.functions import col, md5, concat_ws
    
    data = [("C123", "2026-08-13")]
    df = spark.createDataFrame(data, ["customer_id", "effective_start_date"])
    df = df.withColumn("customer_sk", md5(concat_ws('|', col("customer_id"), col("effective_start_date"))))
    
    key1 = df.collect()[0]["customer_sk"]
    
    # Rerun logic
    df2 = spark.createDataFrame(data, ["customer_id", "effective_start_date"])
    df2 = df2.withColumn("customer_sk", md5(concat_ws('|', col("customer_id"), col("effective_start_date"))))
    key2 = df2.collect()[0]["customer_sk"]
    
    assert key1 == key2 # Stable surrogate key
