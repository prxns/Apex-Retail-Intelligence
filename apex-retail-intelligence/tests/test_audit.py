import sys
import os
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class TestAuditReconciliation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

    def test_reconcile_row_count(self):
        from audit.reconciliation import reconcile_row_count
        # Create dummy df with 3 rows
        schema = StructType([StructField("id", IntegerType(), True)])
        df = self.spark.createDataFrame([(1,), (2,), (3,)], schema)
        
        # We need a dummy audit csv
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("table_name,row_count\n")
            f.write("test_table,3\n")
            f.write("fail_table,5\n")
            audit_path = f.name
            
        try:
            recon_df, passed = reconcile_row_count(self.spark, df, audit_path, "test_table")
            self.assertTrue(passed)
            
            recon_df2, passed2 = reconcile_row_count(self.spark, df, audit_path, "fail_table")
            self.assertFalse(passed2)
        finally:
            os.remove(audit_path)

if __name__ == '__main__':
    unittest.main()
