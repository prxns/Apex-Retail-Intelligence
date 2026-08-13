import sys
import os
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class TestDQ(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

    def test_clean_customer_dedup(self):
        from quality.dq_rules import clean_customer
        
        # Test missing PK and deduplication
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        data = [
            (None, "2023-01-01", 30), # Should be dropped (no PK)
            ("C1", "2023-01-01", 30),
            ("C1", "2023-01-02", 31)  # Should keep this latest one
        ]
        df = self.spark.createDataFrame(data, schema)
        clean_df = clean_customer(df)
        
        results = clean_df.collect()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["age"], 31)

if __name__ == '__main__':
    unittest.main()
