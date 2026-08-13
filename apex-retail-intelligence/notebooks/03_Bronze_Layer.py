# Databricks notebook source
# MAGIC %md
# MAGIC # 03_Bronze_Layer
# MAGIC 
# MAGIC **Purpose**: Append Landing Parquet data to Bronze Delta tables. Preserves source values, historical/incremental tracking, avoids duplicate ingestion of same batches.

# COMMAND ----------

import sys
import os
from pathlib import Path
for _candidate in (os.environ.get("APEX_RETAIL_SRC_PATH"), os.path.join(os.getcwd(), "src"), os.path.abspath("../src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)
from config.paths import IS_DATABRICKS, LANDING_DIR, BRONZE_DIR

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("BronzeLayer")

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# COMMAND ----------

def process_bronze_entity(entity):
    landing_paths = [f"{LANDING_DIR}/{entity}/historical", f"{LANDING_DIR}/{entity}/incremental"]
    bronze_path = f"{BRONZE_DIR}/{entity}"
    
    for l_path in landing_paths:
        try:
            df_landing = spark.read.parquet(l_path)
            load_type = l_path.split("/")[-1]
            
            # Idempotency check: don't ingest the same load_type again if it already exists with identical timestamps
            # For simplicity, if we re-run, we might overwrite or merge. 
            # The prompt asks for "append-oriented processing... Bronze must be safely rerunnable. Do not append the same ingestion batch repeatedly"
            
            if not DeltaTable.isDeltaTable(spark, bronze_path):
                df_landing.write.format("delta").mode("overwrite").save(bronze_path)
            else:
                # To be safely rerunnable, we can delete the existing load_type batch before appending, or use merge.
                # Since Bronze is append-only theoretically, the safest idempotency without watermarking is to overwrite the specific partition/load_type,
                # or just use merge on all columns. We'll use a transaction/batch ID or just overwrite where load_type matches (if we treat historical/incremental as partitions).
                # To be purely append-only but idempotent, we can MERGE on everything.
                dt = DeltaTable.forPath(spark, bronze_path)
                
                # We can delete existing load_type to simulate idempotent append
                dt.delete(col("load_type") == load_type)
                
                # Now append
                df_landing.write.format("delta").mode("append").save(bronze_path)
                
        except Exception as e:
            raise RuntimeError(f"Bronze processing failed for {entity} at {l_path}") from e

# COMMAND ----------

entities = ["customer", "product", "sales"]
for e in entities:
    print(f"Processing Bronze for {e}...")
    process_bronze_entity(e)

print("Bronze Layer processing complete.")
