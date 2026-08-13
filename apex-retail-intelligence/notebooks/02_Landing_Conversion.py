# Databricks notebook source
# MAGIC %md
# MAGIC # 02_Landing_Conversion
# MAGIC 
# MAGIC **Purpose**: Convert Raw CSVs to Parquet, dynamically read audit files, compare expected row counts against actuals, output reconciliation table, and fail if audit fails.

# COMMAND ----------

import sys
import os
from pathlib import Path
for _candidate in (os.environ.get("APEX_RETAIL_SRC_PATH"), os.path.join(os.getcwd(), "src"), os.path.abspath("../src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)
from config.paths import IS_DATABRICKS, RAW_DIR, LANDING_DIR, AUDIT_LANDING
from audit.reconciliation import reconcile_row_count

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("LandingConversion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process & Audit

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

def process_landing_for_entity(entity):
    all_recon_dfs = []
    has_failure = False
    
    for load_type in ["historical", "incremental"]:
        raw_path = f"{RAW_DIR}/{entity}/{load_type}"
        
        try:
            # Read CSV as string
            df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw_path)
            
            # Determine expected table name for audit
            expected_table_name = f"{entity}_{load_type}"
            audit_file = f"{entity}_{load_type}_audit.csv" if load_type == "historical" else f"{entity}_incrementalaudit.csv"
            audit_path = f"{AUDIT_LANDING}/{audit_file}"
            
            # Reconcile
            recon_df, passed = reconcile_row_count(spark, df, audit_path, expected_table_name)
            if recon_df is not None:
                all_recon_dfs.append(recon_df)
            
            if not passed:
                has_failure = True
                print(f"AUDIT FAILURE for {expected_table_name}")
            
            # Write to Parquet efficiently
            out_path = f"{LANDING_DIR}/{entity}/{load_type}"
            
            # Add metadata for tracking
            df = df.withColumn("load_type", lit(load_type)) \
                .withColumn("source_file", input_file_name()) \
                .withColumn("ingested_at", current_timestamp())
            df.write.mode("overwrite").parquet(out_path)
            
        except Exception as e:
            has_failure = True
            print(f"Error in {entity} {load_type}: {e}")
            
    return all_recon_dfs, has_failure

# COMMAND ----------

entities = ["customer", "product", "sales"]
final_recon_dfs = []
any_failure = False

for e in entities:
    print(f"Processing Landing for {e}...")
    recon_dfs, has_failure = process_landing_for_entity(e)
    final_recon_dfs.extend(recon_dfs)
    if has_failure:
        any_failure = True

# Output structured reconciliation table
if final_recon_dfs:
    final_df = final_recon_dfs[0]
    for r in final_recon_dfs[1:]:
        final_df = final_df.union(r)
    final_df.show(truncate=False)
    if IS_DATABRICKS:
        display(final_df)

if any_failure:
    raise Exception("MANDATORY AUDIT FAILED. Stopping downstream processing.")
print("Landing conversion complete.")
