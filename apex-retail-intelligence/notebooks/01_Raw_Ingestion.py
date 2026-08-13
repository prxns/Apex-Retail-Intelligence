# Databricks notebook source
# MAGIC %md
# MAGIC # 01_Raw_Ingestion
# MAGIC 
# MAGIC **Purpose**: Discover actual incoming CSV files, separate historical and incremental datasets, read data as STRING, and write/copy them to the Raw zone without modification.

# COMMAND ----------

import sys
import os
from pathlib import Path

# Allow importing from src when run as a local script or a Databricks Repo notebook.
for _candidate in (os.environ.get("APEX_RETAIL_SRC_PATH"), os.path.join(os.getcwd(), "src"), os.path.abspath("../src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)
from config.paths import IS_DATABRICKS, INCOMING_HISTORICAL, INCOMING_INCREMENTAL, RAW_DIR

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("RawIngestion")
    def list_dir_local(path):
        import glob
        return [f.replace('\\', '/') for f in glob.glob(f"{path}/**/*.csv", recursive=True)]
else:
    def list_dir_local(path):
        files = []
        try:
            for f in dbutils.fs.ls(path):
                if f.isDir():
                    files.extend(list_dir_local(f.path))
                elif f.name.endswith('.csv'):
                    files.append(f.path)
        except Exception:
            pass
        return files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input & Processing
# MAGIC We read incoming CSVs as strings and save them to the Raw zone.
# MAGIC This preserves source values and historical/incremental distinction.

# COMMAND ----------

def ingest_to_raw(source_dir, dest_dir, load_type):
    # Discover CSV files
    csv_files = list_dir_local(source_dir)
    if not csv_files:
        print(f"No CSV files found in {source_dir}")
        return

    for file_path in csv_files:
        # Determine entity (customer, product, sales) from filename or folder
        file_name = file_path.split('/')[-1].lower()
        entity = "unknown"
        if "customer" in file_name: entity = "customer"
        elif "product" in file_name: entity = "product"
        elif "sales" in file_name: entity = "sales"
        else: continue

        # We must not process audit files here
        if "audit" in file_name: continue

        print(f"Ingesting {entity} ({load_type}) from {file_path}")
        
        # Read as STRING
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(file_path)
        
        # Write to Raw zone (keeping it as CSV or basic Parquet)
        # We will write as CSV to fulfill "Raw CSV -> Parquet" in Phase 2
        out_path = f"{RAW_DIR}/{entity}/{load_type}"
        df.write.mode("overwrite").option("header", "true").csv(out_path)

# COMMAND ----------

print("Ingesting Historical Data...")
ingest_to_raw(INCOMING_HISTORICAL, RAW_DIR, "historical")

print("Ingesting Incremental Data...")
ingest_to_raw(INCOMING_INCREMENTAL, RAW_DIR, "incremental")

print("Raw ingestion complete.")
