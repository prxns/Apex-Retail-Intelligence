# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Raw Ingestion
# MAGIC Read supplied CSV payloads into a Raw zone while preserving source columns as strings.
# MAGIC Historical and incremental loads remain separated; audit files are not treated as business data.

# COMMAND ----------
import os
import sys

# Add this project's src/ directory to Python's import path.
PROJECT_SRC = "/Workspace/Users/pranshurwt2003@gmail.com/Apex-Retail-Intelligence/src"
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

from config.paths import IS_DATABRICKS, INCOMING_HISTORICAL, INCOMING_INCREMENTAL, RAW_DIR
from config.contracts import SOURCE_COLUMNS, string_schema

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("ApexRawIngestion")

# COMMAND ----------
def list_csv(path):
    if IS_DATABRICKS:
        files = []
        for item in dbutils.fs.ls(path):
            if item.isDir(): files.extend(list_csv(item.path))
            elif item.name.lower().endswith(".csv"): files.append(item.path)
        return files
    import glob
    return glob.glob(f"{path}/**/*.csv", recursive=True)


def ingest_entity(entity, source_root, load_type):
    files = [f for f in list_csv(source_root) if "audit" not in f.lower() and entity in f.lower()]
    if not files:
        raise FileNotFoundError(f"No {entity} CSV found under {source_root}")
    # A source load is one logical dataset. Multiple matching files are unioned by name.
    frames = []
    for file_path in files:
        df = (
           spark.read 
           .option("header", "true")
           .option("mode", "PERMISSIVE")
           .schema(string_schema(entity))
           .csv(file_path)
        )
        frames.append(df)
    df = frames[0]
    for other in frames[1:]: df = df.unionByName(other, allowMissingColumns=False)
    out = f"{RAW_DIR}/{entity}/{load_type}"
    df.write.mode("overwrite").option("header", "true").csv(out)
    display(
)
    print(f"RAW PASS | {entity}_{load_type} | {df.count()} rows | {out}")


for entity in SOURCE_COLUMNS:
    ingest_entity(entity, INCOMING_HISTORICAL, "historical")
    ingest_entity(entity, INCOMING_INCREMENTAL, "incremental")

print("Phase 1 complete.")