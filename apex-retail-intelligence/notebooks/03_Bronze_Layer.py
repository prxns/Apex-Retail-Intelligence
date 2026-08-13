# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 — Bronze Delta
# MAGIC Preserve Landing values in Delta, add audit metadata, and keep historical/incremental batches separate.
# MAGIC Bronze is append-only: a deterministic batch_id prevents the same logical batch from being appended twice.

# COMMAND ----------
import os, sys
from pathlib import Path
for p in [os.environ.get("APEX_RETAIL_SRC_PATH"), str(Path.cwd() / "src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None]:
    if p and os.path.isdir(p) and p not in sys.path: sys.path.insert(0, p)
from config.paths import IS_DATABRICKS, LANDING_DIR, BRONZE_DIR
from pyspark.sql.functions import current_timestamp, lit, sha2, concat_ws
from delta.tables import DeltaTable

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("ApexBronze")

# COMMAND ----------

def process(entity, load_type):
    landing = f"{LANDING_DIR}/{entity}/{load_type}"
    bronze = f"{BRONZE_DIR}/{entity}/{load_type}"
    df = spark.read.parquet(landing)
    batch_id = sha2(concat_ws("|", lit("apex-retail"), lit(entity), lit(load_type)), 256)
    df = df.withColumn("batch_id", batch_id).withColumn("load_type", lit(load_type)).withColumn("source_file", lit(f"{entity}_{load_type}.csv")).withColumn("ingested_at", current_timestamp())

    if DeltaTable.isDeltaTable(spark, bronze):
        existing = spark.read.format("delta").load(bronze).select("batch_id").distinct()
        if existing.filter(existing.batch_id == batch_id).limit(1).count() > 0:
            print(f"BRONZE SKIP | {entity}_{load_type} already ingested")
            return
        df.write.format("delta").mode("append").save(bronze)
    else:
        df.write.format("delta").mode("overwrite").save(bronze)
    print(f"BRONZE PASS | {entity}_{load_type} | {df.count()} rows")


for entity in ["customer", "product", "sales"]:
    process(entity, "historical")
    process(entity, "incremental")
print("Phase 3 complete.")
