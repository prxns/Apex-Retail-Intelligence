# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — Gold Star Schema
# MAGIC Build `dim_customer`, `dim_product`, `dim_promotion`, `dim_date`, and `fact_sales`, then register them in Unity Catalog under `GOLD_tables`.

# COMMAND ----------
import os, sys
from pathlib import Path
for p in [os.environ.get("APEX_RETAIL_SRC_PATH"), str(Path.cwd() / "src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None]:
    if p and os.path.isdir(p) and p not in sys.path: sys.path.insert(0, p)
from config.paths import IS_DATABRICKS, SILVER_DIR, GOLD_DIR, CATALOG_NAME, GOLD_SCHEMA
from gold.dimensions import build_dim_customer, build_dim_product, build_dim_promotion, build_dim_date, build_fact_sales, register_gold_tables
from pyspark.sql.functions import col

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("ApexGold")

# COMMAND ----------

dim_customer = build_dim_customer(spark, f"{SILVER_DIR}/customer", GOLD_DIR)
dim_product = build_dim_product(spark, f"{SILVER_DIR}/product", GOLD_DIR)
dim_promotion = build_dim_promotion(spark, f"{SILVER_DIR}/sales", GOLD_DIR)
dim_date = build_dim_date(spark, f"{SILVER_DIR}/sales", GOLD_DIR)
fact_sales = build_fact_sales(spark, f"{SILVER_DIR}/sales", GOLD_DIR)

# Referential integrity checks. Unknown member SK 0 is intentional and documented.
assert fact_sales.filter(col("customer_sk").isNull()).limit(1).count() == 0
assert fact_sales.filter(col("product_sk").isNull()).limit(1).count() == 0
assert fact_sales.filter(col("promotion_sk").isNull()).limit(1).count() == 0
assert fact_sales.filter(col("date_sk").isNull()).limit(1).count() == 0

if IS_DATABRICKS:
    register_gold_tables(spark, CATALOG_NAME, GOLD_SCHEMA, GOLD_DIR)
    print(f"Unity Catalog registration complete: {CATALOG_NAME}.{GOLD_SCHEMA}")
else:
    print("Unity Catalog registration skipped outside Databricks.")

print("Phase 5 complete — Gold Star Schema built.")
