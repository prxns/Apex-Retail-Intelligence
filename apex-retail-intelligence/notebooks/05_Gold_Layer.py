# Databricks notebook source
# MAGIC %md
# MAGIC # 05_Gold_Layer
# MAGIC 
# MAGIC **Purpose**: Build Gold Star Schema (dim_customer, dim_product, dim_promotion, dim_date, fact_sales).
# MAGIC Register in Unity Catalog.

# COMMAND ----------

import sys
import os
from pathlib import Path
for _candidate in (os.environ.get("APEX_RETAIL_SRC_PATH"), os.path.join(os.getcwd(), "src"), os.path.abspath("../src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)
from config.paths import IS_DATABRICKS, SILVER_DIR, GOLD_DIR
from gold.dimensions import build_dim_customer, build_dim_product, build_dim_promotion, build_dim_date, build_fact_sales

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("GoldLayer")

# Configurable Unity Catalog target (can be overridden)
CATALOG_NAME = os.environ.get("APEX_RETAIL_CATALOG", "main")
SCHEMA_NAME = os.environ.get("APEX_RETAIL_GOLD_SCHEMA", "GOLD_tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold Tables

# COMMAND ----------

print("Building dim_customer...")
build_dim_customer(spark, f"{SILVER_DIR}/customer", GOLD_DIR)

print("Building dim_product...")
build_dim_product(spark, f"{SILVER_DIR}/product", GOLD_DIR)

print("Building dim_promotion...")
build_dim_promotion(spark, f"{SILVER_DIR}/sales", GOLD_DIR)

print("Building dim_date...")
build_dim_date(spark, f"{SILVER_DIR}/sales", GOLD_DIR)

print("Building fact_sales...")
build_fact_sales(spark, f"{SILVER_DIR}/sales", GOLD_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Registration

# COMMAND ----------

def register_tables():
    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
    
    tables = ["dim_customer", "dim_product", "dim_promotion", "dim_date", "fact_sales"]
    for t in tables:
        path = f"{GOLD_DIR}/{t}"
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{t} USING DELTA LOCATION '{path}'")
            print(f"Registered {t} in {CATALOG_NAME}.{SCHEMA_NAME}")
        except Exception as e:
            print(f"Could not register {t} in Unity Catalog. It might not be available in this environment. Error: {e}")

if IS_DATABRICKS:
    register_tables()
else:
    print("Unity Catalog registration skipped outside Databricks.")
print("Gold layer processing complete.")
