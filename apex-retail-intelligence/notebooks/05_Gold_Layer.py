# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5: Gold Star Schema
# MAGIC Build `dim_customer`, `dim_product`, `dim_promotion`, `dim_date`, and `fact_sales`, then register them in Unity Catalog under `GOLD_tables`.

# COMMAND ----------
import os
import sys

PROJECT_SRC = "/Workspace/Users/pranshurwt2003@gmail.com/Apex-Retail-Intelligence/src"
if PROJECT_SRC not in sys.path:
    sys.path.insert(0, PROJECT_SRC)

from pyspark.sql import SparkSession

# Explicitly create Spark in this same cell
spark = SparkSession.builder.getOrCreate()

from config.paths import (
    IS_DATABRICKS,
    SILVER_DIR,
    GOLD_DIR,
    CATALOG_NAME,
    GOLD_SCHEMA,
)

from gold.dimensions import (
    build_dim_customer,
    build_dim_product,
    build_dim_promotion,
    build_dim_date,
    build_fact_sales,
    register_gold_tables,
)

from pyspark.sql.functions import col

print("Gold imports ready")

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

print("Gold tables built successfully.")

print("Phase 5 complete — Gold Star Schema built.")