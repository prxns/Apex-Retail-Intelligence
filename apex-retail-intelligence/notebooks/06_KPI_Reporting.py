# Databricks notebook source
# MAGIC %md
# MAGIC # 06_KPI_Reporting
# MAGIC 
# MAGIC **Purpose**: Executive Snapshot & KPI Reporting
# MAGIC 1. Regional Net Margin
# MAGIC 2. Promotion Performance (AOV)
# MAGIC 3. Customer Churn Heatmap
# MAGIC 4. Product Quality
# MAGIC 5. Store Traffic by Hour

# COMMAND ----------

import sys
import os
from pathlib import Path
for _candidate in (os.environ.get("APEX_RETAIL_SRC_PATH"), os.path.join(os.getcwd(), "src"), os.path.abspath("../src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)
from config.paths import IS_DATABRICKS, GOLD_DIR
from pyspark.sql.functions import col, sum as _sum, count, avg, max as _max, when, round

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("KPIReporting")
    def display(df): df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold Data

# COMMAND ----------

fact_sales = spark.read.format("delta").load(f"{GOLD_DIR}/fact_sales")
dim_cust = spark.read.format("delta").load(f"{GOLD_DIR}/dim_customer")
dim_prod = spark.read.format("delta").load(f"{GOLD_DIR}/dim_product")
dim_promo = spark.read.format("delta").load(f"{GOLD_DIR}/dim_promotion")
dim_date = spark.read.format("delta").load(f"{GOLD_DIR}/dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 1 - Regional Net Margin
# MAGIC Net Margin = Total Sales - Discounts Applied

# COMMAND ----------

# Regional Net Margin
kpi1 = fact_sales.groupBy("store_location").agg(
    _sum("total_sales").alias("gross_revenue"),
    _sum("discount_applied").alias("discount_amount")
).withColumn(
    "net_margin", col("gross_revenue") - col("discount_amount")
).orderBy(col("net_margin").desc())

print("--- KPI 1: Regional Net Margin ---")
display(kpi1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 2 - AOV by Promotion

# COMMAND ----------

# AOV by Promotion
joined_promo = fact_sales.join(dim_promo, "promotion_sk", "left")

kpi2 = joined_promo.groupBy("promotion_type").agg(
    count("transaction_id").alias("transaction_count"),
    _sum("total_sales").alias("total_sales")
).withColumn(
    "average_order_value", 
    when(col("transaction_count") > 0, round(col("total_sales") / col("transaction_count"), 2)).otherwise(0.0)
).orderBy(col("average_order_value").desc())

print("--- KPI 2: AOV by Promotion ---")
display(kpi2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 3 - Demographic Churn Heatmap

# COMMAND ----------

# Customer Churn Heatmap
# Using dim_customer
kpi3 = dim_cust.filter(col("is_current") == True).groupBy("customer_state", "loyalty_program").agg(
    count("customer_sk").alias("total_customers"),
    _sum(when(col("churned") == "True", 1).otherwise(0)).alias("churned_customers")
).withColumn(
    "churn_rate",
    when(col("total_customers") > 0, round((col("churned_customers") / col("total_customers")) * 100, 2)).otherwise(0.0)
).orderBy("customer_state", "loyalty_program")

print("--- KPI 3: Demographic Churn Heatmap ---")
display(kpi3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 4 - Product Quality

# COMMAND ----------

# Product Quality
kpi4 = dim_prod.groupBy("product_category").agg(
    count("product_sk").alias("number_of_products"),
    round(avg("product_return_rate"), 4).alias("average_return_rate"),
    _max("product_return_rate").alias("highest_return_rate")
).orderBy(col("average_return_rate").desc())

print("--- KPI 4: Product Quality ---")
display(kpi4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI 5 - Store Traffic by Hour (Proxy)

# COMMAND ----------

# Store Traffic by Hour
joined_date = fact_sales.join(dim_date, "date_sk", "left")

kpi5 = joined_date.groupBy("store_location", "day_of_week", "transaction_hour").agg(
    count("transaction_id").alias("transaction_count")
).orderBy(col("transaction_count").desc())

print("--- KPI 5: Store Traffic by Hour (Transaction Proxy) ---")
display(kpi5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Observations & Limitations
# MAGIC - The churn rate uses explicit boolean check; ensure `churned` is cast or string matched appropriately.
# MAGIC - Traffic proxy indicates busiest transaction times, which might differ from actual footfall.
# MAGIC - Sales transactions are deduplicated, ensuring ledger immutability and precise margins.
