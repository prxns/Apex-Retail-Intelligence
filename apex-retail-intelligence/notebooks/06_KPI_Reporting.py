# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 6 — Apex Retail Intelligence KPI Report
# MAGIC All five mandatory KPIs are calculated from the Gold Star Schema and rendered directly in Databricks. No external BI dashboard is used.

# COMMAND ----------
import os, sys
from pathlib import Path
for p in [os.environ.get("APEX_RETAIL_SRC_PATH"), str(Path.cwd() / "src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None]:
    if p and os.path.isdir(p) and p not in sys.path: sys.path.insert(0, p)
from config.paths import IS_DATABRICKS, GOLD_DIR
from gold.kpis import net_margin_by_region, aov_by_promotion, churn_heatmap, product_quality, store_traffic_proxy

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("ApexKPI")
    display = lambda df: df.show(truncate=False)

# COMMAND ----------
fact = spark.read.format("delta").load(f"{GOLD_DIR}/fact_sales")
customers = spark.read.format("delta").load(f"{GOLD_DIR}/dim_customer")
products = spark.read.format("delta").load(f"{GOLD_DIR}/dim_product")
promotions = spark.read.format("delta").load(f"{GOLD_DIR}/dim_promotion")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Net Margin by Region
# MAGIC **Required definition:** total gross revenue minus `discount_applied`, grouped by store region/location.

kpi1 = net_margin_by_region(fact)
display(kpi1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Average Order Value by Promotion
# MAGIC AOV is observed average `total_sales` per transaction for each promotion type. This is descriptive, not causal.

kpi2 = aov_by_promotion(fact, promotions)
display(kpi2)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Demographic Churn Heatmap
# MAGIC Churn rate is calculated from the supplied customer `churned` field, split by state and loyalty-program membership.

kpi3 = churn_heatmap(customers)
display(kpi3)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Product Quality Index
# MAGIC The assignment defines this as identifying categories with the highest return rates. The report therefore uses the supplied `product_return_rate` rather than inventing a composite score.

kpi4 = product_quality(products)
display(kpi4)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Store Traffic by Hour — Transaction Proxy
# MAGIC The supplied data has transaction activity, not visitor/footfall counts. Transaction count is therefore the observable traffic proxy.

kpi5 = store_traffic_proxy(fact)
display(kpi5)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Business interpretation / limitations
# MAGIC - Use the highest observed value in each KPI as a descriptive signal, not a causal claim.
# MAGIC - Actual store footfall is unavailable; transaction volume is the traffic proxy.
# MAGIC - Net Margin follows the assignment definition and is **not** a profit-after-cost calculation.
# MAGIC - Churn uses the supplied `churned` field.
