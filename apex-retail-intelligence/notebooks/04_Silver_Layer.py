# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4 — Silver: DQ, MERGE, SCD1/SCD2 and Immutable Sales Ledger
# MAGIC 
# MAGIC **Mandatory MERGE outcome explanation:**
# MAGIC - **Customer:** historical rows initialize the SCD2 target; incremental current records are compared by a deterministic attribute hash. Matching hashes are a no-op; changed keys close the current row and append a new current version; new keys are inserted.
# MAGIC - **Product:** historical rows initialize SCD1; incremental rows use Delta MERGE on `product_id`, updating matches in place and inserting new products.
# MAGIC - **Sales:** each batch is deduplicated by `transaction_id` with the latest ingested instance retained. Delta MERGE inserts only unseen transaction IDs, so reruns do not duplicate the immutable ledger.
# MAGIC - **Audits:** supplied Silver audit CSVs are read dynamically and must reconcile before the corresponding transformation continues.
# MAGIC - **Idempotency:** rerunning unchanged inputs produces no new customer SCD2 version, no new product row, and no new sales transaction.

# COMMAND ----------
import os, sys
from pathlib import Path
for p in [os.environ.get("APEX_RETAIL_SRC_PATH"), str(Path.cwd() / "src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None]:
    if p and os.path.isdir(p) and p not in sys.path: sys.path.insert(0, p)
from config.paths import IS_DATABRICKS, BRONZE_DIR, SILVER_DIR, AUDIT_SILVER
from quality.dq_rules import clean_customer, clean_product, clean_sales
from silver.scd_logic import initialize_customer_scd2, merge_customer_scd2, process_silver_product
from silver.sales_logic import process_silver_sales
from audit.reconciliation import reconcile_row_count, assert_reconciliation
from pyspark.sql.functions import col, lower

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("ApexSilver")

# COMMAND ----------

def audit_path(entity, load_type):
    name = f"{entity}_silver_audit.csv" if load_type == "historical" else f"{entity}_incrementalaudit_silver.csv"
    if IS_DATABRICKS:
        for item in dbutils.fs.ls(AUDIT_SILVER):
            if item.name.lower() == name.lower(): return item.path
    else:
        p = Path(AUDIT_SILVER) / name
        if p.exists(): return str(p)
    raise FileNotFoundError(name)


def audit_table(entity, load_type):
    return f"{entity}_historical" if load_type == "historical" else f"{entity}_new"


def load_clean(entity, load_type, cleaner):
    df = spark.read.format("delta").load(f"{BRONZE_DIR}/{entity}/{load_type}")
    cleaned = cleaner(df)
    recon, passed = reconcile_row_count(spark, cleaned, audit_path(entity, load_type), audit_table(entity, load_type))
    assert_reconciliation(passed, audit_table(entity, load_type))
    print(f"SILVER AUDIT PASS | {audit_table(entity, load_type)}")
    display(recon) if IS_DATABRICKS else recon.show()
    return cleaned

# COMMAND ----------
# Customer SCD2
customer_hist = load_clean("customer", "historical", clean_customer)
# Source-system SCD metadata is not copied to Silver. Historical customer state is the initial version.
customer_hist = customer_hist.drop("surrogate_key", "version", "effective_start_date", "effective_end_date", "is_current")
initialize_customer_scd2(spark, customer_hist, f"{SILVER_DIR}/customer")

customer_inc_all = load_clean("customer", "incremental", clean_customer)
# The supplied incremental file contains prior and current source versions. Only current source states
# become Silver change events; source version/SK/history fields are discarded after this selection.
customer_inc = customer_inc_all.filter(lower(col("is_current").cast("string")) == "true")
customer_inc = customer_inc.drop("surrogate_key", "version", "effective_end_date", "is_current")
merge_customer_scd2(spark, customer_inc, f"{SILVER_DIR}/customer")

# COMMAND ----------
# Product SCD1
product_hist = load_clean("product", "historical", clean_product)
product_hist = product_hist.drop("last_updated") if "last_updated" in product_hist.columns else product_hist
process_silver_product(spark, product_hist, f"{SILVER_DIR}/product")
product_inc = load_clean("product", "incremental", clean_product)
product_inc = product_inc.drop("last_updated") if "last_updated" in product_inc.columns else product_inc
process_silver_product(spark, product_inc, f"{SILVER_DIR}/product")

# COMMAND ----------
# Sales immutable ledger
sales_hist = load_clean("sales", "historical", clean_sales)
process_silver_sales(spark, sales_hist, f"{SILVER_DIR}/sales")
sales_inc = load_clean("sales", "incremental", clean_sales)
process_silver_sales(spark, sales_inc, f"{SILVER_DIR}/sales")

# COMMAND ----------
# Final Silver assertions
customer = spark.read.format("delta").load(f"{SILVER_DIR}/customer")
assert customer.filter(col("is_current") == True).groupBy("customer_id").count().filter(col("count") != 1).limit(1).count() == 0
assert customer.filter(col("customer_sk").isNull()).limit(1).count() == 0
product = spark.read.format("delta").load(f"{SILVER_DIR}/product")
assert product.groupBy("product_id").count().filter(col("count") != 1).limit(1).count() == 0
sales = spark.read.format("delta").load(f"{SILVER_DIR}/sales")
assert sales.groupBy("transaction_id").count().filter(col("count") != 1).limit(1).count() == 0
print("Phase 4 complete — Silver invariants PASS.")
