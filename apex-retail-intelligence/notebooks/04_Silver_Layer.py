# Databricks notebook source
# MAGIC %md
# MAGIC # 04_Silver_Layer
# MAGIC **Purpose**: cleanse Bronze data, reconcile the supplied Silver audits,
# MAGIC then apply SCD and immutable-ledger processing.

import sys
import os
from pathlib import Path
for _candidate in (os.environ.get("APEX_RETAIL_SRC_PATH"), os.path.join(os.getcwd(), "src"), os.path.abspath("../src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

from config.paths import IS_DATABRICKS, BRONZE_DIR, SILVER_DIR, AUDIT_SILVER
from quality.dq_rules import clean_customer, clean_product, clean_sales
from silver.scd_logic import process_silver_customer, process_silver_product
from silver.sales_logic import process_silver_sales
from audit.reconciliation import reconcile_row_count
from pyspark.sql.functions import col, lower

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("SilverLayer")


PROCESSORS = {
    "customer": (clean_customer, process_silver_customer),
    "product": (clean_product, process_silver_product),
    "sales": (clean_sales, process_silver_sales),
}


def audit_name(entity, load_type):
    if load_type == "historical":
        return f"{entity}_silver_audit.csv", f"{entity}_historical"
    return f"{entity}_incrementalaudit_silver.csv", f"{entity}_new"


recon_results = []
for entity, (cleaner, processor) in PROCESSORS.items():
    bronze = spark.read.format("delta").load(f"{BRONZE_DIR}/{entity}")
    for load_type in ("historical", "incremental"):
        cleaned = cleaner(bronze.filter(col("load_type") == load_type))
        audit_file, audit_table = audit_name(entity, load_type)
        recon, passed = reconcile_row_count(spark, cleaned, f"{AUDIT_SILVER}/{audit_file}", audit_table)
        if recon is not None:
            recon_results.append(recon)
        if not passed:
            raise RuntimeError(f"MANDATORY SILVER AUDIT FAILED for {entity}_{load_type}")

        # The supplied incremental customer extract includes source SCD history.
        # Reconcile every cleaned row, but apply only its current source version.
        updates = cleaned
        if entity == "customer" and load_type == "incremental" and "is_current" in cleaned.columns:
            updates = cleaned.filter(lower(col("is_current").cast("string")) == "true")
            # These are source-system SCD fields, not Silver's generated SCD2
            # fields; retaining them would cause a schema mismatch on append.
            updates = updates.drop("surrogate_key", "version", "effective_start_date", "effective_end_date", "is_current")
        processor(spark, updates, f"{SILVER_DIR}/{entity}")

if recon_results:
    reconciliation = recon_results[0]
    for item in recon_results[1:]:
        reconciliation = reconciliation.unionByName(item)
    reconciliation.show(truncate=False)
    if IS_DATABRICKS:
        display(reconciliation)

print("Silver processing and audit reconciliation complete.")
