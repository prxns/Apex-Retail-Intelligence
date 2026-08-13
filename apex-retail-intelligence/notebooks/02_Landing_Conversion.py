# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2 — Landing + Audit Reconciliation
# MAGIC Convert Raw CSV to Parquet and dynamically reconcile every load against the supplied audit CSV.

# COMMAND ----------
import os, sys
from pathlib import Path
for p in [os.environ.get("APEX_RETAIL_SRC_PATH"), str(Path.cwd() / "src"), str(Path(__file__).resolve().parents[1] / "src") if "__file__" in globals() else None]:
    if p and os.path.isdir(p) and p not in sys.path: sys.path.insert(0, p)
from config.paths import IS_DATABRICKS, RAW_DIR, LANDING_DIR, AUDIT_LANDING
from audit.reconciliation import reconcile_row_count, assert_reconciliation

if not IS_DATABRICKS:
    from config.runtime import get_spark
    spark = get_spark("ApexLanding")

# COMMAND ----------

def find_audit(entity, load_type):
    token = f"{entity}_historical_audit.csv" if load_type == "historical" else f"{entity}_incrementalaudit.csv"
    candidates = dbutils.fs.ls(AUDIT_LANDING) if IS_DATABRICKS else []
    if IS_DATABRICKS:
        for item in candidates:
            if item.name.lower() == token.lower(): return item.path
    else:
        p = Path(AUDIT_LANDING) / token
        if p.exists(): return str(p)
    raise FileNotFoundError(f"Audit file not found for {entity}_{load_type}: {token}")


results = []
for entity in ["customer", "product", "sales"]:
    for load_type in ["historical", "incremental"]:
        raw = f"{RAW_DIR}/{entity}/{load_type}"
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(raw)
        audit_name = f"{entity}_{load_type}"
        audit_path = find_audit(entity, load_type)
        recon, passed = reconcile_row_count(spark, df, audit_path, audit_name)
        results.append(recon)
        assert_reconciliation(passed, audit_name)
        df.write.mode("overwrite").parquet(f"{LANDING_DIR}/{entity}/{load_type}")

final_recon = results[0]
for r in results[1:]: final_recon = final_recon.unionByName(r)
final_recon.orderBy("dataset").show(truncate=False)
if IS_DATABRICKS: display(final_recon)
print("Phase 2 complete — all mandatory Landing audits PASS.")
