# Databricks Execution Guide

1. Create or use a Unity Catalog-enabled Databricks workspace/cluster.
2. Create a writable Volume for the supplied internship files and upload **all load CSVs and all audit CSVs** using the layout documented in `datasets/README.md`.
3. Set `APEX_RETAIL_SOURCE_ROOT` to that Volume root.
4. Set `APEX_RETAIL_DATA_ROOT` to a writable Delta storage location/Volume.
5. Set `APEX_RETAIL_CATALOG` to the evaluator's catalog and keep `APEX_RETAIL_GOLD_SCHEMA=GOLD_tables` unless instructed otherwise.
6. Import the six files from `notebooks/` into Databricks in this exact order:
   `01_Raw_Ingestion` → `02_Landing_Conversion` → `03_Bronze_Layer` → `04_Silver_Layer` → `05_Gold_Layer` → `06_KPI_Reporting`.
7. Phase 2 must show six Landing audit PASS rows before continuing.
8. Phase 4 must show the supplied Silver audit PASS rows and the final SCD/duplicate assertions must pass.
9. Phase 5 must show the five Gold tables and Unity Catalog registration.
10. Phase 6 must render all five KPIs directly in the Databricks notebook.

Capture screenshots only after actual Databricks execution. Do not fabricate PASS states or KPI values in the repository.
