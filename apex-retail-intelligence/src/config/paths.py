"""Centralized project paths. Override with environment variables in Databricks."""
import os
from pathlib import Path

IS_DATABRICKS = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))

if IS_DATABRICKS:
    SOURCE_ROOT = os.environ.get("APEX_RETAIL_SOURCE_ROOT", "/Volumes/main/default/apex_retail/Datasets")
    DATA_ROOT = os.environ.get("APEX_RETAIL_DATA_ROOT", "/Volumes/main/default/apex_retail_data")
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    SOURCE_ROOT = os.environ.get("APEX_RETAIL_SOURCE_ROOT", str(PROJECT_ROOT.parent / "Datasets"))
    DATA_ROOT = os.environ.get("APEX_RETAIL_DATA_ROOT", str(PROJECT_ROOT / "data"))

SOURCE_ROOT = SOURCE_ROOT.replace("\\", "/").rstrip("/")
DATA_ROOT = DATA_ROOT.replace("\\", "/").rstrip("/")

INCOMING_HISTORICAL = f"{SOURCE_ROOT}/historical_data"
INCOMING_INCREMENTAL = f"{SOURCE_ROOT}/incremental_data"
AUDIT_LANDING = f"{SOURCE_ROOT}/audit_landing"
AUDIT_SILVER = f"{SOURCE_ROOT}/audit_silver"

RAW_DIR = f"{DATA_ROOT}/raw"
LANDING_DIR = f"{DATA_ROOT}/landing"
BRONZE_DIR = f"{DATA_ROOT}/bronze"
SILVER_DIR = f"{DATA_ROOT}/silver"
GOLD_DIR = f"{DATA_ROOT}/gold"

CATALOG_NAME = os.environ.get("APEX_RETAIL_CATALOG", "apex_retail_intelligence")
GOLD_SCHEMA = os.environ.get("APEX_RETAIL_GOLD_SCHEMA", "GOLD_tables")
