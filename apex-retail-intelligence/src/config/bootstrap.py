"""Notebook bootstrap helpers."""
import os
import sys
from pathlib import Path


def add_src_to_path():
    candidates = [
        os.environ.get("APEX_RETAIL_SRC_PATH"),
        str(Path.cwd() / "src"),
        str(Path(__file__).resolve().parents[2] / "src"),
    ]
    for candidate in candidates:
        if candidate and os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.insert(0, candidate)


def get_spark_for_notebook(name: str):
    from config.paths import IS_DATABRICKS
    if IS_DATABRICKS:
        return spark  # noqa: F821
    from config.runtime import get_spark
    return get_spark(name)
