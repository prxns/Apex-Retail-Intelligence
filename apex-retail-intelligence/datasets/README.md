# Supplied Internship Datasets

The original Celebal source CSVs and audit files are intentionally kept **out of the final public repository**. Upload them to a Databricks Volume before execution.

Expected Volume layout:

```text
/Volumes/<catalog>/<schema>/<volume>/
├── historical_data/
│   ├── customer/customer_historical.csv
│   ├── product/product_historical.csv
│   └── sales/sales_historical.csv
├── incremental_data/
│   ├── customer_incremental/customer_incremental.csv
│   ├── product_incremental/product_incremental.csv
│   └── sales_incremental/sales_incremental.csv
├── audit_landing/
└── audit_silver/
```

Set `APEX_RETAIL_SOURCE_ROOT` to this Volume root before running the notebooks.

The supplied assignment PDF remains the source of truth for the required schemas, audit behavior, SCD rules, Gold tables, and KPIs.
