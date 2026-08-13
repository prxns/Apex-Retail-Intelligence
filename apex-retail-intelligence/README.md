# Apex Retail Intelligence

> Enterprise-grade batch retail data engineering pipeline implemented with **Databricks, PySpark, Delta Lake, Unity Catalog concepts, and a strict Medallion Architecture**.

[![Databricks](https://img.shields.io/badge/Platform-Databricks-orange?logo=databricks)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/Engine-PySpark-blue?logo=apachespark)](https://spark.apache.org/docs/latest/api/python/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-0F766E)](https://delta.io/)
[![Python](https://img.shields.io/badge/Language-Python-3776AB?logo=python&logoColor=white)](https://www.python.org/)

## 1. Project Overview

**Apex Retail Intelligence** is a batch data engineering pipeline developed as a major internship project. The solution ingests historical and incremental retail transaction data, performs validation and quality controls, progressively transforms the data through Medallion layers, builds a business-ready Gold Star Schema, and produces five mandatory retail KPIs directly in Databricks.

The project is designed around an enterprise-style separation of concerns:

```text
                 SUPPLIED CSV + AUDIT FILES
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Raw Ingestion     │
                 │ Notebook 01         │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ Landing Conversion  │
                 │ Notebook 02         │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ Bronze Layer        │
                 │ Notebook 03         │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ Silver Layer        │
                 │ Notebook 04         │
                 │ DQ + SCD Type 2     │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ Gold Star Schema    │
                 │ Notebook 05         │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ KPI Reporting       │
                 │ Notebook 06         │
                 └─────────────────────┘
```

The implementation keeps source data outside the public Git repository. The supplied CSVs and audit files are expected to be uploaded into a Databricks Unity Catalog Volume before execution.

---

## 2. Project Objectives

The pipeline was built to satisfy the following engineering and analytics objectives:

- Ingest high-volume historical and incremental retail datasets in batch mode.
- Preserve source fidelity while creating reproducible downstream layers.
- Implement a strict **Raw → Landing → Bronze → Silver → Gold** Medallion flow.
- Apply data quality rules and explicit row-count reconciliation checks.
- Implement customer history management using **SCD Type 2** logic.
- Produce a reusable **Gold Star Schema** for BI-style analytical consumption.
- Calculate five mandatory retail KPIs directly from the Gold layer.
- Maintain auditability across key pipeline stages.
- Keep the code modular enough for local development, testing, and repository-based review.

---

## 3. Technology Stack

| Component | Technology |
|---|---|
| Cloud data platform | Databricks |
| Development environment used for execution | Databricks Free Edition |
| Compute model | Serverless compute |
| Processing engine | Apache Spark / PySpark |
| Storage format | Delta Lake |
| Data governance model | Unity Catalog / Unity Catalog Volumes |
| Programming language | Python |
| Version control | Git + GitHub |
| Documentation | Markdown |
| Analytics output | Databricks notebook results |

Databricks Free Edition currently provides serverless compute only and has a number of feature and API limitations compared with the full Databricks platform. citeturn557839search0turn557839search1

---

## 4. Architecture

### 4.1 Medallion Architecture

The solution follows a strict layered pipeline:

### Raw

The Raw stage ingests historical and incremental CSV files with minimal transformation. Source files are preserved in a structured Raw output so that downstream stages operate from a reproducible input.

### Landing

The Landing stage converts Raw outputs into Delta-backed datasets and performs the first reconciliation checks against supplied audit information.

### Bronze

The Bronze stage represents the standardized, persistent ingestion layer. Data is structured for downstream processing without prematurely applying business-specific analytical transformations.

### Silver

The Silver layer is the main data-quality and business-standardization layer. It performs:

- schema-aware cleansing,
- null and validity handling,
- duplicate handling,
- audit reconciliation,
- SCD Type 2 customer processing,
- product and sales transformation logic.

### Gold

The Gold layer converts the cleaned Silver datasets into a dimensional model suitable for analytical consumption.

### KPI Reporting

The final notebook calculates the required business KPIs directly against Gold datasets and renders the results in Databricks. No external BI platform is required for the assignment implementation.

---

## 5. Gold Star Schema

The final analytical model contains:

```text
                  ┌───────────────────┐
                  │   dim_customer    │
                  └─────────┬─────────┘
                            │
                            │ customer_sk
                            ▼
┌──────────────────┐  ┌───────────────────┐  ┌──────────────────┐
│  dim_product     │  │    fact_sales      │  │ dim_promotion    │
└────────┬─────────┘  └─────────┬──────────┘  └────────┬─────────┘
         │                       │                      │
         │ product_sk            │ promotion_sk        │
         └───────────────────────┼──────────────────────┘
                                 │
                                 │ date_sk
                                 ▼
                        ┌──────────────────┐
                        │    dim_date      │
                        └──────────────────┘
```

### Dimensions

- `dim_customer`
- `dim_product`
- `dim_promotion`
- `dim_date`

### Fact

- `fact_sales`

The model uses surrogate keys where appropriate and uses a defined **Unknown member (`SK = 0`)** approach to maintain referential integrity when a dimension lookup cannot be resolved.

---

## 6. Data Sources

The project uses supplied internship datasets containing:

- historical customer data,
- historical product data,
- historical sales data,
- incremental customer data,
- incremental product data,
- incremental sales data,
- Landing audit files,
- Silver audit files.

The source datasets are intentionally **not committed to the public Git repository**. They are operational inputs and should be uploaded directly to the Databricks Volume before execution.

### Expected Volume Layout

```text
/Volumes/<catalog>/<schema>/<volume>/
├── historical_data/
│   ├── customer/
│   │   └── customer_historical.csv
│   ├── product/
│   │   └── product_historical.csv
│   └── sales/
│       └── sales_historical.csv
│
├── incremental_data/
│   ├── customer/
│   │   └── customer_incremental.csv
│   ├── product/
│   │   └── product_incremental.csv
│   └── sales/
│       └── sales_incremental.csv
│
├── audit_landing/
└── audit_silver/
```

Unity Catalog Volume file paths use the `/Volumes/<catalog>/<schema>/<volume>/...` convention. citeturn557839search6

---

## 7. Repository Structure

```text
apex-retail-intelligence/
│
├── notebooks/
│   ├── 01_Raw_Ingestion.py
│   ├── 02_Landing_Conversion.py
│   ├── 03_Bronze_Layer.py
│   ├── 04_Silver_Layer.py
│   ├── 05_Gold_Layer.py
│   └── 06_KPI_Reporting.py
│
├── src/
│   ├── audit/
│   ├── config/
│   ├── gold/
│   │   ├── dimensions.py
│   │   └── kpis.py
│   ├── quality/
│   └── silver/
│       ├── sales_logic.py
│       └── scd_logic.py
│
├── docs/
│   ├── execution.md
│   └── images/
│
├── tests/
│
├── .gitignore
├── config.env.example
├── requirements.txt
└── README.md
```

Empty placeholder packages that did not contain implementation code were removed from the final repository to keep the structure intentional and maintainable.

---

## 8. Notebook Execution Order

The notebooks are designed to run in the following order:

```text
01_Raw_Ingestion
        ↓
02_Landing_Conversion
        ↓
03_Bronze_Layer
        ↓
04_Silver_Layer
        ↓
05_Gold_Layer
        ↓
06_KPI_Reporting
```

Running later stages before their required inputs exist will result in missing-path or missing-table errors.

---

## 9. Configuration

The pipeline uses environment-driven configuration rather than embedding the supplied dataset contents in source control.

Primary configuration concepts include:

```text
APEX_RETAIL_SOURCE_ROOT
APEX_RETAIL_DATA_ROOT
APEX_RETAIL_CATALOG
APEX_RETAIL_GOLD_SCHEMA
```

A typical Free Edition source root used during development was conceptually equivalent to:

```text
/Volumes/workspace/apex_retail/source_data
```

and the pipeline output root was maintained under the project Volume rather than the local Git working directory.

### Local / Repository Usage

Do not commit workspace-specific credentials, `.env` files, tokens, or private storage configuration. Use `config.env.example` as the documented configuration template.

---

## 10. Data Quality and Audit Controls

Auditability is a first-class component of the project rather than an afterthought.

The pipeline uses explicit reconciliation checks for key stages and datasets. The implemented workflow validates expected row counts against supplied audit information and stops processing when mandatory reconciliation conditions fail.

The Silver layer additionally performs data-quality transformations before downstream SCD and analytical processing.

Examples of validation behavior implemented during development include:

- historical vs. audit row-count reconciliation,
- incremental vs. audit row-count reconciliation,
- cleaned-row validation,
- SCD customer assertions,
- referential integrity checks in Gold,
- null surrogate-key protection through Unknown-member handling.

The final execution evidence is documented separately in [`docs/execution.md`](docs/execution.md).

---

## 11. SCD Type 2 Implementation

Customer history is handled as an SCD Type 2 flow in the Silver layer.

The design maintains historical and current customer states using fields such as:

- surrogate key,
- version / history metadata,
- effective start date,
- effective end date,
- current-state indicator.

The Gold fact-building logic also performs a point-in-time customer lookup so that transactions can be resolved against the appropriate effective customer version.

---

## 12. Mandatory KPI Reporting

The final notebook calculates the five mandatory KPIs defined by the internship assignment.

### 12.1 Net Margin by Region

**Definition used:** total gross revenue minus `discount_applied`, grouped by store region/location.

Implementation:

```python
kpi1 = net_margin_by_region(fact)
display(kpi1)
```

### 12.2 Average Order Value by Promotion

**Definition used:** observed average `total_sales` per transaction for each promotion type.

Implementation:

```python
kpi2 = aov_by_promotion(fact, promotions)
display(kpi2)
```

The final implementation groups directly on the `promotion_type` already present in the Gold fact because the fact output already contains that analytical attribute.

### 12.3 Demographic Churn Heatmap

**Definition used:** churn rate derived from the supplied `churned` field, split by customer state and loyalty-program membership.

Implementation:

```python
kpi3 = churn_heatmap(customers)
display(kpi3)
```

### 12.4 Product Quality Index

The assignment defines this KPI in terms of identifying categories with the highest return rates. The implementation therefore uses the supplied `product_return_rate` field rather than inventing an unsupported composite score.

Implementation:

```python
kpi4 = product_quality(products)
display(kpi4)
```

### 12.5 Store Traffic by Hour — Transaction Proxy

The supplied data contains transaction activity rather than physical visitor/footfall counts. Transaction count is therefore used as the observable store-traffic proxy.

Implementation:

```python
kpi5 = store_traffic_proxy(fact)
display(kpi5)
```

### KPI Interpretation

The KPI report is descriptive. Observed highs/lows should not automatically be interpreted as causal relationships. The project intentionally documents limitations instead of manufacturing unsupported business conclusions.

---

## 13. Databricks Free Edition Adaptations

The implementation was completed in **Databricks Free Edition** because the initial Azure-based execution path introduced unnecessary workspace and compute provisioning friction for the project.

Databricks Free Edition currently provides serverless compute only and has documented limitations compared with full Databricks deployments. Serverless execution uses Spark Connect and does not support Spark RDD APIs. citeturn557839search0turn557839search1

The following adaptations were therefore made during implementation.

### 13.1 Serverless / Spark Connect Compatibility

Legacy RDD-based checks were removed from execution-critical paths. For example, logic equivalent to:

```python
some_dataframe.rdd.isEmpty()
```

was replaced with a serverless-compatible DataFrame action such as:

```python
some_dataframe.limit(1).count() > 0
```

This was necessary because Spark RDD APIs are not supported on Databricks serverless compute. citeturn557839search1

### 13.2 Workspace Import Path Handling

The repository is structured as a Python source tree, while Databricks Workspace notebooks execute separately from the local Git working directory. The notebooks therefore explicitly add the project's `src/` directory to `sys.path` before importing modules such as:

```python
from config.paths import ...
from gold.kpis import ...
from silver.scd_logic import ...
```

This keeps the modular repository structure intact while allowing the Workspace copies of the notebooks to import the project packages.

### 13.3 Volume-Based Source Data

The project uses Unity Catalog Volumes for supplied CSV inputs rather than committing source data into Git. This is consistent with Databricks' recommended Volume-based file access model for governed data. citeturn557839search6

### 13.4 Gold Table Registration Limitation

The Gold transformation successfully writes the five Delta outputs:

```text
GOLD_DIR/
├── dim_customer/
├── dim_product/
├── dim_promotion/
├── dim_date/
└── fact_sales/
```

During execution in Free Edition, the optional final step that attempted to create Unity Catalog tables using a filesystem `LOCATION` over the Volume path was rejected by the environment. The implementation therefore keeps **Gold Delta generation as the authoritative successful output** and skips the unsupported registration step in the Free Edition execution path.

This is an environment-specific adaptation, not a change to the underlying Star Schema design. In a full Unity Catalog deployment with the appropriate supported storage/table configuration, the Gold registration step can be enabled as originally intended.

Databricks documents that Volume files are accessed through `/Volumes/...` paths and that managed/external table registration has distinct Unity Catalog storage requirements. citeturn557839search2turn557839search6

### 13.5 No External BI Dependency

The assignment requires KPI output directly in Databricks. The project therefore renders the five KPI outputs directly inside the KPI notebook instead of introducing a Power BI or other external dashboard dependency.

---

## 14. Error Resolution Highlights

Several environment- and schema-specific issues were identified and resolved during development. The final repository reflects the corrected implementations rather than the temporary diagnostic work used during troubleshooting.

Key fixes included:

- corrected Databricks Volume paths,
- corrected project `src/` import resolution,
- standardized historical/incremental input directory layout,
- fixed malformed source-file path assumptions,
- preserved mandatory audit reconciliation behavior,
- resolved Silver customer audit count expectations without disabling the audit,
- handled missing source columns safely during SCD processing,
- removed serverless-incompatible RDD usage,
- corrected Gold date-dimension schema construction,
- fixed point-in-time customer join operator precedence,
- corrected Gold fact dimension alias resolution,
- handled the Free Edition Unity Catalog registration limitation explicitly,
- corrected KPI promotion logic to use the promotion information actually carried by the Gold fact,
- reloaded modified Python modules during iterative Databricks notebook development.

Temporary diagnostic cells used during troubleshooting are **not part of the intended final execution flow**.

---

## 15. Reproducibility Guide

### Step 1 — Prepare Databricks

1. Open a Unity Catalog-enabled Databricks workspace.
2. Create the required catalog/schema/Volume structure.
3. Upload the supplied CSV and audit files to the Volume using the documented layout.
4. Configure the project roots in the runtime environment.

For Free Edition specifically, compute is serverless-only. citeturn557839search0

### Step 2 — Upload / Open the Notebooks

Import the notebooks from `notebooks/` in the following order:

```text
01_Raw_Ingestion.py
02_Landing_Conversion.py
03_Bronze_Layer.py
04_Silver_Layer.py
05_Gold_Layer.py
06_KPI_Reporting.py
```

### Step 3 — Execute Sequentially

Run all cells in order. Validate the audit output before moving to the next stage.

### Step 4 — Validate Gold

Confirm that the following Delta outputs exist:

```text
fact_sales
dim_customer
dim_product
dim_promotion
dim_date
```

### Step 5 — Run KPI Reporting

Execute `06_KPI_Reporting.py` and verify all five KPI result sets.

### Step 6 — Capture Evidence

Capture screenshots only from actual Databricks execution results. Store them under `docs/images/` and reference them from `docs/execution.md`.

---

## 16. Documentation and Execution Evidence

The repository deliberately separates project documentation from execution evidence.

### Main README

This document provides the architectural, technical, implementation, and reproducibility overview.

### Execution Guide

See [`docs/execution.md`](docs/execution.md) for stage-by-stage Databricks evidence, including:

- Raw ingestion results,
- Landing audit outputs,
- Bronze execution,
- Silver reconciliation and SCD validation,
- Gold outputs,
- KPI results.

### Data Documentation

The supplied source data is excluded from Git. Its expected Volume structure is documented above so that the project can be reproduced without exposing the internship source files in the repository.

---

## 17. Security and Repository Hygiene

The repository intentionally excludes:

- supplied internship CSVs,
- audit data files,
- `.env` files,
- credentials,
- tokens,
- private cloud storage configuration,
- generated caches such as `__pycache__`.

Only configuration templates such as `config.env.example` should be committed.

Workspace-specific paths containing personal identifiers should be replaced with environment-driven values before public distribution.

---

## 18. Engineering Practices Demonstrated

This project demonstrates practical data-engineering patterns beyond simple notebook-based transformations:

- Medallion Architecture,
- modular PySpark code organization,
- Delta Lake persistence,
- batch ingestion,
- historical + incremental processing,
- schema-aware transformations,
- data quality enforcement,
- row-count reconciliation,
- SCD Type 2 handling,
- surrogate key management,
- referential integrity checks,
- star-schema modeling,
- KPI-oriented analytical transformations,
- serverless/Spark Connect compatibility,
- source-control-based development,
- execution evidence and reproducibility documentation.

---

## 19. Known Limitations

The following limitations are explicitly acknowledged rather than hidden:

1. **Source data is not stored in Git.** It must be supplied through the Databricks Volume before execution.
2. **Free Edition is not equivalent to a production Databricks deployment.** It is subject to serverless and usage limitations. citeturn557839search0
3. **Store footfall is not available in the supplied data.** Transaction volume is used as the traffic proxy.
4. **Net margin follows the assignment's definition** (gross revenue minus applied discount) and is not a complete accounting profit calculation.
5. **KPI results are descriptive, not causal.**
6. **Unity Catalog Gold registration using the rejected Volume `LOCATION` pattern was skipped in the Free Edition execution path.** The underlying Gold Delta outputs remain available and form the basis of KPI reporting.

---

## 20. Future Production Enhancements

A production deployment could extend this implementation with:

- Databricks Workflows / Jobs orchestration,
- scheduled incremental ingestion,
- Auto Loader or Structured Streaming for event-driven ingestion,
- formal Unity Catalog managed-table registration,
- centralized data-quality monitoring,
- expectations / automated quality alerts,
- job-level observability and lineage,
- environment-specific configuration management,
- CI/CD for Databricks assets,
- dedicated BI dashboards,
- partitioning and performance optimization at scale,
- automated regression tests for data contracts and KPI outputs.

---

## 21. Project Status

**Pipeline implementation:** Complete  
**Medallion layers:** Complete  
**Silver SCD Type 2 processing:** Complete  
**Gold Star Schema:** Complete  
**Mandatory KPI reporting:** Complete  
**Free Edition compatibility adaptations:** Complete  
**Execution evidence:** Documented separately  

---

## 22. Repository Notes

This repository contains the **implementation and documentation**, not the supplied internship source dataset.

The assignment materials remain the source of truth for the required schemas, audit behavior, SCD rules, Gold outputs, and KPI definitions. The repository documents the engineering decisions taken to implement those requirements in the available Databricks execution environment.

---

## 23. References

- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Databricks Serverless compute limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
- [Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/)
- [Unity Catalog Volume path rules](https://docs.databricks.com/gcp/en/volumes/paths)
- [Databricks best practices for DBFS and Unity Catalog](https://docs.databricks.com/aws/en/dbfs/unity-catalog)

---

## 24. Author

Pranshu Rawat

**Apex Retail Intelligence — Major Internship Project**

Developed as part of the **Celebal Excellence Internship** data engineering project work.
