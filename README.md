🎧 Chinook Data Pipeline — Azure + Databricks

📌 Overview

This project implements a robust end-to-end data pipeline using the Medallion Architecture (Raw → Bronze → Silver → Gold) on the Chinook dataset.

The pipeline is designed to ensure:

✅ Data reliability

✅ Strong data quality enforcement

✅ Scalable transformation workflows

✅ Analytics-ready outputs

🧱 Architecture

Azure SQL → Raw (Parquet) → Bronze (Delta) → Silver (Cleaned) → Gold (Star Schema)

🔹 Layers Explained

**Raw Layer**

Extracted from Azure SQL Chinook database

Stored as Parquet files

No transformations applied


**Bronze Layer**

Exact copy of raw data

Stored in Delta format

Ensures traceability


**Silver Layer**

Data validation and cleaning layer

Applies data quality rules

Handles bad data using quarantine strategy


**Gold Layer**

Business-level data model

Star schema with fact & dimension tables

Optimized for analytics


**⚙️ Tech Stack**

Azure SQL Database – Source system

Databricks (PySpark) – Data processing

Delta Lake – Storage format

Parquet – Raw data storage

YAML – Pipeline orchestration


**🔄 Pipeline Workflow :**
Extract from Source
Raw → Bronze
Bronze → Silver
Silver → Gold

Pipeline is orchestrated using Databricks Jobs with task dependencies.

🧪 Data Quality Framework (DQX)

The Silver layer implements a custom Data Quality Framework:

✔️ Validation Checks
Null checks

Duplicate detection

Range validation (e.g., UnitPrice > 0)

Schema consistency

**🚨 Bad Data Handling**

**Instead of deleting bad data:**

❌ Failed records are not discarded

📦 Stored in quarantine tables

🏷️ Tagged with failure reasons

**💡 This ensures:**

Auditability

Debugging capability

Future reprocessing


**🧹 Data Cleaning**

Applied only to valid records:

Trim whitespace

Handle null values (COALESCE)

Standardize formats (e.g., lowercase emails)

Type casting (dates, numeric fields)

🏆 Data Modeling (Gold Layer)

**⭐ Dimension Tables**

dim_date

dim_artist

dim_album

dim_genre

dim_media_type

dim_employee

dim_customer (SCD Type 2)

**📊 Fact Tables**

fact_sales

fact_sales_customer_agg

🔁 Slowly Changing Dimension (SCD Type 2)

**Implemented for Customer Dimension:**

Tracks historical changes

Maintains active & expired records

Uses effective date ranges
