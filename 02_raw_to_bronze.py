# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 — Raw to Bronze
# MAGIC Reads the latest Parquet snapshot from the Raw Volume for each table
# MAGIC and writes it as a Delta table in the Bronze schema.
# MAGIC
# MAGIC **Rules:**
# MAGIC - Source path comes from the child metrics table (file_location) — never hardcoded
# MAGIC - Write mode is OVERWRITE — Bronze always holds today's snapshot only
# MAGIC - Zero transformations — exact copy of Raw in Delta format

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "workspace")
dbutils.widgets.text("schema_name",   "pipeline_control")
dbutils.widgets.text("bronze_schema", "bronze")

# COMMAND ----------

catalog_name  = dbutils.widgets.get("catalog_name")
schema_name   = dbutils.widgets.get("schema_name")
bronze_schema = dbutils.widgets.get("bronze_schema")

print(f"catalog_name  : {catalog_name}")
print(f"schema_name   : {schema_name}")
print(f"bronze_schema : {bronze_schema}")

# COMMAND ----------
# MAGIC %md ## Read latest file_location for each table from child metrics table
# MAGIC
# MAGIC We pick the most recent SUCCESS row per table — this gives us
# MAGIC exactly the Parquet file NB01 just wrote.

# COMMAND ----------

latest_files_df = spark.sql(f"""
    SELECT table_name, file_location
    FROM (
        SELECT table_name,
               file_location,
               ROW_NUMBER() OVER (
                   PARTITION BY table_name
                   ORDER BY execution_time DESC
               ) AS rn
        FROM {catalog_name}.{schema_name}.pipeline_metadata_child
        WHERE status = 'SUCCESS'
    )
    WHERE rn = 1
    ORDER BY table_name
""")

print(f"Tables to load into Bronze: {latest_files_df.count()}")
latest_files_df.show(truncate=False)

# COMMAND ----------
# MAGIC %md ## Read each Parquet file and write to Bronze Delta table

# COMMAND ----------

import traceback
from datetime import datetime

tables = latest_files_df.collect()
summary = []

for row in tables:
    table_name    = row["table_name"]
    file_location = row["file_location"]
    bronze_table  = f"{catalog_name}.{bronze_schema}.{table_name.lower()}"

    print(f"\n{'='*60}")
    print(f"Table  : {table_name}")
    print(f"Source : {file_location}")
    print(f"Target : {bronze_table}")
    print(f"{'='*60}")

    try:
        # ── 1. Read Parquet from Raw Volume ───────────────────────
        raw_df    = spark.read.parquet(file_location)
        raw_count = raw_df.count()
        print(f"  Raw rows read : {raw_count}")

        # ── 2. Write to Bronze as Delta — overwrite ───────────────
        # Bronze = today's snapshot only. Overwrite replaces previous load.
        (raw_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(bronze_table)
        )

        # ── 3. Validate ───────────────────────────────────────────
        bronze_count = spark.read.table(bronze_table).count()
        print(f"  Bronze rows   : {bronze_count}")

        if raw_count == bronze_count:
            print(f"  Validation    : PASSED")
            summary.append({"table": table_name, "rows": bronze_count, "status": "SUCCESS"})
        else:
            print(f"  Validation    : FAILED — mismatch raw={raw_count} bronze={bronze_count}")
            summary.append({"table": table_name, "rows": bronze_count, "status": "FAILED"})

    except Exception as e:
        print(f"  ERROR: {str(e)}")
        traceback.print_exc()
        summary.append({"table": table_name, "rows": 0, "status": "FAILED"})

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("RAW → BRONZE SUMMARY")
print("="*60)
print(f"  {'Table':<20} {'Rows':>8}   Status")
print(f"  {'─'*20} {'─'*8}   {'─'*7}")
for s in summary:
    icon = "OK  " if s["status"] == "SUCCESS" else "FAIL"
    print(f"  [{icon}]  {s['table']:<20} {s['rows']:>8}")

failed = [s for s in summary if s["status"] == "FAILED"]
print(f"\n  Total: {len(summary)}  |  Success: {len(summary)-len(failed)}  |  Failed: {len(failed)}")
print("="*60)

if failed:
    print(f"\n  Failed tables: {[s['table'] for s in failed]}")
    dbutils.notebook.exit("FAILED")
else:
    print("\n  All tables loaded into Bronze successfully.")
    dbutils.notebook.exit("SUCCESS")
