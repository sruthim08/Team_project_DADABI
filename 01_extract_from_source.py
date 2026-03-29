# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Extract from Source
# MAGIC Reads from Azure SQL, writes Parquet to Raw Volume, logs to child metrics table.
# MAGIC NB02 is called only to log — the Parquet write happens here in NB01's session.

# COMMAND ----------

dbutils.widgets.text("catalog_name",   "workspace")
dbutils.widgets.text("schema_name",    "pipeline_control")
dbutils.widgets.text("base_path",      "/Volumes/workspace/raw_zone/chinook")
dbutils.widgets.text("source_catalog", "chinook_source")
dbutils.widgets.text("source_schema",  "chinook")

# COMMAND ----------

catalog_name   = dbutils.widgets.get("catalog_name")
schema_name    = dbutils.widgets.get("schema_name")
base_path      = dbutils.widgets.get("base_path")
source_catalog = dbutils.widgets.get("source_catalog")
source_schema  = dbutils.widgets.get("source_schema")

print(f"catalog_name   : {catalog_name}")
print(f"schema_name    : {schema_name}")
print(f"base_path      : {base_path}")
print(f"source_catalog : {source_catalog}")
print(f"source_schema  : {source_schema}")

# COMMAND ----------

# MAGIC %md ## Read parent metadata table

# COMMAND ----------

parent_metadata_df = spark.sql(f"""
    SELECT table_name, file_name, active_flag
    FROM   {catalog_name}.{schema_name}.pipeline_metadata_parent
    WHERE  active_flag = 'Y'
    ORDER BY table_name
""")

print(f"Tables to extract: {parent_metadata_df.count()}")
parent_metadata_df.show(truncate=False)

# COMMAND ----------

import traceback
import time
from datetime import datetime, date
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, TimestampType, DateType)
 
# ── Connection warmup ─────────────────────────────────────────────
# Runs a lightweight query against the federated catalog before the
# main loop. This forces the JDBC connection to fully initialise so
# the first real table (Album) does not hit a cold-start timeout.
 
MAX_RETRIES = 3
warmup_ok   = False
 
for attempt in range(1, MAX_RETRIES + 1):
    try:
        print(f"Connection warmup — attempt {attempt}/{MAX_RETRIES} ...")
        warmup_count = spark.sql(
            f"SELECT COUNT(*) AS c FROM {source_catalog}.{source_schema}.Album"
        ).collect()[0]["c"]
        print(f"Connection OK — Album row count: {warmup_count}")
        warmup_ok = True
        break
    except Exception as e:
        print(f"  Warmup attempt {attempt} failed: {str(e)}")
        if attempt < MAX_RETRIES:
            print(f"  Waiting 10 seconds before retry...")
            time.sleep(10)
 
if not warmup_ok:
    raise Exception(
        "Connection warmup failed after 3 attempts. "
        "Check the federated catalog connection and cluster status."
    )
 
print(f"\nProceeding to extract {len(parent_metadata_df.collect())} tables.\n")

# COMMAND ----------

# MAGIC %md ## Extract, write Parquet, log metrics

# COMMAND ----------

import traceback
from datetime import datetime, date
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, TimestampType, DateType)

log_schema = StructType([
    StructField("table_name",       StringType(),    False),
    StructField("execution_time",   TimestampType(), False),
    StructField("status",           StringType(),    False),
    StructField("source_row_count", LongType(),      False),
    StructField("target_row_count", LongType(),      False),
    StructField("file_location",    StringType(),    False),
    StructField("created_date",     DateType(),      False),
])

tables_to_process = parent_metadata_df.collect()
overall_status    = []

for row in tables_to_process:
    table_name     = row["table_name"]
    execution_time = datetime.now()
    status         = "FAILED"
    target_row_count = 0

    # Build dynamic file path
    ts            = execution_time.strftime("%Y%m%d_%H%M%S")
    year          = execution_time.strftime("%Y")
    month         = execution_time.strftime("%m")
    day           = execution_time.strftime("%d")
    file_location = f"{base_path}/{table_name}/{year}/{month}/{day}/{table_name}_{ts}.parquet"

    print(f"\n{'='*60}")
    print(f"Processing: {table_name}")
    print(f"Target    : {file_location}")
    print(f"{'='*60}")

    try:
        # ── 1. Extract from Azure SQL ─────────────────────────────
        source_ref = f"{source_catalog}.{source_schema}.{table_name}"
        source_df  = spark.read.table(source_ref)

        # ── 2. Count source rows ──────────────────────────────────
        source_row_count = source_df.count()
        print(f"  Source rows  : {source_row_count}")

        # ── 3. Write Parquet to Volume (same session — no temp view needed)
        source_df.write.mode("append").parquet(file_location)
        print(f"  Parquet written.")

        # ── 4. Read back and validate ────────────────────────────
        target_row_count = spark.read.parquet(file_location).count()
        print(f"  Written rows : {target_row_count}")

        if source_row_count == target_row_count:
            status = "SUCCESS"
            print(f"  Validation   : PASSED")
        else:
            status = "FAILED"
            print(f"  Validation   : FAILED — mismatch!")

    except Exception as e:
        source_row_count = 0
        print(f"  ERROR: {str(e)}")
        traceback.print_exc()

    # ── 5. Log to child metrics table ────────────────────────────
    try:
        log_row = [(table_name, execution_time, status,
                    int(source_row_count), int(target_row_count),
                    file_location, date.today())]

        log_df = spark.createDataFrame(log_row, schema=log_schema)
        (log_df.write.format("delta").mode("append")
             .saveAsTable(f"{catalog_name}.{schema_name}.pipeline_metadata_child"))

        print(f"  Metrics logged: {status}")
    except Exception as e:
        print(f"  ERROR logging metrics: {str(e)}")

    overall_status.append({
        "table"  : table_name,
        "status" : status,
        "rows"   : target_row_count,
        "file"   : file_location
    })

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("EXTRACTION SUMMARY")
print("="*60)
print(f"  {'Table':<20} {'Rows':>8}   Status")
print(f"  {'─'*20} {'─'*8}   {'─'*7}")
for s in overall_status:
    icon = "OK  " if s["status"] == "SUCCESS" else "FAIL"
    print(f"  [{icon}]  {s['table']:<20} {s['rows']:>8}")

failed = [s for s in overall_status if s["status"] == "FAILED"]
print(f"\n  Total: {len(overall_status)}  |  Success: {len(overall_status)-len(failed)}  |  Failed: {len(failed)}")
print("="*60)

if failed:
    print(f"\nFailed: {[s['table'] for s in failed]}")
    dbutils.notebook.exit("FAILED")
else:
    print("\nAll tables extracted successfully.")
    dbutils.notebook.exit("SUCCESS")
