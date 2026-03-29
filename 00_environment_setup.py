# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Environment Setup
# MAGIC
# MAGIC Run this notebook **once** before anything else.
# MAGIC It creates every schema, volume, and metadata table the pipeline needs.
# MAGIC
# MAGIC ### What this does
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Creates the `raw_zone` schema and `chinook` Volume |
# MAGIC | 2 | Creates `bronze`, `silver`, `gold` schemas |
# MAGIC | 3 | Creates the `pipeline_control` schema for metadata tables |
# MAGIC | 4 | Creates all 4 metadata/control tables |
# MAGIC | 5 | Seeds the parent metadata with all 11 Chinook tables |
# MAGIC | 6 | Verifies everything was created correctly |
# MAGIC
# MAGIC ### Before running
# MAGIC - Fill in the `catalog_name` widget with your Unity Catalog name
# MAGIC - Make sure you have **CREATE SCHEMA** and **CREATE VOLUME** privileges on that catalog

# COMMAND ----------

# MAGIC %md ## Widget — set your catalog name here

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")   # e.g.  workspace

catalog_name = dbutils.widgets.get("catalog_name")

if not catalog_name:
    raise ValueError("catalog_name widget is empty. Set it before running.")

print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1 — Raw Zone: schema + Volume

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a — Create raw_zone schema
# MAGIC This lives under the **default workspace catalog** as the professor specified.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.raw_zone")
print(f"Schema ready: {catalog_name}.raw_zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b — Create the chinook Volume inside raw_zone
# MAGIC Raw Parquet files will be stored here.
# MAGIC Path will be: `/Volumes/{catalog_name}/raw_zone/chinook/`

# COMMAND ----------

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.raw_zone.chinook
    COMMENT 'Raw zone Volume for Chinook pipeline. Stores immutable Parquet snapshots.'
""")

print(f"Volume ready: /Volumes/{catalog_name}/raw_zone/chinook/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1c — Verify Volume is accessible

# COMMAND ----------

volume_path = f"/Volumes/{catalog_name}/raw_zone/chinook"
dbutils.fs.ls(volume_path)   # will error if Volume not created correctly
print(f"Volume accessible at: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2 — Bronze / Silver / Gold schemas

# COMMAND ----------

for layer in ["bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.{layer}
        COMMENT '{layer.capitalize()} layer for Chinook medallion pipeline'
    """)
    print(f"Schema ready: {catalog_name}.{layer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3 — Pipeline control schema + metadata tables

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.pipeline_control
    COMMENT 'Metadata and control tables for the Chinook pipeline'
""")
print(f"Schema ready: {catalog_name}.pipeline_control")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a — Parent metadata table
# MAGIC One row per source table. Controls what the pipeline extracts.
# MAGIC `active_flag = Y` means include; `N` means skip — no code change needed.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.pipeline_control.pipeline_metadata_parent (
        table_name      STRING  NOT NULL  COMMENT 'Source table name e.g. Album',
        file_name       STRING  NOT NULL  COMMENT 'Source file name e.g. album.csv',
        active_flag     STRING  NOT NULL  COMMENT 'Y = include in pipeline run, N = skip',
        created_date    DATE    NOT NULL  COMMENT 'Row creation date',
        modified_date   DATE    NOT NULL  COMMENT 'Last modification date'
    )
    USING DELTA
    COMMENT 'Parent metadata table - drives the extraction pipeline. One row per source table.'
""")
print("Table ready: pipeline_metadata_parent")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b — Seed parent metadata with all 11 Chinook tables

# COMMAND ----------

# Only insert if table is empty (safe to re-run)
existing_count = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {catalog_name}.pipeline_control.pipeline_metadata_parent
""").collect()[0]["cnt"]

if existing_count == 0:
    spark.sql(f"""
        INSERT INTO {catalog_name}.pipeline_control.pipeline_metadata_parent VALUES
        ('Album',         'album.csv',         'Y', current_date(), current_date()),
        ('Artist',        'artist.csv',        'Y', current_date(), current_date()),
        ('Customer',      'customer.csv',      'Y', current_date(), current_date()),
        ('Employee',      'employee.csv',      'Y', current_date(), current_date()),
        ('Genre',         'genre.csv',         'Y', current_date(), current_date()),
        ('Invoice',       'invoice.csv',       'Y', current_date(), current_date()),
        ('InvoiceLine',   'invoiceline.csv',   'Y', current_date(), current_date()),
        ('MediaType',     'mediatype.csv',     'Y', current_date(), current_date()),
        ('Playlist',      'playlist.csv',      'Y', current_date(), current_date()),
        ('PlaylistTrack', 'playlisttrack.csv', 'Y', current_date(), current_date()),
        ('Track',         'track.csv',         'Y', current_date(), current_date())
    """)
    print("Seeded 11 tables into pipeline_metadata_parent")
else:
    print(f"Table already has {existing_count} rows — skipping seed insert")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c — Child execution metrics table
# MAGIC One row per table per pipeline run. The audit trail.
# MAGIC Pipeline is validated when `source_row_count == target_row_count`.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.pipeline_control.pipeline_metadata_child (
        table_name          STRING      NOT NULL  COMMENT 'Source table that was processed',
        execution_time      TIMESTAMP   NOT NULL  COMMENT 'Timestamp when extraction ran',
        status              STRING      NOT NULL  COMMENT 'SUCCESS or FAILED',
        source_row_count    LONG        NOT NULL  COMMENT 'Row count from Azure SQL source',
        target_row_count    LONG        NOT NULL  COMMENT 'Row count written to Raw Volume',
        file_location       STRING      NOT NULL  COMMENT 'Full dynamic path to Parquet file',
        created_date        DATE        NOT NULL  COMMENT 'Date this log row was created'
    )
    USING DELTA
    COMMENT 'Child execution metrics - audit trail. Validated when source_row_count = target_row_count.'
""")
print("Table ready: pipeline_metadata_child")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3d — DQX execution log table

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.pipeline_control.dqx_execution_log (
        table_name          STRING      NOT NULL  COMMENT 'Table that was validated',
        execution_time      TIMESTAMP   NOT NULL  COMMENT 'When DQX ran',
        total_records       LONG        NOT NULL  COMMENT 'Total records processed',
        passed_records      LONG        NOT NULL  COMMENT 'Records that passed all rules',
        failed_records      LONG        NOT NULL  COMMENT 'Records that failed at least one rule',
        rules_applied       STRING                COMMENT 'JSON list of rules that were applied',
        created_date        DATE        NOT NULL  COMMENT 'Date this log row was created'
    )
    USING DELTA
    COMMENT 'DQX execution log - tracks validation results per table per run.'
""")
print("Table ready: dqx_execution_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3e — DQX quarantine table
# MAGIC Records that fail quality rules land here instead of Silver.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.pipeline_control.dqx_quarantine (
        table_name          STRING      NOT NULL  COMMENT 'Source table of the failed record',
        execution_time      TIMESTAMP   NOT NULL  COMMENT 'When the validation ran',
        failed_rule         STRING      NOT NULL  COMMENT 'Name of the rule that was violated',
        failed_column       STRING                COMMENT 'Column that triggered the failure',
        record_data         STRING      NOT NULL  COMMENT 'Full failed row serialised as JSON',
        created_date        DATE        NOT NULL  COMMENT 'Date this quarantine row was created'
    )
    USING DELTA
    COMMENT 'Quarantine table - failed DQX records stored here for review and reprocessing.'
""")
print("Table ready: dqx_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4 — Verification: confirm everything exists

# COMMAND ----------

print("=" * 60)
print("ENVIRONMENT SETUP VERIFICATION")
print("=" * 60)

# Check schemas
schemas_to_check = ["raw_zone", "bronze", "silver", "gold", "pipeline_control"]
print("\nSchemas:")
for s in schemas_to_check:
    result = spark.sql(f"SHOW SCHEMAS IN {catalog_name} LIKE '{s}'").count()
    status = "OK" if result > 0 else "MISSING"
    print(f"  [{status}]  {catalog_name}.{s}")

# Check volume
print("\nVolume:")
try:
    dbutils.fs.ls(f"/Volumes/{catalog_name}/raw_zone/chinook")
    print(f"  [OK]   /Volumes/{catalog_name}/raw_zone/chinook")
except:
    print(f"  [MISSING]  /Volumes/{catalog_name}/raw_zone/chinook")

# Check tables
tables_to_check = [
    ("pipeline_control", "pipeline_metadata_parent"),
    ("pipeline_control", "pipeline_metadata_child"),
    ("pipeline_control", "dqx_execution_log"),
    ("pipeline_control", "dqx_quarantine"),
]
print("\nControl tables:")
for schema, table in tables_to_check:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) as c FROM {catalog_name}.{schema}.{table}").collect()[0]["c"]
        print(f"  [OK]   {catalog_name}.{schema}.{table}  ({cnt} rows)")
    except Exception as e:
        print(f"  [MISSING]  {catalog_name}.{schema}.{table}  — {e}")

# Show parent metadata contents
print("\nParent metadata table contents:")
spark.sql(f"""
    SELECT table_name, file_name, active_flag, created_date
    FROM   {catalog_name}.pipeline_control.pipeline_metadata_parent
    ORDER  BY table_name
""").show(20, truncate=False)

print("\nSetup complete. Proceed to Connection Manager configuration.")
