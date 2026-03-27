# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 — Bronze to Silver
# MAGIC Runs DQX profiling and quality validation on each Bronze table,
# MAGIC quarantines failed records, cleans passing records, writes to Silver.
# MAGIC
# MAGIC **Steps per table:**
# MAGIC 1. Read from Bronze Delta
# MAGIC 2. DQX profile — nulls, duplicates, type consistency, ranges
# MAGIC 3. Apply quality rules — split into passed / failed records
# MAGIC 4. Write failed records to quarantine table with reason
# MAGIC 5. Apply cleaning — TRIM, COALESCE, lowercase email, cast dates
# MAGIC 6. Write cleaned records to Silver Delta
# MAGIC 7. Log results to DQX execution log

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "workspace")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("control_schema","pipeline_control")

# COMMAND ----------

catalog_name   = dbutils.widgets.get("catalog_name")
bronze_schema  = dbutils.widgets.get("bronze_schema")
silver_schema  = dbutils.widgets.get("silver_schema")
control_schema = dbutils.widgets.get("control_schema")

print(f"catalog_name   : {catalog_name}")
print(f"bronze_schema  : {bronze_schema}")
print(f"silver_schema  : {silver_schema}")
print(f"control_schema : {control_schema}")

# COMMAND ----------
# MAGIC %md ## Helper — DQX profiling function
# MAGIC Analyses a DataFrame and prints a profile report.
# MAGIC Returns a dict of profile stats.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                                LongType, TimestampType, DateType)
from datetime import datetime, date
import traceback, json

def profile_dataframe(df, table_name):
    """Run data profiling on a Bronze DataFrame and print findings."""
    print(f"\n  ── DQX Profile: {table_name} ──")
    total = df.count()
    print(f"  Total rows     : {total}")

    # Null counts per column
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    cols_with_nulls = {k: v for k, v in null_counts.items() if v > 0}
    if cols_with_nulls:
        print(f"  Columns with nulls:")
        for col, cnt in cols_with_nulls.items():
            print(f"    {col:<30} {cnt:>6} nulls")
    else:
        print(f"  Null check     : No nulls found")

    # Duplicate check
    dup_count = total - df.distinct().count()
    print(f"  Duplicate rows : {dup_count}")

    return {"total": total, "nulls": cols_with_nulls, "duplicates": dup_count}


# COMMAND ----------
# MAGIC %md ## Quality rules per table
# MAGIC
# MAGIC Each rule is a Spark filter condition (string).
# MAGIC A record PASSES if ALL rules evaluate to True.
# MAGIC A record FAILS if ANY rule evaluates to False or NULL.

# COMMAND ----------

# Rules dict: table_name → list of (rule_name, column, condition_expression)
QUALITY_RULES = {
    "album": [
        ("not_null", "AlbumId", "AlbumId IS NOT NULL"),
        ("not_null", "Title",   "Title IS NOT NULL"),
        ("not_null", "ArtistId","ArtistId IS NOT NULL"),
    ],
    "artist": [
        ("not_null", "ArtistId", "ArtistId IS NOT NULL"),
    ],
    "customer": [
        ("not_null",   "CustomerId", "CustomerId IS NOT NULL"),
        ("not_null",   "FirstName",  "FirstName IS NOT NULL"),
        ("not_null",   "LastName",   "LastName IS NOT NULL"),
        ("not_null",   "Email",      "Email IS NOT NULL"),
        ("not_null",   "Country",    "Country IS NOT NULL"),
    ],
    "employee": [
        ("not_null", "EmployeeId", "EmployeeId IS NOT NULL"),
        ("not_null", "FirstName",  "FirstName IS NOT NULL"),
        ("not_null", "LastName",   "LastName IS NOT NULL"),
    ],
    "genre": [
        ("not_null", "GenreId", "GenreId IS NOT NULL"),
    ],
    "invoice": [
        ("not_null",   "InvoiceId",   "InvoiceId IS NOT NULL"),
        ("not_null",   "CustomerId",  "CustomerId IS NOT NULL"),
        ("not_null",   "InvoiceDate", "InvoiceDate IS NOT NULL"),
        ("valid_range","Total",       "Total > 0"),
    ],
    "invoiceline": [
        ("not_null",   "InvoiceLineId", "InvoiceLineId IS NOT NULL"),
        ("not_null",   "InvoiceId",     "InvoiceId IS NOT NULL"),
        ("not_null",   "TrackId",       "TrackId IS NOT NULL"),
        ("valid_range","UnitPrice",     "UnitPrice > 0"),
        ("valid_range","Quantity",      "Quantity > 0"),
    ],
    "mediatype": [
        ("not_null", "MediaTypeId", "MediaTypeId IS NOT NULL"),
    ],
    "playlist": [
        ("not_null", "PlaylistId", "PlaylistId IS NOT NULL"),
    ],
    "playlisttrack": [
        ("not_null", "PlaylistId", "PlaylistId IS NOT NULL"),
        ("not_null", "TrackId",    "TrackId IS NOT NULL"),
    ],
    "track": [
        ("not_null",   "TrackId",      "TrackId IS NOT NULL"),
        ("not_null",   "Name",         "Name IS NOT NULL"),
        ("not_null",   "MediaTypeId",  "MediaTypeId IS NOT NULL"),
        ("valid_range","Milliseconds", "Milliseconds > 0"),
        ("valid_range","UnitPrice",    "UnitPrice > 0"),
    ],
}


# COMMAND ----------
# MAGIC %md ## Cleaning transformations per table
# MAGIC
# MAGIC Applied only to records that PASS DQX rules.

# COMMAND ----------

from pyspark.sql import functions as F

def clean_dataframe(df, table_name):
    """Apply Silver-layer cleaning to a passed-records DataFrame."""

    if table_name == "album":
        return df.withColumn("Title", F.trim(F.col("Title")))

    elif table_name == "artist":
        return df.withColumn("Name", F.trim(F.coalesce(F.col("Name"), F.lit("Unknown"))))

    elif table_name == "customer":
        return (df
            .withColumn("FirstName",  F.trim(F.col("FirstName")))
            .withColumn("LastName",   F.trim(F.col("LastName")))
            .withColumn("Company",    F.trim(F.coalesce(F.col("Company"),    F.lit(""))))
            .withColumn("Address",    F.trim(F.coalesce(F.col("Address"),    F.lit(""))))
            .withColumn("City",       F.trim(F.coalesce(F.col("City"),       F.lit("Unknown"))))
            .withColumn("State",      F.trim(F.coalesce(F.col("State"),      F.lit("Unknown"))))
            .withColumn("Country",    F.trim(F.col("Country")))
            .withColumn("PostalCode", F.trim(F.coalesce(F.col("PostalCode"), F.lit(""))))
            .withColumn("Phone",      F.trim(F.coalesce(F.col("Phone"),      F.lit(""))))
            .withColumn("Fax",        F.trim(F.coalesce(F.col("Fax"),        F.lit(""))))
            .withColumn("Email",      F.lower(F.trim(F.col("Email"))))
        )

    elif table_name == "employee":
        return (df
            .withColumn("FirstName", F.trim(F.col("FirstName")))
            .withColumn("LastName",  F.trim(F.col("LastName")))
            .withColumn("Title",     F.trim(F.coalesce(F.col("Title"),   F.lit("Unknown"))))
            .withColumn("City",      F.trim(F.coalesce(F.col("City"),    F.lit("Unknown"))))
            .withColumn("State",     F.trim(F.coalesce(F.col("State"),   F.lit("Unknown"))))
            .withColumn("Country",   F.trim(F.coalesce(F.col("Country"), F.lit("Unknown"))))
            .withColumn("Email",     F.lower(F.trim(F.coalesce(F.col("Email"), F.lit("")))))
            .withColumn("Phone",     F.trim(F.coalesce(F.col("Phone"),   F.lit(""))))
            .withColumn("BirthDate", F.to_date(F.col("BirthDate")))
            .withColumn("HireDate",  F.to_date(F.col("HireDate")))
        )

    elif table_name == "genre":
        return df.withColumn("Name", F.trim(F.coalesce(F.col("Name"), F.lit("Unknown"))))

    elif table_name == "invoice":
        return (df
            .withColumn("InvoiceDate",      F.to_date(F.col("InvoiceDate")))
            .withColumn("BillingAddress",   F.trim(F.coalesce(F.col("BillingAddress"),   F.lit(""))))
            .withColumn("BillingCity",      F.trim(F.coalesce(F.col("BillingCity"),      F.lit("Unknown"))))
            .withColumn("BillingState",     F.trim(F.coalesce(F.col("BillingState"),     F.lit("Unknown"))))
            .withColumn("BillingCountry",   F.trim(F.coalesce(F.col("BillingCountry"),   F.lit("Unknown"))))
            .withColumn("BillingPostalCode",F.trim(F.coalesce(F.col("BillingPostalCode"),F.lit(""))))
        )

    elif table_name == "invoiceline":
        return df  # all columns are numeric — no string cleaning needed

    elif table_name == "mediatype":
        return df.withColumn("Name", F.trim(F.coalesce(F.col("Name"), F.lit("Unknown"))))

    elif table_name == "playlist":
        return df.withColumn("Name", F.trim(F.coalesce(F.col("Name"), F.lit("Unknown"))))

    elif table_name == "playlisttrack":
        return df  # only integer keys — no cleaning needed

    elif table_name == "track":
        return (df
            .withColumn("Name",     F.trim(F.col("Name")))
            .withColumn("Composer", F.trim(F.coalesce(F.col("Composer"), F.lit("Unknown"))))
        )

    return df  # fallback — return as-is


# COMMAND ----------
# MAGIC %md ## Main loop — process each Bronze table

# COMMAND ----------

# Get list of Bronze tables to process
bronze_tables = [row.tableName for row in
                 spark.sql(f"SHOW TABLES IN {catalog_name}.{bronze_schema}").collect()]

print(f"Bronze tables found: {len(bronze_tables)}")
print(f"Tables: {sorted(bronze_tables)}")

summary = []

for table_name in sorted(bronze_tables):
    bronze_table = f"{catalog_name}.{bronze_schema}.{table_name}"
    silver_table = f"{catalog_name}.{silver_schema}.{table_name}"
    execution_time = datetime.now()

    print(f"\n{'='*60}")
    print(f"Processing: {table_name}")
    print(f"{'='*60}")

    try:
        # ── 1. Read Bronze ────────────────────────────────────────
        bronze_df   = spark.read.table(bronze_table)
        total_count = bronze_df.count()

        # ── 2. DQX Profile ────────────────────────────────────────
        profile = profile_dataframe(bronze_df, table_name)

        # ── 3. Apply quality rules ────────────────────────────────
        rules = QUALITY_RULES.get(table_name, [])

        if rules:
            # Build combined pass condition — record must pass ALL rules
            pass_condition = " AND ".join([f"({expr})" for _, _, expr in rules])
            fail_condition = f"NOT ({pass_condition})"

            passed_df = bronze_df.filter(pass_condition)
            failed_df = bronze_df.filter(fail_condition)

            passed_count = passed_df.count()
            failed_count = failed_df.count()
        else:
            # No rules defined — all records pass
            passed_df     = bronze_df
            failed_df     = bronze_df.limit(0)  # empty
            passed_count  = total_count
            failed_count  = 0

        print(f"\n  DQX Results:")
        print(f"  Total    : {total_count}")
        print(f"  Passed   : {passed_count}")
        print(f"  Failed   : {failed_count}")

        # ── 4. Write failed records to quarantine ─────────────────
        if failed_count > 0:
            # Tag each failed record with the first rule it violated
            quarantine_rows = []
            for rule_name, col_name, expr in rules:
                violators = bronze_df.filter(f"NOT ({expr})").filter(pass_condition if passed_count > 0 else "1=1")
                # Simpler approach: write all failed records once with combined reason
            
            # Write quarantine records with metadata columns added
            quar_df = (failed_df
                .withColumn("table_name",     F.lit(table_name))
                .withColumn("execution_time", F.lit(execution_time.isoformat()))
                .withColumn("failed_rule",    F.lit("quality_rule_violation"))
                .withColumn("failed_column",  F.lit(",".join([c for _, c, _ in rules])))
                .withColumn("record_data",    F.to_json(F.struct([F.col(c) for c in failed_df.columns])))
                .withColumn("created_date",   F.lit(str(date.today())))
                .select("table_name", "execution_time", "failed_rule",
                        "failed_column", "record_data", "created_date")
            )

            (quar_df.write.format("delta").mode("append")
                .saveAsTable(f"{catalog_name}.{control_schema}.dqx_quarantine"))

            print(f"  {failed_count} records written to quarantine")

        # ── 5. Clean passed records ───────────────────────────────
        cleaned_df    = clean_dataframe(passed_df, table_name)
        print(f"\n  Cleaning applied for: {table_name}")

        # ── 6. Write to Silver ────────────────────────────────────
        (cleaned_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table)
        )

        silver_count = spark.read.table(silver_table).count()
        print(f"  Silver rows written: {silver_count}")

        # ── 7. Log to DQX execution log ───────────────────────────
        dqx_log_schema = StructType([
            StructField("table_name",     StringType(),    False),
            StructField("execution_time", TimestampType(), False),
            StructField("total_records",  LongType(),      False),
            StructField("passed_records", LongType(),      False),
            StructField("failed_records", LongType(),      False),
            StructField("rules_applied",  StringType(),    True),
            StructField("created_date",   DateType(),      False),
        ])

        rules_json = json.dumps([{"rule": r, "column": c} for r, c, _ in rules])
        dqx_log_row = [(table_name, execution_time, int(total_count),
                        int(passed_count), int(failed_count),
                        rules_json, date.today())]

        dqx_log_df = spark.createDataFrame(dqx_log_row, schema=dqx_log_schema)
        (dqx_log_df.write.format("delta").mode("append")
             .saveAsTable(f"{catalog_name}.{control_schema}.dqx_execution_log"))

        print(f"  DQX log written.")
        summary.append({"table": table_name, "total": total_count,
                         "passed": passed_count, "failed": failed_count,
                         "silver": silver_count, "status": "SUCCESS"})

    except Exception as e:
        print(f"  ERROR: {str(e)}")
        traceback.print_exc()
        summary.append({"table": table_name, "total": 0, "passed": 0,
                         "failed": 0, "silver": 0, "status": "FAILED"})

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

print("\n" + "="*70)
print("BRONZE → SILVER SUMMARY")
print("="*70)
print(f"  {'Table':<20} {'Total':>7} {'Passed':>7} {'Failed':>7} {'Silver':>7}   Status")
print(f"  {'─'*20} {'─'*7} {'─'*7} {'─'*7} {'─'*7}   {'─'*7}")
for s in summary:
    icon = "OK  " if s["status"] == "SUCCESS" else "FAIL"
    print(f"  [{icon}]  {s['table']:<20} {s['total']:>7} {s['passed']:>7} {s['failed']:>7} {s['silver']:>7}")

failed_tables = [s for s in summary if s["status"] == "FAILED"]
print(f"\n  Total: {len(summary)}  |  Success: {len(summary)-len(failed_tables)}  |  Failed: {len(failed_tables)}")
print("="*70)

if failed_tables:
    print(f"\n  Failed: {[s['table'] for s in failed_tables]}")
    dbutils.notebook.exit("FAILED")
else:
    print("\n  All tables written to Silver successfully.")
    dbutils.notebook.exit("SUCCESS")
