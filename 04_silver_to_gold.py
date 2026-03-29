# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver to Gold
# MAGIC Builds the dimensional model from Silver tables.
# MAGIC
# MAGIC **Build order:**
# MAGIC 1. dim_date          (derived from invoice dates)
# MAGIC 2. dim_artist
# MAGIC 3. dim_album         (FK → dim_artist)
# MAGIC 4. dim_genre
# MAGIC 5. dim_media_type
# MAGIC 6. dim_employee      (self-referencing manager FK)
# MAGIC 7. dim_customer      (SCD Type 2 — MERGE logic)
# MAGIC 8. dim_track         (FK → dim_album, dim_genre, dim_media_type)
# MAGIC 9. fact_sales        (grain: one row per invoice line)
# MAGIC 10. fact_sales_customer_agg (built FROM fact_sales, not Silver)

# COMMAND ----------

dbutils.widgets.text("catalog_name",  "workspace")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema",   "gold")

# COMMAND ----------

catalog_name  = dbutils.widgets.get("catalog_name")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema   = dbutils.widgets.get("gold_schema")

s = f"{catalog_name}.{silver_schema}"
g = f"{catalog_name}.{gold_schema}"

print(f"Silver : {s}")
print(f"Gold   : {g}")

# COMMAND ----------

# MAGIC %md ## 1 — dim_date

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_date
USING DELTA
AS
SELECT
    CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT)  AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    DATE_FORMAT(full_date, 'MMMM') AS month_name,
    DAY(full_date) AS day,
    DAYOFWEEK(full_date) AS day_of_week,
    DATE_FORMAT(full_date, 'EEEE') AS day_name,
    CASE WHEN DAYOFWEEK(full_date) IN (1,7)
         THEN 'Y' ELSE 'N' END AS is_weekend
FROM (
    SELECT DISTINCT CAST(InvoiceDate AS DATE) AS full_date
    FROM {s}.invoice
    WHERE InvoiceDate IS NOT NULL
)
ORDER BY full_date
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_date").collect()[0]["c"]
print(f"dim_date        : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 2 — dim_artist

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_artist
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ArtistId)  AS artist_key,
    ArtistId                               AS artist_id,
    Name                                   AS artist_name
FROM {s}.artist
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_artist").collect()[0]["c"]
print(f"dim_artist      : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 3 — dim_album

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_album
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY a.AlbumId)  AS album_key,
    a.AlbumId                               AS album_id,
    a.Title                                 AS album_title,
    da.artist_key
FROM {s}.album     a
JOIN {g}.dim_artist da ON a.ArtistId = da.artist_id
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_album").collect()[0]["c"]
print(f"dim_album       : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 4 — dim_genre

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_genre
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY GenreId)  AS genre_key,
    GenreId                               AS genre_id,
    Name                                  AS genre_name
FROM {s}.genre
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_genre").collect()[0]["c"]
print(f"dim_genre       : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 5 — dim_media_type

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_media_type
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY MediaTypeId)  AS media_type_key,
    MediaTypeId                               AS media_type_id,
    Name                                      AS media_type_name
FROM {s}.mediatype
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_media_type").collect()[0]["c"]
print(f"dim_media_type  : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 6 — dim_employee

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_employee
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY e.EmployeeId)       AS employee_key,
    e.EmployeeId                                    AS employee_id,
    e.FirstName                                     AS first_name,
    e.LastName                                      AS last_name,
    CONCAT(e.FirstName, ' ', e.LastName)            AS full_name,
    e.Title                                         AS title,
    e.ReportsTo                                     AS reports_to_id,
    e.BirthDate                                     AS birth_date,
    e.HireDate                                      AS hire_date,
    e.City                                          AS city,
    e.State                                         AS state,
    e.Country                                       AS country,
    e.Email                                         AS email,
    e.Phone                                         AS phone
FROM {s}.employee e
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_employee").collect()[0]["c"]
print(f"dim_employee    : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 7 — dim_customer (SCD Type 2)
# MAGIC
# MAGIC First run (Version 1): creates the table and inserts all customers
# MAGIC as current records (is_current = true, effective_end_date = NULL).
# MAGIC
# MAGIC Subsequent runs (Version 2): MERGE detects changed attributes,
# MAGIC expires the old record and inserts a new active record.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date
 
# Columns we track for SCD2 changes
SCD2_COLS = ["first_name","last_name","company","address","city",
             "state","country","postal_code","phone","email"]
 
# Build the incoming Silver snapshot with surrogate-ready columns
incoming_df = spark.sql(f"""
    SELECT
        c.CustomerId                                        AS customer_id,
        TRIM(c.FirstName)                                   AS first_name,
        TRIM(c.LastName)                                    AS last_name,
        TRIM(COALESCE(c.Company,    ''))                    AS company,
        TRIM(COALESCE(c.Address,    ''))                    AS address,
        TRIM(COALESCE(c.City,       'Unknown'))             AS city,
        TRIM(COALESCE(c.State,      'Unknown'))             AS state,
        TRIM(c.Country)                                     AS country,
        TRIM(COALESCE(c.PostalCode, ''))                    AS postal_code,
        TRIM(COALESCE(c.Phone,      ''))                    AS phone,
        TRIM(COALESCE(c.Fax,        ''))                    AS fax,
        LOWER(TRIM(c.Email))                                AS email,
        de.employee_key                                     AS support_rep_key
    FROM {s}.customer c
    LEFT JOIN {g}.dim_employee de ON c.SupportRepId = de.employee_id
""")
 
today = str(date.today())
 
# ── Check if dim_customer already exists ─────────────────────────
table_exists = spark.sql(f"""
    SELECT COUNT(*) AS c
    FROM information_schema.tables
    WHERE table_catalog = '{catalog_name}'
      AND table_schema   = '{gold_schema}'
      AND table_name     = 'dim_customer'
""").collect()[0]["c"] > 0
 
if not table_exists:
    # ── FIRST RUN — build entirely with DataFrame API ─────────────
    print("First run detected — creating dim_customer with all current records.")
 
    w = Window.orderBy("customer_id")
 
    first_run_df = (incoming_df
        .withColumn("customer_key",         F.row_number().over(w))
        .withColumn("effective_start_date",  F.lit(today).cast("date"))
        .withColumn("effective_end_date",    F.lit(None).cast("date"))
        .withColumn("is_current",            F.lit(True))
        .select(
            "customer_key","customer_id","first_name","last_name",
            "company","address","city","state","country","postal_code",
            "phone","fax","email","support_rep_key",
            "effective_start_date","effective_end_date","is_current"
        )
    )
 
    (first_run_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema","true")
         .saveAsTable(f"{g}.dim_customer"))
 
else:
    # ── SUBSEQUENT RUNS — SCD2 MERGE ─────────────────────────────
    print("Existing dim_customer found — running SCD2 MERGE.")
 
    dim_customer = DeltaTable.forName(spark, f"{g}.dim_customer")
 
    # Step 1: Expire changed current records
    # Build change detection condition
    change_conditions = " OR ".join([
        f"existing.{c} <> incoming.{c}" for c in SCD2_COLS
    ])
 
    dim_customer.alias("existing").merge(
        incoming_df.alias("incoming"),
        "existing.customer_id = incoming.customer_id AND existing.is_current = true"
    ).whenMatchedUpdate(
        condition = change_conditions,
        set = {
            "effective_end_date" : F.expr(f"date_sub('{today}', 1)"),
            "is_current"         : F.lit(False)
        }
    ).execute()
 
    # Step 2: Insert new records for changed + new customers
    # Get max surrogate key to continue sequence
    max_key = spark.sql(f"SELECT MAX(customer_key) AS mk FROM {g}.dim_customer").collect()[0]["mk"] or 0
 
    # Find customers that are changed or brand new
    new_records_df = incoming_df.alias("inc").join(
        spark.read.table(f"{g}.dim_customer").filter("is_current = true").alias("dim"),
        F.col("inc.customer_id") == F.col("dim.customer_id"),
        "left_anti"   # customers not in current dim = new or just expired
    )
 
    if new_records_df.count() > 0:
        w = Window.orderBy("customer_id")
        new_records_df = (new_records_df
            .withColumn("customer_key",         F.row_number().over(w) + F.lit(max_key))
            .withColumn("effective_start_date",  F.lit(today).cast("date"))
            .withColumn("effective_end_date",    F.lit(None).cast("date"))
            .withColumn("is_current",            F.lit(True))
            .select("customer_key","customer_id","first_name","last_name",
                    "company","address","city","state","country","postal_code",
                    "phone","fax","email","support_rep_key",
                    "effective_start_date","effective_end_date","is_current")
        )
 
        (new_records_df.write.format("delta").mode("append")
             .saveAsTable(f"{g}.dim_customer"))
 
        print(f"  New/changed records inserted: {new_records_df.count()}")
    else:
        print("  No changes detected — dim_customer is up to date.")
 
cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_customer").collect()[0]["c"]
current_cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_customer WHERE is_current = true").collect()[0]["c"]
print(f"dim_customer    : {cnt} total rows | {current_cnt} current")

# COMMAND ----------

# MAGIC %md ## 8 — dim_track

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_track
USING DELTA
AS
SELECT
    ROW_NUMBER() OVER (ORDER BY t.TrackId)      AS track_key,
    t.TrackId                                   AS track_id,
    t.Name                                      AS track_name,
    da.album_key,
    dmt.media_type_key,
    dg.genre_key,
    t.Composer                                  AS composer,
    t.Milliseconds                              AS duration_ms,
    ROUND(t.Milliseconds / 60000.0, 2)          AS duration_min,
    t.Bytes                                     AS bytes,
    t.UnitPrice                                 AS unit_price
FROM {s}.track          t
LEFT JOIN {g}.dim_album      da  ON t.AlbumId      = da.album_id
LEFT JOIN {g}.dim_media_type dmt ON t.MediaTypeId  = dmt.media_type_id
LEFT JOIN {g}.dim_genre      dg  ON t.GenreId      = dg.genre_id
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.dim_track").collect()[0]["c"]
print(f"dim_track       : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 9 — fact_sales (grain: one row per invoice line)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.fact_sales
USING DELTA
AS
SELECT
    il.InvoiceLineId                            AS invoice_line_id,
    il.InvoiceId                                AS invoice_id,
    dc.customer_key,
    dd.date_key,
    dt.track_key,
    dt.album_key,
    dt.genre_key,
    dt.media_type_key,
    de.employee_key,
    il.UnitPrice                                AS unit_price,
    il.Quantity                                 AS quantity,
    ROUND(il.UnitPrice * il.Quantity, 2)        AS line_total,
    inv.BillingCity                             AS billing_city,
    inv.BillingCountry                          AS billing_country
FROM {s}.invoiceline    il
JOIN {s}.invoice        inv ON il.InvoiceId      = inv.InvoiceId
JOIN {g}.dim_customer   dc  ON inv.CustomerId    = dc.customer_id
                            AND dc.is_current    = true
JOIN {g}.dim_date       dd  ON CAST(inv.InvoiceDate AS DATE) = dd.full_date
JOIN {g}.dim_track      dt  ON il.TrackId        = dt.track_id
LEFT JOIN {g}.dim_employee de ON dc.support_rep_key = de.employee_key
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.fact_sales").collect()[0]["c"]
print(f"fact_sales      : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## 10 — fact_sales_customer_agg
# MAGIC Built FROM fact_sales — never directly from Silver.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.fact_sales_customer_agg
USING DELTA
AS
SELECT
    customer_key,
    MAX(employee_key)                           AS employee_key,
    COUNT(DISTINCT invoice_id)                  AS total_invoices,
    COUNT(invoice_line_id)                      AS total_line_items,
    SUM(quantity)                               AS total_quantity,
    ROUND(SUM(line_total),   2)                 AS total_revenue,
    ROUND(AVG(line_total),   2)                 AS avg_line_total,
    ROUND(MAX(line_total),   2)                 AS max_line_total,
    MIN(date_key)                               AS first_purchase_date_key,
    MAX(date_key)                               AS last_purchase_date_key
FROM {g}.fact_sales
GROUP BY customer_key
""")

cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.fact_sales_customer_agg").collect()[0]["c"]
print(f"fact_sales_customer_agg : {cnt} rows")

# COMMAND ----------

# MAGIC %md ## Final summary

# COMMAND ----------

gold_tables = [
    "dim_date", "dim_artist", "dim_album", "dim_genre",
    "dim_media_type", "dim_employee", "dim_customer",
    "dim_track", "fact_sales", "fact_sales_customer_agg"
]

print("\n" + "="*50)
print("GOLD LAYER — FINAL ROW COUNTS")
print("="*50)
for t in gold_tables:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {g}.{t}").collect()[0]["c"]
        print(f"  {t:<30} {cnt:>7} rows")
    except Exception as e:
        print(f"  {t:<30}   ERROR: {e}")
print("="*50)

dbutils.notebook.exit("SUCCESS")
