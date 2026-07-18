"""
Microsoft Fabric notebook (PySpark) -- Lakehouse ingestion for the inventory
domain, as a migration pilot alongside the existing Azure Data Factory (ADF)
pipeline.

This is written exactly as it would run inside a Fabric notebook cell
(Fabric notebooks use Spark, not plain Python -- `spark` and `notebookutils`
are provided by the Fabric runtime, not imported). Kept here as a .py file
so it's readable/reviewable outside the Fabric UI and can be pasted directly
into a notebook cell.

Validated end-to-end in a live Fabric workspace against a 500-row sample
(see README "Real run results"). One correction made after that real run:
saveAsTable() takes just the table name ("fct_inventory"), not
"nikkiso_lakehouse.fct_inventory" -- once a Lakehouse is attached to the
notebook as the default, Spark already scopes writes to its `dbo` schema,
and the extra qualifier raises SCHEMA_NOT_FOUND.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

# ---------------------------------------------------------------------------
# 1. Define the expected schema explicitly (do not let Spark infer it --
#    inferred schemas silently drift when the source adds/changes columns).
# ---------------------------------------------------------------------------
inventory_schema = StructType([
    StructField("sku", StringType(), False),
    StructField("product_family", StringType(), True),
    StructField("warehouse", StringType(), False),
    StructField("on_hand_qty", IntegerType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("reorder_point", IntegerType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("last_received_date", DateType(), True),
])

# ---------------------------------------------------------------------------
# 2. Read raw CSV extracts landed in the Fabric Lakehouse "Files" area
#    (this is the same drop location ADF currently writes to -- the pilot
#    only changes what reads FROM there, not the extraction step itself).
# ---------------------------------------------------------------------------
raw_path = "Files/raw/inventory/*.csv"

df_raw = (
    spark.read
    .option("header", True)
    .schema(inventory_schema)
    .csv(raw_path)
)

# ---------------------------------------------------------------------------
# 3. Data quality gate BEFORE writing to the managed Lakehouse table.
#    Anything that fails these checks gets quarantined, not silently loaded.
# ---------------------------------------------------------------------------
bad_rows = df_raw.filter(
    F.col("sku").isNull()
    | F.col("warehouse").isNull()
    | (F.col("on_hand_qty") < 0)
)

bad_row_count = bad_rows.count()
if bad_row_count > 0:
    (
        bad_rows.write
        .mode("append")
        .format("delta")
        .save("Files/quarantine/inventory_rejects")
    )
    print(f"WARNING: {bad_row_count} rows quarantined -- see Files/quarantine/inventory_rejects")

df_clean = df_raw.subtract(bad_rows)

# ---------------------------------------------------------------------------
# 4. Transform (mirrors stg_inventory.sql / fct_inventory.sql from the dbt
#    pilot in Project 1 -- same business logic, Spark instead of Snowflake SQL,
#    so the two projects tell a consistent story about the underlying model).
# ---------------------------------------------------------------------------
df_transformed = (
    df_clean
    .withColumn("inventory_value", F.col("on_hand_qty") * F.col("unit_cost"))
    .withColumn(
        "is_below_reorder_point",
        F.when(F.col("on_hand_qty") <= F.col("reorder_point"), True).otherwise(False)
    )
    .withColumn("ingested_at", F.current_timestamp())
)

# ---------------------------------------------------------------------------
# 5. Write to the managed Lakehouse Delta table. `mergeSchema` allows additive
#    schema evolution (new column added upstream) without breaking the load.
# ---------------------------------------------------------------------------
(
    df_transformed.write
    .mode("overwrite")
    .format("delta")
    .option("mergeSchema", "true")
    .saveAsTable("fct_inventory")
)

print(f"Loaded {df_transformed.count()} rows into nikkiso_lakehouse.fct_inventory")

# ---------------------------------------------------------------------------
# 6. Log a lightweight run record so pipeline health can be tracked over time
#    (feeds the observability approach in Project 6).
# ---------------------------------------------------------------------------
run_log_row = spark.createDataFrame(
    [(
        "fct_inventory",
        df_transformed.count(),
        bad_row_count,
    )],
    ["table_name", "rows_loaded", "rows_quarantined"],
).withColumn("run_timestamp", F.current_timestamp())

(
    run_log_row.write
    .mode("append")
    .format("delta")
    .saveAsTable("pipeline_run_log")
)
