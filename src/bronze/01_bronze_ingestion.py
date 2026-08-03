# Databricks notebook source
# MAGIC %md
# MAGIC Create Parameters

# COMMAND ----------

dbutils.widgets.text("source_name", "customer", "Source Name")
dbutils.widgets.text(
    "landing_path",
    "/Volumes/adb_customer360_dev/bronze/raw_files/customer/customer_20260729",
    "Landing Path"
)
dbutils.widgets.text(
    "target_table",
    "adb_customer360_dev.bronze.customers",
    "Target Table"
)
dbutils.widgets.text("source_system", "CRM", "Source System")
dbutils.widgets.text("batch_id", "20260729", "Batch ID")
dbutils.widgets.dropdown(
    "load_mode",
    "overwrite",
    ["overwrite", "append"],
    "Load Mode"
)
dbutils.widgets.dropdown(
    "source_format",
    "csv",
    ["csv", "avro", "json", "parquet"],
    "Source Format"
)

source_format = dbutils.widgets.get("source_format")

print(f"Source format: {source_format}")

# COMMAND ----------

# MAGIC %md
# MAGIC Read Parameters

# COMMAND ----------

source_name = dbutils.widgets.get("source_name")
landing_path = dbutils.widgets.get("landing_path")
target_table = dbutils.widgets.get("target_table")
source_system = dbutils.widgets.get("source_system")
batch_id = dbutils.widgets.get("batch_id")
load_mode = dbutils.widgets.get("load_mode")

print(f"Source name: {source_name}")
print(f"Landing path: {landing_path}")
print(f"Target table: {target_table}")
print(f"Source system: {source_system}")
print(f"Batch ID: {batch_id}")
print(f"Load mode: {load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC Read Landing CSV and add Metadata

# COMMAND ----------

from pyspark.sql import functions as F

reader = spark.read.format(source_format)

if source_format == "csv":
    reader = (
        reader
        .option("header", True)
        .option("inferSchema", True)
    )

bronze_df = (
    reader
    .load(landing_path)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_modification_time").alias(
            "_source_file_modified_at"
        )
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_name", F.lit(source_name))
    .withColumn("_source_system", F.lit(source_system))
    .withColumn("_source_format", F.lit(source_format))
    .withColumn("_batch_id", F.lit(batch_id))
)

display(bronze_df.limit(10))

# COMMAND ----------

record_count = bronze_df.count()

if record_count == 0:
    raise ValueError(
        f"No records were found in landing path: {landing_path}"
    )

print(f"Validation passed: {record_count} records found.")

# COMMAND ----------

# MAGIC %md
# MAGIC Write into Bronze Delta

# COMMAND ----------

display(bronze_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the source_name, .... , load_mode

# COMMAND ----------

print("Source name:", source_name)
print("Landing path:", landing_path)
print("Target table:", target_table)
print("Source format:", source_format)
print("Load mode:", load_mode)

# COMMAND ----------

from pyspark.sql import functions as F

target_table = "adb_customer360_dev.bronze.orders"
target_schema = spark.table(target_table).schema

source_columns = set(bronze_df.columns)

for field in target_schema.fields:
    if field.name in source_columns:
        bronze_df = bronze_df.withColumn(
            field.name,
            F.col(field.name).cast(field.dataType)
        )

bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

if "order_status" in bronze_df.columns:
    bronze_df = (
        bronze_df
        .withColumn(
            "status",
            F.upper(F.trim(F.col("order_status")))
        )
        .drop("order_status")
    )

# COMMAND ----------

writer = (
    bronze_df.write
    .format("delta")
    .mode(load_mode)
)

if load_mode == "overwrite":
    writer = writer.option("overwriteSchema", "true")

elif load_mode == "append":
    writer = writer.option("mergeSchema", "true")

writer.saveAsTable(target_table)

print(
    f"Successfully loaded {record_count} records "
    f"from {landing_path} into {target_table}"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN adb_customer360_dev.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS table_name, COUNT(*) AS record_count
# MAGIC FROM adb_customer360_dev.bronze.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'payments', COUNT(*)
# MAGIC FROM adb_customer360_dev.bronze.payments;

# COMMAND ----------

display(spark.table(target_table).limit(10))

print(
    "Target record count:",
    spark.table(target_table).count()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _source_name,
# MAGIC     _source_system,
# MAGIC     _batch_id,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM adb_customer360_dev.bronze.products
# MAGIC GROUP BY
# MAGIC     _source_name,
# MAGIC     _source_system,
# MAGIC     _batch_id;

# COMMAND ----------

# MAGIC %md
# MAGIC **Product TABLE**
# MAGIC                                                                         

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CAST(order_timestamp AS DATE) AS order_date,
# MAGIC     COUNT(*) AS orders
# MAGIC FROM adb_customer360_dev.bronze.orders
# MAGIC GROUP BY CAST(order_timestamp AS DATE)
# MAGIC ORDER BY order_date DESC;