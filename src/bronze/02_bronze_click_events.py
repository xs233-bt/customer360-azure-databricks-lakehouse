# Databricks notebook source
spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

click_events_path = (
    "abfss://landing@stcustomer360dev.dfs.core.windows.net/"
    "click_events/"
)

raw_df = (
    spark.read
    .option("multiline", "true")
    .json(click_events_path)
)

display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Expode df to get the looped data**

# COMMAND ----------

from pyspark.sql import functions as F

events_df = (
    raw_df
    .select(
        F.explode("events").alias("event"),
        F.col("page").alias("source_page"),
        F.col("page_size"),
        F.col("total_pages"),
        F.col("total_records"),
        F.expr("_metadata.file_path").alias("_source_file")
    )
    .select(
        F.col("event.event_id").alias("event_id"),
        F.col("event.customer_id").alias("customer_id"),
        F.col("event.product_id").alias("product_id"),
        F.col("event.session_id").alias("session_id"),
        F.col("event.event_time").alias("event_time"),
        F.col("event.event_type").alias("event_type"),
        F.col("event.page").alias("page_name"),
        F.col("event.device").alias("device"),
        F.col("event.country").alias("country"),
        F.to_date("event.business_date").alias("business_date"),
        "source_page",
        "page_size",
        "total_pages",
        "total_records",
        "_source_file"
    )
    .dropDuplicates(["event_id"])
    .withColumn("_bronze_ingested_at", F.current_timestamp())
)
display(events_df)

# COMMAND ----------

events_df.printSchema()

# COMMAND ----------

print(events_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Write click_evnets into Bronze Delta

# COMMAND ----------

target_table = "adb_customer360_dev.bronze.click_events"

if spark.catalog.tableExists(target_table):
    bronze_table = DeltaTable.forName(spark, target_table)

    (
        bronze_table.alias("target")
        .merge(
            events_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        events_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE adb_customer360_dev.bronze.click_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT event_id) AS distinct_events
# MAGIC FROM adb_customer360_dev.bronze.click_events;