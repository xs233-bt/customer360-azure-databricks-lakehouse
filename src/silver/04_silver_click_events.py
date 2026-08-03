# Databricks notebook source
# MAGIC %md
# MAGIC # Read Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

bronze_table = "adb_customer360_dev.bronze.click_events"
silver_table = "adb_customer360_dev.silver.click_events"

bronze_df = spark.table(bronze_table)

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleasing

# COMMAND ----------

silver_source_df = (
    bronze_df

    # Keep one record per event
    .dropDuplicates(["event_id"])

    # Required business keys
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("session_id").isNotNull())
    .filter(F.col("event_type").isNotNull())

    # Convert data types
    .withColumn(
        "event_timestamp",
        F.to_timestamp("event_time")
    )
    .withColumn(
        "business_date",
        F.to_date("business_date")
    )

    # Standardize categorical values
    .withColumn(
        "event_type",
        F.lower(F.trim(F.col("event_type")))
    )
    .withColumn(
        "device",
        F.lower(F.trim(F.col("device")))
    )
    .withColumn(
        "country",
        F.upper(F.trim(F.col("country")))
    )
    .withColumn(
        "page_name",
        F.lower(F.trim(F.col("page_name")))
    )

    # Basic validation
    .filter(F.col("event_timestamp").isNotNull())
    .filter(
        F.col("event_type").isin(
            "page_view",
            "product_view",
            "add_to_cart",
            "checkout",
            "purchase",
            "click"
        )
    )

    # Audit fields
    .withColumn("_silver_processed_at", F.current_timestamp())

    # Remove raw string timestamp after conversion
    .drop("event_time")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Check Row Nums

# COMMAND ----------

print("Bronze rows:", bronze_df.count())
print("Silver valid rows:", silver_source_df.count())

display(silver_source_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Check

# COMMAND ----------

dq_summary_df = silver_source_df.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("event_id").alias("distinct_event_ids"),
    F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer_ids"),
    F.sum(F.col("session_id").isNull().cast("int")).alias("null_session_ids"),
    F.sum(F.col("event_timestamp").isNull().cast("int")).alias("invalid_timestamps")
)

display(dq_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge to Delta Silver Table

# COMMAND ----------

if spark.catalog.tableExists(silver_table):
    silver_delta = DeltaTable.forName(spark, silver_table)

    (
        silver_delta.alias("target")
        .merge(
            silver_source_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        silver_source_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT event_id) AS distinct_events,
# MAGIC     MIN(event_timestamp) AS min_event_timestamp,
# MAGIC     MAX(event_timestamp) AS max_event_timestamp
# MAGIC FROM adb_customer360_dev.silver.click_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     event_type,
# MAGIC     COUNT(*) AS event_count
# MAGIC FROM adb_customer360_dev.silver.click_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY event_count DESC;