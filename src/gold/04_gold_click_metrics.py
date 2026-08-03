# Databricks notebook source
# MAGIC %md
# MAGIC # Read Data From Silver Layer

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

silver_table = "adb_customer360_dev.silver.click_events"
gold_table = "adb_customer360_dev.gold.daily_customer_engagement"

silver_df = spark.table(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregate Daily Metrics

# COMMAND ----------

gold_source_df = (
    silver_df
    .groupBy("business_date")
    .agg(

        F.count("*").alias("total_events"),

        F.countDistinct("customer_id")
            .alias("unique_customers"),

        F.countDistinct("session_id")
            .alias("unique_sessions"),

        F.sum(
            F.when(
                F.col("event_type") == "click",
                1
            ).otherwise(0)
        ).alias("click_count"),

        F.sum(
            F.when(
                F.col("event_type") == "add_to_cart",
                1
            ).otherwise(0)
        ).alias("add_to_cart_count"),

        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                1
            ).otherwise(0)
        ).alias("purchase_count")

    )

    .withColumn(
        "click_percentage",
        F.round(
            F.col("click_count") /
            F.col("total_events"),
            4
        )
    )

    .withColumn(
        "cart_percentage",
        F.round(
            F.col("add_to_cart_count") /
            F.col("total_events"),
            4
        )
    )

    .withColumn(
        "purchase_percentage",
        F.round(
            F.col("purchase_count") /
            F.col("total_events"),
            4
        )
    )

    .withColumn(
        "_gold_processed_at",
        F.current_timestamp()
    )
)

display(gold_source_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge Into Gold Delta Layer

# COMMAND ----------

if spark.catalog.tableExists(gold_table):

    gold_delta = DeltaTable.forName(
        spark,
        gold_table
    )

    (
        gold_delta.alias("target")
        .merge(
            gold_source_df.alias("source"),
            "target.business_date = source.business_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        gold_source_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_customer360_dev.gold.daily_customer_engagement
# MAGIC ORDER BY business_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Creat the External Location for Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ext_datalake
# MAGIC URL 'abfss://datalake@stcustomer360dev.dfs.core.windows.net/' -- abfss://container name + storage account name + dfs.core.windows.net(end points)
# MAGIC WITH (
# MAGIC     STORAGE CREDENTIAL cred_customer360_adls
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Publish Gold to ADLS path

# COMMAND ----------

gold_serving_path = (
    "abfss://datalake@stcustomer360dev.dfs.core.windows.net/"
    "gold/daily_customer_engagement/"
)

(
    spark.table("adb_customer360_dev.gold.daily_customer_engagement")
    .write
    .format("parquet")
    .mode("overwrite")
    .save(gold_serving_path)
)