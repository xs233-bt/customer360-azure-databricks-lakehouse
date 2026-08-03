# Databricks notebook source
# MAGIC %md
# MAGIC Step 1: Read Silver Customer Data

# COMMAND ----------

from pyspark.sql import functions as F

silver_customers_df = spark.table(
    "adb_customer360_dev.silver.customers"
)

print(f"Silver customer count: {silver_customers_df.count()}")

display(silver_customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Build the Customer Dimension

# COMMAND ----------

# Build the customer dimension for analytics

dim_customer_df = (
    silver_customers_df
    .select(
        F.col("customer_id").cast("long").alias("customer_key"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.concat_ws(
            " ",
            F.col("first_name"),
            F.col("last_name")
        ).alias("customer_name"),
        F.col("email"),
        F.col("city"),
        F.col("province"),
        F.col("signup_date"),
        F.col("status"),
        F.current_timestamp().alias("_gold_processed_at")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Write the Customer Dimension

# COMMAND ----------

from delta.tables import DeltaTable

target_table = "adb_customer360_dev.gold.dim_customer"

if not spark.catalog.tableExists(target_table):
    (
        dim_customer_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
else:
    target = DeltaTable.forName(spark, target_table)

    (
        target.alias("t")
        .merge(
            dim_customer_df.alias("s"),
            "t.customer_id = s.customer_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4: Validate the Customer Dimension

# COMMAND ----------

customer_dimension_count = spark.table(
    "adb_customer360_dev.gold.dim_customer"
).count()

duplicate_customer_keys = (
    spark.table("adb_customer360_dev.gold.dim_customer")
    .groupBy("customer_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Customer dimension count: "
    f"{customer_dimension_count}"
)

print(
    f"Duplicate customer keys remaining: "
    f"{duplicate_customer_keys}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Upload to ADLS Gold Folder

# COMMAND ----------

customer_path = (
    "abfss://datalake@stcustomer360dev.dfs.core.windows.net/"
    "gold/dim_customer/"
)

(
    spark.table("adb_customer360_dev.gold.dim_customer")
    .write
    .mode("overwrite")
    .parquet(customer_path)
)