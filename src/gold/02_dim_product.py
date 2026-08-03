# Databricks notebook source
# MAGIC %md
# MAGIC Step 1: Read Silver Product Data

# COMMAND ----------

# Read Silver product data

from pyspark.sql import functions as F

silver_products_df = spark.table(
    "adb_customer360_dev.silver.products"
)

print(f"Silver product count: {silver_products_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN adb_customer360_dev.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Create dim_product_df

# COMMAND ----------

# Build the product dimension for analytics

dim_product_df = (
    silver_products_df
    .select(
        F.col("product_id").cast("long").alias("product_key"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("price").cast("decimal(12,2)").alias("current_price"),
        F.col("status"),
        F.current_timestamp().alias("_gold_processed_at")
    )
)

display(dim_product_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Write into gold table

# COMMAND ----------

from delta.tables import DeltaTable

target_table = "adb_customer360_dev.gold.dim_product"

if not spark.catalog.tableExists(target_table):
    (
        dim_product_df.write
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
            dim_product_df.alias("s"),
            "t.product_id = s.product_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

display(
    spark.table("adb_customer360_dev.gold.dim_product")
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Upload to ADLS Gold Folder

# COMMAND ----------

product_path = (
    "abfss://datalake@stcustomer360dev.dfs.core.windows.net/"
    "gold/dim_product/"
)

(
    spark.table("adb_customer360_dev.gold.dim_product")
    .write
    .mode("overwrite")
    .parquet(product_path)
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://datalake@stcustomer360dev.dfs.core.windows.net/gold/"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://datalake@stcustomer360dev.dfs.core.windows.net/gold/fact_orders/"))