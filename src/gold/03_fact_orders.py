# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 - Read Source Tables

# COMMAND ----------

from pyspark.sql import functions as F

gold_customer_df = spark.table(
    "adb_customer360_dev.gold.dim_customer"
)

gold_product_df = spark.table(
    "adb_customer360_dev.gold.dim_product"
)

silver_orders_df = spark.table(
    "adb_customer360_dev.silver.orders"
)

print(f"Customers: {gold_customer_df.count()}")
print(f"Products: {gold_product_df.count()}")
print(f"Orders: {silver_orders_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Build the Order Fact Table

# COMMAND ----------

# Build the order fact table by joining Silver orders
# with the customer and product dimensions

fact_orders_df = (
    silver_orders_df.alias("o")

    .join(
        gold_customer_df.select(
            "customer_key",
            "customer_id"
        ).alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )

    .join(
        gold_product_df.select(
            "product_key",
            "product_id"
        ).alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )

    .select(
        F.col("o.order_id").cast("long").alias("order_id"),
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("o.order_timestamp"),
        F.to_date("o.order_timestamp").alias("order_date"),
        F.col("o.quantity"),
        F.col("o.unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.col("o.discount_pct").cast("decimal(5,4)").alias("discount_pct"),
        F.col("o.tax_amount").cast("decimal(12,2)").alias("tax_amount"),
        F.col("o.order_amount").cast("decimal(12,2)").alias("order_amount"),
        F.col("o.status").alias("order_status"),
        F.current_timestamp().alias("_gold_processed_at")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Add Derived DATE Attributes

# COMMAND ----------

# Add date attributes to support time-based analytics

fact_orders_df = (
    fact_orders_df
    .withColumn(
        "order_year",
        F.year(F.col("order_date"))
    )
    .withColumn(
        "order_month",
        F.month(F.col("order_date"))
    )
    .withColumn(
        "order_month_name",
        F.date_format(F.col("order_date"), "MMMM")
    )
    .withColumn(
        "order_quarter",
        F.quarter(F.col("order_date"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Validate Referential Integrity

# COMMAND ----------

# Validate that all fact records have matching dimension keys

referential_integrity_df = fact_orders_df.select(
    F.sum(
        F.when(
            F.col("customer_key").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_customer_keys"),

    F.sum(
        F.when(
            F.col("product_key").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_product_keys")
)

display(referential_integrity_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 5 - Create Valid and Rejected Fact Records

# COMMAND ----------

# Separate records with valid dimension keys
# from records that violate referential integrity

valid_fact_orders_df = fact_orders_df.filter(
    F.col("customer_key").isNotNull()
    & F.col("product_key").isNotNull()
)

rejected_fact_orders_df = (
    fact_orders_df
    .filter(
        F.col("customer_key").isNull()
        | F.col("product_key").isNull()
    )
    .withColumn(
        "reject_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("customer_key").isNull(),
                F.lit("Missing Customer Dimension Key")
            ),
            F.when(
                F.col("product_key").isNull(),
                F.lit("Missing Product Dimension Key")
            )
        )
    )
    .withColumn(
        "_rejected_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

source_count = silver_orders_df.count()
valid_count = valid_fact_orders_df.count()
rejected_count = rejected_fact_orders_df.count()

print(f"Source order count: {source_count}")
print(f"Valid fact records: {valid_count}")
print(f"Rejected fact records: {rejected_count}")
print(
    f"Reconciliation passed: "
    f"{source_count == valid_count + rejected_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 6 - Validate Duplicate Order IDs

# COMMAND ----------

# Validate that the fact table contains one row per order

duplicate_order_count = (
    valid_fact_orders_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate order IDs before writing: "
    f"{duplicate_order_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 7 - Write the Gold Tables
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

target_table = "adb_customer360_dev.gold.fact_orders"

if not spark.catalog.tableExists(target_table):
    (
        fact_orders_df.write
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
            fact_orders_df.alias("s"),
            "t.order_id = s.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# Write rejected order fact records for auditing

(
    rejected_fact_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "adb_customer360_dev.gold.fact_order_rejected"
    )
)

print("Rejected order fact table created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 8 - Validate the Gold Results

# COMMAND ----------

gold_fact_count = spark.table(
    "adb_customer360_dev.gold.fact_orders"
).count()

gold_rejected_count = spark.table(
    "adb_customer360_dev.gold.fact_order_rejected"
).count()

duplicate_order_count = (
    spark.table("adb_customer360_dev.gold.fact_orders")
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Gold fact order count: {gold_fact_count}")
print(f"Rejected fact order count: {gold_rejected_count}")
print(f"Duplicate order IDs remaining: {duplicate_order_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Check the uniqueness of the primary key

# COMMAND ----------

# MAGIC %sql
# MAGIC Select
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT order_id) AS distinct_order_ids
# MAGIC FROM
# MAGIC     adb_customer360_dev.gold.fact_orders;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY adb_customer360_dev.gold.fact_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ![{44FF1035-5053-46B7-A443-6447E9ED4D55}_1785610086381.png](./{44FF1035-5053-46B7-A443-6447E9ED4D55}_1785610086381.png "{44FF1035-5053-46B7-A443-6447E9ED4D55}_1785610086381.png")

# COMMAND ----------

# MAGIC %md
# MAGIC Uplode to ADLS gold folder

# COMMAND ----------

fact_orders_path = (
    "abfss://datalake@stcustomer360dev.dfs.core.windows.net/"
    "gold/fact_orders/"
)

(
    spark.table("adb_customer360_dev.gold.fact_orders")
    .write
    .mode("overwrite")
    .parquet(fact_orders_path)
)