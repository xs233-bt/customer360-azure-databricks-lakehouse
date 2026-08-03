# Databricks notebook source
from pyspark.sql import functions as F

bronze_orders = spark.table("adb_customer360_dev.bronze.orders")

print("Bronze rows:", bronze_orders.count())
print(
    "Duplicate order IDs:",
    bronze_orders.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1: Read Bronze Order Data**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_orders_df = spark.table(
    "adb_customer360_dev.bronze.orders"
)

print(f"Bronze order count: {bronze_orders_df.count()}")

display(bronze_orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Profile Data**

# COMMAND ----------

# Profile null values

display(
    bronze_orders_df.select([
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in bronze_orders_df.columns
    ])
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Standardize Data**

# COMMAND ----------

# Standardize text columns

silver_orders_df = (
    bronze_orders_df
    .withColumn(
        "status",
        F.upper(
            F.trim(F.col("status"))
        )
    )
)

# COMMAND ----------

display(
    silver_orders_df
    .groupBy("status")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4: Calculate the Expected Order Amount**

# COMMAND ----------

# MAGIC %md
# MAGIC ![{03F125ED-58A7-4804-A77B-B573AEDD9BB6}_1785442432390.png](./{03F125ED-58A7-4804-A77B-B573AEDD9BB6}_1785442432390.png "{03F125ED-58A7-4804-A77B-B573AEDD9BB6}_1785442432390.png")

# COMMAND ----------

# Calculate the expected order amount

silver_orders_df = (
    silver_orders_df
    .withColumn(
        "expected_order_amount",
        F.round(
            (
                F.col("quantity")
                * F.col("unit_price")
                * (1 - F.col("discount_pct"))
            )
            + F.col("tax_amount"),
            2
        )
    )
)

# COMMAND ----------

display(
    silver_orders_df.select(
        "order_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "tax_amount",
        "order_amount",
        "expected_order_amount"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5: Build the Validation Rules**

# COMMAND ----------

# Define the complete order validation condition

valid_order_condition = (
    F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
    & F.col("order_timestamp").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("unit_price") > 0)
    & (F.col("discount_pct") >= 0)
    & (F.col("discount_pct") <= 1)
    & F.col("status").isin(
        "COMPLETED",
        "PENDING",
        "CANCELLED",
        "REFUNDED"
    )
    & (
        F.abs(
            F.col("order_amount")
            - F.col("expected_order_amount")
        ) <= 0.01
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6: Split Valid and Rejected Records**

# COMMAND ----------

# Split valid and rejected orders

valid_orders_df = silver_orders_df.filter(
    valid_order_condition
)

rejected_orders_df = silver_orders_df.filter(
    ~valid_order_condition
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7: Validate the Split**

# COMMAND ----------

print(f"Bronze records: {bronze_orders_df.count()}")

print(f"Valid records: {valid_orders_df.count()}")

print(f"Rejected records: {rejected_orders_df.count()}")

print(
    "Reconciliation passed:",
    bronze_orders_df.count()
    ==
    valid_orders_df.count()
    + rejected_orders_df.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8: Add Rejection Reasons**

# COMMAND ----------

# Add detailed rejection reasons for audit and troubleshooting

rejected_orders_df = (
    rejected_orders_df
    .withColumn(
        "reject_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("customer_id").isNull(),
                F.lit("Missing Customer ID")
            ),
            F.when(
                F.col("product_id").isNull(),
                F.lit("Missing Product ID")
            ),
            F.when(
                F.col("order_timestamp").isNull(),
                F.lit("Missing Order Timestamp")
            ),
            F.when(
                F.col("quantity") <= 0,
                F.lit("Invalid Quantity")
            ),
            F.when(
                F.col("unit_price") <= 0,
                F.lit("Invalid Unit Price")
            ),
            F.when(
                (F.col("discount_pct") < 0)
                | (F.col("discount_pct") > 1),
                F.lit("Invalid Discount")
            ),
            F.when(
                ~F.col("status").isin(
                    "COMPLETED",
                    "PENDING",
                    "CANCELLED",
                    "REFUNDED"
                ),
                F.lit("Invalid Status")
            ),
            F.when(
                F.abs(
                    F.col("order_amount")
                    - F.col("expected_order_amount")
                ) > 0.01,
                F.lit("Order Amount Mismatch")
            )
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 9: Deduplicate Valid Orders**

# COMMAND ----------

# Keep the latest record for each order ID

order_dedup_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file_modified_at").desc()
    )
)

silver_orders_df = (
    valid_orders_df
    .withColumn(
        "_row_number",
        F.row_number().over(order_dedup_window)
    )
    .filter(F.col("_row_number") == 1)
    .drop(
        "_row_number",
        "expected_gross_amount",
        "expected_discounted_amount",
        "expected_tax_amount",
        "expected_order_amount"
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC prepare the rejected table

# COMMAND ----------

# Prepare rejected order records for auditing

silver_order_rejected_df = (
    rejected_orders_df
    .withColumn(
        "_rejected_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 10: Write the Silver Tables**

# COMMAND ----------

from delta.tables import DeltaTable

silver_table = "adb_customer360_dev.silver.orders"

if not spark.catalog.tableExists(silver_table):
    (
        silver_orders_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    print("Silver orders table created successfully.")

else:
    silver_delta = DeltaTable.forName(
        spark,
        silver_table
    )

    (
        silver_delta.alias("t")
        .merge(
            silver_orders_df.alias("s"),
            "t.order_id = s.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver orders table merged successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Write rejected order records for data quality review

# COMMAND ----------

# Write rejected order records for data quality review

(
    silver_order_rejected_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "adb_customer360_dev.silver.order_rejected"
    )
)

print("Rejected order table created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 11: Validate the Silver Results**

# COMMAND ----------

silver_count = spark.table(
    "adb_customer360_dev.silver.orders"
).count()

rejected_count = spark.table(
    "adb_customer360_dev.silver.order_rejected"
).count()

duplicate_count = (
    spark.table("adb_customer360_dev.silver.orders")
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Silver order count: {silver_count}")
print(f"Rejected order count: {rejected_count}")
print(f"Duplicate order IDs remaining: {duplicate_count}")

# COMMAND ----------

# Review rejected orders by reason

display(
    spark.table(
        "adb_customer360_dev.silver.order_rejected"
    )
    .groupBy("reject_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Quality Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT order_id) AS distinct_order_ids
# MAGIC FROM adb_customer360_dev.silver.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY adb_customer360_dev.silver.orders;