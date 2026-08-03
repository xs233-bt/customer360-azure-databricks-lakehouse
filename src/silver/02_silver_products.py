# Databricks notebook source
# MAGIC %md
# MAGIC **Step 1: Read Bronze Product Data**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_products_df = spark.table(
    "adb_customer360_dev.bronze.products"
)

print(f"Bronze product count: {bronze_products_df.count()}")

display(bronze_products_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Define Data Quality Rules**

# COMMAND ----------

valid_product_statuses = ["ACTIVE", "INACTIVE"]
valid_categories = [
    "LAPTOP",
    "MONITOR",
    "KEYBOARD",
    "MOUSE",
    "PHONE",
    "TABLET"
]

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Standardize Product Data**

# COMMAND ----------

standardized_products_df = (
    bronze_products_df

    # Standardize product names
    .withColumn(
        "product_name",
        F.trim(F.col("product_name"))
    )

    # Standardize category values
    .withColumn(
        "category",
        F.upper(F.trim(F.col("category")))
    )

    # Standardize product status
    .withColumn(
        "status",
        F.upper(F.trim(F.col("status")))
    )

    # Cast price to a consistent decimal type
    .withColumn(
        "price",
        F.col("price").cast("decimal(12,2)")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4: Add Data Quality Flags**

# COMMAND ----------

validated_products_df = (
    standardized_products_df

    # Validate product ID
    .withColumn(
        "_is_product_id_valid",
        F.col("product_id").isNotNull()
        & (F.col("product_id") > 0)
    )

    # Validate product name
    .withColumn(
        "_is_product_name_valid",
        F.col("product_name").isNotNull()
        & (F.length(F.col("product_name")) > 0)
    )

    # Validate category
    .withColumn(
        "_is_category_valid",
        F.col("category").isin(valid_categories)
    )

    # Validate product price
    .withColumn(
        "_is_price_valid",
        F.col("price").isNotNull()
        & (F.col("price") > 0)
    )

    # Validate product status
    .withColumn(
        "_is_status_valid",
        F.col("status").isin(valid_product_statuses)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5: Create Rejection Reasons**

# COMMAND ----------

validated_products_df = validated_products_df.withColumn(
    "reject_reason",
    F.concat_ws(
        "; ",
        F.when(
            ~F.col("_is_product_id_valid"),
            F.lit("Invalid Product ID")
        ),
        F.when(
            ~F.col("_is_product_name_valid"),
            F.lit("Missing Product Name")
        ),
        F.when(
            ~F.col("_is_category_valid"),
            F.lit("Invalid Category")
        ),
        F.when(
            ~F.col("_is_price_valid"),
            F.lit("Invalid Price")
        ),
        F.when(
            ~F.col("_is_status_valid"),
            F.lit("Invalid Status")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6: Split Valid and Rejected Records**

# COMMAND ----------

all_product_rules_valid = (
    F.col("_is_product_id_valid")
    & F.col("_is_product_name_valid")
    & F.col("_is_category_valid")
    & F.col("_is_price_valid")
    & F.col("_is_status_valid")
)

valid_products_df = validated_products_df.filter(
    all_product_rules_valid
)

rejected_products_df = validated_products_df.filter(
    ~all_product_rules_valid
)

bronze_count = bronze_products_df.count()
valid_count = valid_products_df.count()
rejected_count = rejected_products_df.count()

print(f"Bronze records: {bronze_count}")
print(f"Valid records: {valid_count}")
print(f"Rejected records: {rejected_count}")
print(
    f"Reconciliation passed: "
    f"{bronze_count == valid_count + rejected_count}"
)

# COMMAND ----------

# Validate the deduplication result

duplicate_summary = (
    valid_products_df
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_summary)

# COMMAND ----------

# Write cleansed product records to the Silver layer

(
    valid_products_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "adb_customer360_dev.silver.products"
    )
)

print("Silver product table created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Final Verify

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Keep one latest valid record for each product ID

product_dedup_window = (
    Window
    .partitionBy("product_id")
    .orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file_modified_at").desc()
    )
)

silver_products_df = (
    valid_products_df
    .withColumn(
        "_row_number",
        F.row_number().over(product_dedup_window)
    )
    .filter(F.col("_row_number") == 1)
    .drop(
        "_row_number",
        "_is_product_id_valid",
        "_is_product_name_valid",
        "_is_category_valid",
        "_is_price_valid",
        "_is_status_valid",
        "reject_reason"
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# Validate the deduplicated DataFrame before writing

display(
    silver_products_df
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

print(f"Deduplicated product count: {silver_products_df.count()}")

# COMMAND ----------

# Overwrite the Silver product table with deduplicated records

(
    silver_products_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "adb_customer360_dev.silver.products"
    )
)

print("Silver product table rebuilt successfully.")