# Databricks notebook source
# MAGIC %md
# MAGIC **Step 1: Read Bronze Customer Data**

# COMMAND ----------

from pyspark.sql import functions as F

bronze_customers_df = spark.table(
    "adb_customer360_dev.bronze.customers"
)

print(f"Bronze customer count: {bronze_customers_df.count()}")

display(bronze_customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Define Data Quality Rules**

# COMMAND ----------

email_pattern = (
    r"^[A-Za-z0-9._%+-]+@"
    r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

valid_statuses = ["ACTIVE", "INACTIVE"]
valid_provinces = ["AB", "BC", "MB", "ON", "QC"]

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Standardize Data Before Validation**

# COMMAND ----------

standardized_customer_df = (
    bronze_customers_df

    # Trim leading and trailing spaces
    .withColumn(
        "first_name",
        F.trim(F.col("first_name"))
    )
    .withColumn(
        "last_name",
        F.trim(F.col("last_name"))
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "city",
        F.initcap(F.trim(F.col("city")))
    )

    # Standardize province codes
    .withColumn(
        "province",
        F.upper(F.trim(F.col("province")))
    )

    # Standardize customer status
    .withColumn(
        "status",
        F.upper(F.trim(F.col("status")))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4: Add Data Quality Flags**

# COMMAND ----------

validated_customer_df = (
    standardized_customer_df

    # Validate required customer ID
    .withColumn(
        "_is_customer_id_valid",
        F.col("customer_id").isNotNull()
    )

    # Validate email address
    .withColumn(
        "_is_email_valid",
        F.col("email").isNotNull()
        & F.col("email").rlike(email_pattern)
    )

    # Validate province code
    .withColumn(
        "_is_province_valid",
        F.col("province").isin(valid_provinces)
    )

    # Validate customer status
    .withColumn(
        "_is_status_valid",
        F.col("status").isin(valid_statuses)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5: Create the Rejection Reason**

# COMMAND ----------

validated_customer_df = validated_customer_df.withColumn(
    "reject_reason",
    F.concat_ws(
        "; ",
        F.when(
            ~F.col("_is_customer_id_valid"),
            F.lit("Missing Customer ID")
        ),
        F.when(
            ~F.col("_is_email_valid"),
            F.when(
                F.col("email").isNull(),
                F.lit("Missing Email")
            ).otherwise(
                F.lit("Invalid Email")
            )
        ),
        F.when(
            ~F.col("_is_province_valid"),
            F.lit("Invalid Province")
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

valid_customer_df = (
    validated_customer_df
    .filter(
        F.col("_is_customer_id_valid")
        & F.col("_is_email_valid")
        & F.col("_is_province_valid")
        & F.col("_is_status_valid")
    )
)

rejected_customer_df = (
    validated_customer_df
    .filter(
        ~(
            F.col("_is_customer_id_valid")
            & F.col("_is_email_valid")
            & F.col("_is_province_valid")
            & F.col("_is_status_valid")
        )
    )
)

# COMMAND ----------

bronze_count = bronze_customers_df.count()
valid_count = valid_customer_df.count()
rejected_count = rejected_customer_df.count()

print(f"Bronze records: {bronze_count}")
print(f"Valid records: {valid_count}")
print(f"Rejected records: {rejected_count}")
print(
    f"Reconciliation passed: "
    f"{bronze_count == valid_count + rejected_count}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7: Deduplicate Valid Customers**

# COMMAND ----------

from pyspark.sql.window import Window

customer_dedup_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file_modified_at").desc()
    )
)

silver_customer_df = (
    valid_customer_df
    .withColumn(
        "_row_number",
        F.row_number().over(customer_dedup_window)
    )
    .filter(F.col("_row_number") == 1)
    .drop(
        "_row_number",
        "_is_customer_id_valid",
        "_is_email_valid",
        "_is_province_valid",
        "_is_status_valid",
        "reject_reason"
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 8: Write the Silver Tables**

# COMMAND ----------

silver_customer_rejected_df = (
    rejected_customer_df
    .drop(
        "_is_customer_id_valid",
        "_is_email_valid",
        "_is_province_valid",
        "_is_status_valid"
    )
    .withColumn(
        "_rejected_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# Write cleansed customer records to the Silver layer

(
    silver_customer_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("adb_customer360_dev.silver.customers")
)

print("Silver customer table created successfully.")

# COMMAND ----------

# Write rejected customer records for data quality review

(
    silver_customer_rejected_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "adb_customer360_dev.silver.customer_rejected"
    )
)

print("Rejected customer table created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 9: Validate the Silver Results**

# COMMAND ----------

silver_count = spark.table(
    "adb_customer360_dev.silver.customers"
).count()

rejected_count = spark.table(
    "adb_customer360_dev.silver.customer_rejected"
).count()

duplicate_count = (
    spark.table("adb_customer360_dev.silver.customers")
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_email_count = (
    spark.table("adb_customer360_dev.silver.customers")
    .filter(~F.col("email").rlike(email_pattern))
    .count()
)

print(f"Silver customer count: {silver_count}")
print(f"Rejected customer count: {rejected_count}")
print(f"Duplicate customer IDs remaining: {duplicate_count}")
print(f"Invalid emails remaining: {invalid_email_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC Validation to check which records are duplicated

# COMMAND ----------

# Validate the deduplication result

duplicate_summary = (
    valid_customer_df
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC