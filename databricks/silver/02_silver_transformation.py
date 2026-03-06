# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col, when, regexp_replace

spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA silver")

df_bronze = spark.table("main.bronze.bank_loan_raw")

df_silver = (
    df_bronze
    .drop("id")

    # Nettoyage des décimaux européens
    .withColumn("income", regexp_replace("income", ",", "."))
    .withColumn("ccavg", regexp_replace("ccavg", ",", "."))
    .withColumn("mortgage", regexp_replace("mortgage", ",", "."))

    # Typage strict
    .withColumn("age", col("age").cast("int"))
    .withColumn("experience", col("experience").cast("int"))
    .withColumn("income", col("income").cast("double"))
    .withColumn("zip_code", col("zip_code").cast("int"))
    .withColumn("family", col("family").cast("int"))
    .withColumn("ccavg", col("ccavg").cast("double"))
    .withColumn("education", col("education").cast("int"))
    .withColumn("mortgage", col("mortgage").cast("double"))

    # Booléens
    .withColumn("personal_loan", col("personal_loan").cast("boolean"))
    .withColumn("securities_account", col("securities_account").cast("boolean"))
    .withColumn("cd_account", col("cd_account").cast("boolean"))
    .withColumn("online", col("online").cast("boolean"))
    .withColumn("creditcard", col("creditcard").cast("boolean"))

    # Education label
    .withColumn(
        "education_level",
        when(col("education") == 1, "undergraduate")
        .when(col("education") == 2, "graduate")
        .when(col("education") == 3, "advanced_professional")
    )
)

(
    df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("bank_loan_clean")
)

# ----------------------------------------------------
# DATA QUALITY CHECKS
# ----------------------------------------------------

total_count = df_silver.count()

invalid_age = df_silver.filter(col("age") < 18).count()
invalid_income = df_silver.filter(col("income") <= 0).count()
invalid_education = df_silver.filter(~col("education").isin([1,2,3])).count()
null_income = df_silver.filter(col("income").isNull()).count()

print(f"Total rows: {total_count}")
print(f"Invalid age: {invalid_age}")
print(f"Invalid income: {invalid_income}")
print(f"Invalid education: {invalid_education}")
print(f"Null income: {null_income}")

# ----------------------------------------------------
# FAIL FAST LOGIC
# ----------------------------------------------------

if invalid_age > 0:
    raise Exception("Data Quality Error: Invalid age detected")

if invalid_income > 0:
    raise Exception("Data Quality Error: Invalid income detected")

if invalid_education > 0:
    raise Exception("Data Quality Error: Invalid education value detected")

if null_income > 0:
    raise Exception("Data Quality Error: Null income detected")


# ----------------------------------------------------
# CREATE DATA QUALITY LOG ENTRY
# ----------------------------------------------------

dq_log = spark.createDataFrame([
    Row(
        layer="silver",
        total_rows=total_count,
        invalid_age=invalid_age,
        invalid_income=invalid_income,
        invalid_education=invalid_education,
        null_income=null_income
    )
]).withColumn("log_timestamp", current_timestamp())

# ----------------------------------------------------
# WRITE LOG TABLE (Gold monitoring layer)
# ----------------------------------------------------

spark.sql("USE SCHEMA gold")

dq_log.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("data_quality_log")