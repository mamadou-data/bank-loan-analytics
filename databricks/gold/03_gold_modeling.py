# Databricks notebook source

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# ----------------------------------------------------
# 1. Set context
# ----------------------------------------------------
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA gold")

# ----------------------------------------------------
# 2. Read Silver table
# ----------------------------------------------------
df_silver = spark.table("main.silver.bank_loan_clean")

# ----------------------------------------------------
# Dimension: Education
# ----------------------------------------------------

window_spec = Window.orderBy("education")

df_dim_education = (
    df_silver
    .select("education", "education_level")
    .dropDuplicates()
    .withColumn("education_key", row_number().over(window_spec))
)

(
    df_dim_education.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("dim_education")
)

# ----------------------------------------------------
# Dimension: Geography
# ----------------------------------------------------

window_spec_geo = Window.orderBy("zip_code")

df_dim_geography = (
    df_silver
    .select("zip_code")
    .dropDuplicates()
    .withColumn("geography_key", row_number().over(window_spec_geo))
)

(
    df_dim_geography.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("dim_geography")
)

# ----------------------------------------------------
# Dimension: Customer Profile
# ----------------------------------------------------

window_spec_customer = Window.orderBy(
    "age","family","online","creditcard","securities_account","cd_account"
)

df_dim_customer = (
    df_silver
    .select(
        "age",
        "experience",
        "family",
        "online",
        "creditcard",
        "securities_account",
        "cd_account"
    )
    .dropDuplicates()
    .withColumn("customer_profile_key", row_number().over(window_spec_customer))
)

(
    df_dim_customer.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("dim_customer_profile")
)

# ---------------------------------------------------

row_count = df_silver.count()

if row_count == 0:
    raise Exception("Gold Build Aborted: Silver table is empty.")

# ----------------------------------------------------
# Fact Table: Loans
# ----------------------------------------------------

dim_edu = spark.table("main.gold.dim_education")
dim_geo = spark.table("main.gold.dim_geography")
dim_customer = spark.table("main.gold.dim_customer_profile")

df_fact = (
    df_silver
    .join(dim_edu, ["education", "education_level"], "left")
    .join(dim_geo, ["zip_code"], "left")
    .join(
        dim_customer,
        ["age","experience","family","online","creditcard","securities_account","cd_account"],
        "left"
    )
    .select(
        col("education_key"),
        col("geography_key"),
        col("customer_profile_key"),
        col("income"),
        col("ccavg"),
        col("mortgage"),
        col("personal_loan")
    )
)

(
    df_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("fact_loans")
)