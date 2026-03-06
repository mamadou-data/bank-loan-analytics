# Databricks notebook source
# DBTITLE 1,Bronze ingestion (update metadata)

from pyspark.sql.functions import current_timestamp, lit
import uuid

# ----------------------------------------------------
# 1️⃣ Définir le contexte (OBLIGATOIRE en entreprise)
# ----------------------------------------------------
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA bronze")

file_path = "/Volumes/main/insurance_dev/raw/raw_data/bank_loan.csv"

# Lecture CSV
df_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .load(file_path)
)

# ---------------------------------------------------
# 🔥 Normalisation des noms de colonnes
# ---------------------------------------------------
def normalize_column(col_name):
    return (
        col_name.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )

for col in df_raw.columns:
    df_raw = df_raw.withColumnRenamed(col, normalize_column(col))

# ---------------------------------------------------
# Metadata Bronze
# ---------------------------------------------------
batch_id = str(uuid.uuid4())

df_bronze = (
    df_raw
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(file_path))
        .withColumn("batch_id", lit(batch_id))
)

# ---------------------------------------------------
# Write Delta Bronze
# ---------------------------------------------------
(
    df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("bank_loan_raw")
)

print("Bronze table successfully created.")

# COMMAND ----------

# df_raw.printSchema()
# df_raw.display()