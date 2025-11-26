# Databricks notebook source
# -----------------------------------------------------------------------
# Databricks / Delta Lake ETL Pipeline Sample Notebook
# Author: yujimimu-ops
# This notebook demonstrates:
#  - Loading raw CSV
#  - Cleaning & transforming data with PySpark
#  - Writing to Delta Lake (Bronze → Silver)
#  - Auto Loader example
# -----------------------------------------------------------------------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# -----------------------------------------------------------------------
# 1. Load Raw Data (Bronze)
# -----------------------------------------------------------------------

raw_path = "/mnt/raw/sample_sales.csv"

df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(raw_path)
)

display(df_raw)

# COMMAND ----------

# -----------------------------------------------------------------------
# 2. Data Cleaning & Transformations (Silver)
# -----------------------------------------------------------------------

df_clean = (
    df_raw
    .withColumn("sales_amount", col("sales_amount").cast("double"))
    .withColumn("sales_date", to_date(col("sales_date"), "yyyy-MM-dd"))
    .withColumn("sales_month", date_format(col("sales_date"), "yyyy-MM"))
    .dropDuplicates()
    .filter(col("sales_amount") > 0)
)

display(df_clean)

# COMMAND ----------

# -----------------------------------------------------------------------
# 3. Write Data to Delta Lake (Bronze → Silver)
# -----------------------------------------------------------------------

bronze_table_path = "/mnt/delta/bronze/sales"
silver_table_path = "/mnt/delta/silver/sales"

# Bronze
df_raw.write.format("delta").mode("overwrite").save(bronze_table_path)

# Silver
df_clean.write.format("delta").mode("overwrite").save(silver_table_path)

# COMMAND ----------

# -----------------------------------------------------------------------
# 4. Auto Loader Example (Optional)
# -----------------------------------------------------------------------

# Demonstrates Databricks AutoLoader (cloud_files)
# Real pipelines use this for incremental ingestion

auto_bronze_path = "/mnt/delta/autoloader/bronze"

(
    spark.readStream.format("cloud_files")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .load("/mnt/raw/incremental/")
    .writeStream.format("delta")
    .option("checkpointLocation", f"{auto_bronze_path}/_checkpoint")
    .outputMode("append")
    .start(auto_bronze_path)
)

# COMMAND ----------

# -----------------------------------------------------------------------
# 5. Create Delta Table (SQL)
# -----------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver_sales
USING DELTA
LOCATION '{silver_table_path}'
""")

# COMMAND ----------

# -----------------------------------------------------------------------
# 6. Aggregate Example (Gold Layer)
# -----------------------------------------------------------------------

df_monthly = (
    df_clean.groupBy("sales_month")
    .agg(sum("sales_amount").alias("monthly_sales"))
    .orderBy("sales_month")
)

display(df_monthly)

# COMMAND ----------

# -----------------------------------------------------------------------
# END OF NOTEBOOK
# -----------------------------------------------------------------------
print("ETL Pipeline Completed Successfully")
