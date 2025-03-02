# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration/"

# COMMAND ----------

dbutils.fs.ls("/mnt/cityoflondoncrime/silver/")

# COMMAND ----------

from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank,sum

# COMMAND ----------

UK_car_sales_df = spark.read.format("delta") \
                    .load(f"{silver_folder_path}/processed_car_sales_statistics_2023_data")

# COMMAND ----------

electric_cars_sales_df = UK_car_sales_df \
            .filter((col("BodyType")=="Cars") & (col("2023")>0) & (col("fuel")=="Battery electric")) \
            .drop("EngineSizeDesc")

# COMMAND ----------

electric_cars_sales_df = electric_cars_sales_df.groupBy("GenModel").agg(sum(col("2023")).alias("cars_sold")).orderBy("GenModel")

# COMMAND ----------

electric_cars_sales_df = electric_cars_sales_df.filter(~col("GenModel").contains("MISSING"))

# COMMAND ----------

electric_cars_sales_df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save('/mnt/cityoflondoncrime/gold/presentation_electric_cars_sales_2023_df')
