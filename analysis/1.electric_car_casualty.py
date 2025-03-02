# Databricks notebook source
# MAGIC %run "../includes/configuration/"

# COMMAND ----------

from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank
from pyspark.sql.window import Window

# COMMAND ----------

df_electric_cars_casualty = spark.read.format("delta").load(f"{gold_folder_path}/presentation_electric_cars_casualty_statistics_2023_data")

# COMMAND ----------

df_electric_cars_casualty = df_electric_cars_casualty. \
        withColumn("generic_make_model_Rank", row_number().over(Window.partitionBy("generic_make_model").orderBy("generic_make_model")))

# COMMAND ----------

df_electric_cars_casualty.display()
