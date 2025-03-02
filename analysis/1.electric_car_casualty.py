# Databricks notebook source
# MAGIC %run "../includes/configuration/"

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank, avg, round
from pyspark.sql.window import Window

# COMMAND ----------

df_electric_cars_casualty = spark.read.format("delta").load(f"{gold_folder_path}/presentation_electric_cars_casualty_statistics_2023_data")

# COMMAND ----------

df_electric_cars_casualty = df_electric_cars_casualty. \
        withColumn("generic_make_model_Rank", row_number().over(Window.partitionBy("generic_make_model").orderBy("generic_make_model")))

# COMMAND ----------

df_electric_cars_casualty = df_electric_cars_casualty \
        .withColumn("Average_age_over_type", round(avg(col("age_of_driver")).over(Window.partitionBy("generic_make_model"))))

# COMMAND ----------

df_electric_cars_casualty \
            .groupBy("generic_make_model") \
            .agg(round((count("generic_make_model")/df_electric_cars_casualty.count())*100,3).alias("Percentage_of_total_records")).display()

# COMMAND ----------

df_electric_cars_casualty.display()

# COMMAND ----------

df_electric_cars_casualty.display()
