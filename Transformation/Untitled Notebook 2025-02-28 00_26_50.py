# Databricks notebook source
# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %run "../Includes/configuration/"

# COMMAND ----------

from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank

# COMMAND ----------

dbutils.fs.ls("/mnt/cityoflondoncrime/silver/")

# COMMAND ----------

electric_cars_df = spark.read.format("delta").load(f"{silver_folder_path}/processed_electric_vehicle_population_data")

# COMMAND ----------

casualty_statistics_2023_df = spark.read.format("delta").load(f"{silver_folder_path}/processed_road_casualty_statistics_2023_data")

# COMMAND ----------

casualty_statistics_2023_df = casualty_statistics_2023_df.na.drop()

# COMMAND ----------

casualty_statistics_2023_df = casualty_statistics_2023_df \
                .filter(col("generic_make_model") != "-1")

# COMMAND ----------

electric_cars_df = electric_cars_df.withColumn("make_and_model", 
                            concat(upper(col("Make")),lit(" "),upper(col("Model"))))

# COMMAND ----------

casualty_statistics_2023_df \
            .withColumn("generic_make_model", upper(col("generic_make_model")))

# COMMAND ----------

#brands_of_electric_cars = \
            #set(n[0] for n in electric_cars_df.select('make_and_model').collect())

# COMMAND ----------

#casualty_statistics_2023_df.filter(col("generic_make_model") in brands_of_electric_cars)

# COMMAND ----------

outer_df = casualty_statistics_2023_df \
            .join(electric_cars_df,
                  casualty_statistics_2023_df.generic_make_model == electric_cars_df.make_and_model,"inner")

# COMMAND ----------

outer_df.display()
