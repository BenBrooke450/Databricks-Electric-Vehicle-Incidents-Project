# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration/"

# COMMAND ----------

dbutils.fs.ls("/mnt/cityoflondoncrime/silver/")

# COMMAND ----------

from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank

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

electric_cars_df = electric_cars_df \
                    .filter((col("electric_vehicle_type") == "Battery Electric Vehicle (BEV)")).withColumn("make_and_model", concat(upper(col("Make")),lit(" "),upper(col("Model"))))

# COMMAND ----------

electric_cars_df = electric_cars_df \
                    .filter(~col("make_and_model").isin("FORD TRANSIT","FORD FOCUS","MINI COUNTRYMAN","MINI HARDTOP","FIAT 500"))

# COMMAND ----------

casualty_statistics_2023_df \
            .withColumn("generic_make_model", upper(col("generic_make_model")))

# COMMAND ----------

brands_of_electric_cars = set(x[0] for x in electric_cars_df.select("make_and_model").collect())

# COMMAND ----------

casualty_statistics_2023_df = casualty_statistics_2023_df \
            .withColumn("electric_vehicle_type", when(col("generic_make_model").isin(brands_of_electric_cars), lit("Y")).otherwise("N"))

# COMMAND ----------

casualty_electric_cars_statistics_2023 = casualty_statistics_2023_df \
                            .filter(col("electric_vehicle_type") == "Y")

# COMMAND ----------

casualty_electric_cars_statistics_2023.display()

# COMMAND ----------

casualty_non_electric_cars_statistics_2023 = casualty_statistics_2023_df \
                            .filter(col("electric_vehicle_type") == "N")

# COMMAND ----------

casualty_statistics_2023_df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save('/mnt/cityoflondoncrime/gold/presentation_road_casualty_statistics_2023_data')

# COMMAND ----------

casualty_electric_cars_statistics_2023.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save('/mnt/cityoflondoncrime/gold/presentation_electric_cars_casualty_statistics_2023_data')

# COMMAND ----------

casualty_non_electric_cars_statistics_2023.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save('/mnt/cityoflondoncrime/gold/presentation_non_electric_cars_casualty_statistics_2023_data')
