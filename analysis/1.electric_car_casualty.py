# Databricks notebook source
# MAGIC %run "../includes/configuration/"

# COMMAND ----------

from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank, avg, round, sum, max
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_electric_cars_casualty = spark.read.format("delta").load(f"{gold_folder_path}/presentation_electric_cars_casualty_statistics_2023_data")

# COMMAND ----------

df_electric_cars_sales = spark.read.format("delta").load(f"{gold_folder_path}/presentation_electric_cars_sales_2023_df")

# COMMAND ----------

df_electric_cars_casualty = df_electric_cars_casualty. \
        withColumn("generic_make_model_Rank", row_number().over(Window.partitionBy("generic_make_model").orderBy("generic_make_model")))

# COMMAND ----------

df_electric_cars_casualty = df_electric_cars_casualty \
        .withColumn("Average_age_over_type", round(avg(col("age_of_driver")).over(Window.partitionBy("generic_make_model"))))

# COMMAND ----------

df_electric_cars_casualty \
            .groupBy("generic_make_model") \
            .agg(round((count("generic_make_model")/df_electric_cars_casualty.count())*100,3).alias("Percentage_of_casualty_records"))

# COMMAND ----------

df_electric_cars_sales = df_electric_cars_sales.withColumn("cars_sold",col("cars_sold").cast(IntegerType()))

# COMMAND ----------

df_electric_cars_sales = df_electric_cars_sales \
                .withColumn("Percentage_over_all",sum(col("cars_sold")).over(Window.partitionBy())) \
                .withColumn("Percentage_of_cars_cold",max(round(((col("cars_sold")/col("Percentage_over_all"))*100),2)).over(Window.partitionBy("GenModel").orderBy("GenModel"))).drop("Percentage_over_all")

# COMMAND ----------

df_electric_cars_sales.withColumn("top_cars_sold",
                                  row_number().over(Window.partitionBy().orderBy(col("cars_sold").desc()))) \
                                  .filter(col("top_cars_sold")<16).display()


# COMMAND ----------

df_electric_cars_casualty = df_electric_cars_casualty.filter(col("age_of_driver") != -1)

# COMMAND ----------

df_electric_cars_casualty.display()

# COMMAND ----------


