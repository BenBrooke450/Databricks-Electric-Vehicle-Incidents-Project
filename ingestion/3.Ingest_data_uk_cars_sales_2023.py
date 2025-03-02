# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType

# COMMAND ----------

df = spark.read \
    .option("header", True) \
    .csv("/mnt/bronze/df_VEH0270.csv")

# COMMAND ----------

df = df.withColumnRenamed("BodyType", "bodytype") \
                        .withColumnRenamed("Make", "make")\
                        .withColumnRenamed("GenModel", "genmodel") \
                        .withColumnRenamed("Model", "model") \
                        .withColumnRenamed("EngineSizeDesc", "enginesizedesc") \
                        .withColumnRenamed("Fuel", "fuel")

# COMMAND ----------

df = df.select("bodytype","make","genmodel","model","enginesizedesc","fuel","2023")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df = df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save('/mnt/cityoflondoncrime/silver/processed_car_sales_statistics_2023_data')
