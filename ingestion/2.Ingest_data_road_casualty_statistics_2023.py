# Databricks notebook source
# MAGIC %run "../includes/configuration" 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType

# COMMAND ----------

df = spark.read \
    .option("header", True) \
    .csv("/mnt/bronze/dft-road-casualty-statistics-vehicle-2023.csv")

# COMMAND ----------

schema_car = StructType(fields=[StructField('sex_of_driver', StringType(), True),
                StructField("age_of_driver", IntegerType(), True),
                StructField("age_of_vehicle", IntegerType(), True),
                StructField("generic_make_model", StringType(), True),
                StructField("accident_index", StringType(), True)])

# COMMAND ----------

df = df.select("sex_of_driver","age_of_driver","age_of_vehicle","generic_make_model","accident_index")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df = df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save('/mnt/cityoflondoncrime/silver/processed_road_casualty_statistics_2023_data')
