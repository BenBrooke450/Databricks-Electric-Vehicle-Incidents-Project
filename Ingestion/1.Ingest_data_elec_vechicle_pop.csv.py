# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Electric_Vehicle_Population_Data.csv file
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/configuration" 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DataType

# COMMAND ----------

schema_car = StructType(fields=[StructField('VIN (1-10)', StringType(), True),
                StructField('County', StringType(), True),
                StructField('City', StringType(), True),
                StructField('State', StringType(), True),
                StructField('Postal Code', IntegerType(), True),
                StructField('Model Year', IntegerType(), True),
                StructField('Make', StringType(), True),
                StructField('Model', StringType(), True),
                StructField('Electric Vehicle Type', StringType(), True),
                StructField('Clean Alternative Fuel Vehicle (CAFV) Eligibility', StringType(), True),
                StructField('Electric Range', IntegerType(), True),
                StructField('Base MSRP', IntegerType(), True),
                StructField('Legislative District', IntegerType(), True),
                StructField('DOL Vehicle ID', IntegerType(), True),
                StructField('Vehicle Location', StringType(), True),
                StructField('Electric Utility', StringType(), True),
                StructField('2020 Census Tract', IntegerType(), True)])

# COMMAND ----------

df = spark.read \
    .option("header", True) \
    .schema(schema_car) \
    .csv("/mnt/bronze/Electric_Vehicle_Population_Data.csv")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_renamed = df.withColumnRenamed("Postal Code", "postal_code") \
                        .withColumnRenamed("Model Year", "model_year")\
                        .withColumnRenamed("Electric Vehicle Type", "electric_vehicle_type") \
                        .withColumnRenamed("Clean Alternative Fuel Vehicle (CAFV) Eligibility", "clean_alternative_fuel_vehicle_eligibility") \
                        .withColumnRenamed("Electric Range", "electric_range") \
                        .withColumnRenamed("Base MSRP", "base_msrp") \
                        .withColumnRenamed("Legislative District", "legislative_district") \
                        .withColumnRenamed("DOL Vehicle ID", "dol_vehicle_id") \
                        .withColumnRenamed("Electric Utility", "electric_utility") \
                        .withColumnRenamed("VIN (1-10)", "vin_1_10")

# COMMAND ----------

df_renamed_dropped = df_renamed.drop("Vehicle Location", "2020 Census Tract")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df = df_renamed_dropped.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

df.write.format("delta") \
            .mode("overwrite") \
            .save('/mnt/cityoflondoncrime/silver/processed_electric_vehicle_population_data')
