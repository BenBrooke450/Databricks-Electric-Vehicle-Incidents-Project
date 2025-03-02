# Databricks notebook source
# DBTITLE 1,Configure Access
# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

spark.conf.set(
  "fs.azure.account.key.cityoflondoncrime.blob.core.windows.net",
  "hXnMffy5t5WjiqWB1nZZpBnjnhrvvz4cGIhXSHnT9/CAAFuhkpg5ZmqvMWtcXTeGxDZHsXjSGdQ6+AStzjkEjg=="
)


# COMMAND ----------

# MAGIC %md
# MAGIC Unfortunately, I am unable to use secret access keys because of my type of subscription, but I did wish to demonstrate my ability to do so.

# COMMAND ----------

# DBTITLE 1,Secret Access key
#formula1dl_account_key = dbutils.secrets.get(scope = '######', key = '####')

# COMMAND ----------

#Checking what is mounted
dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

# DBTITLE 1,Testing
#Testing the configuration
df = spark.read.csv("dbfs:/mnt/bronze/2024-01-city-of-london-street.csv", header=True, inferSchema=True)
