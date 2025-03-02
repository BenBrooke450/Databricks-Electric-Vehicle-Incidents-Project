# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Service Principal & Secrect keys
# MAGIC Unfortunately, because of my access in Azure, I am unable to use service principals and secret access keys, but I did want to demonstrate below my ability to do so if needed.
# MAGIC

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 


client_id = dbutils.secrets.get(scope = '####', key = '###')
tenant_id = dbutils.secrets.get(scope = '####', key = '####')
client_secret = dbutils.secrets.get(scope = '####', key = '####')


spark.conf.set("fs.azure.account.auth.type.####.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.####.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.####.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.####.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.####.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


display(dbutils.fs.ls("abfss://####@####.dfs.core.windows.net"))


display(spark.read.csv("abfss://####@####.dfs.core.windows.net/circuits.csv"))


