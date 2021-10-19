# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://snowflake@kkgatechstorage.blob.core.windows.net",
  mount_point = "/mnt/azureblob/snowflake",
  extra_configs = {"fs.azure.account.key.kkgatechstorage.blob.core.windows.net":dbutils.secrets.get(scope = "test", key = "storageAccessKey")})

# COMMAND ----------

dbutils.fs.ls("/mnt/azureblob/snowflake")

# COMMAND ----------

df = spark.read.csv("/mnt/azureblob/snowflake/snowflakeFile.txt",header="true")
df.show()
