# Databricks notebook source
dbutils.fs.mount(
  source = dbutils.secrets.get(scope = "test", key = "storageSource"),
  mount_point = "/mnt/azureblob/snowflake1",
  extra_configs = {"fs.azure.account.key.kkgatechstorage.blob.core.windows.net":dbutils.secrets.get(scope = "test", key = "storageAccessKey")})

# COMMAND ----------

dbutils.fs.ls("/mnt/azureblob/snowflake")

# COMMAND ----------

df = spark.read.csv("/mnt/azureblob/snowflake/snowflakeFile.txt",header="true")
df.show()
