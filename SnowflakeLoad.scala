// Databricks notebook source
import org.apache.spark.sql.DataFrame


// COMMAND ----------

// Use secrets DBUtil to get Snowflake credentials.
val user = dbutils.secrets.get(scope="test",key =  "snowflakeUser")
val password = dbutils.secrets.get(scope="test",key =  "snowflakePassword")
val sfUrl = dbutils.secrets.get(scope="test",key =  "snowflakeURL")

val options = Map(
  "sfUrl" -> sfUrl,
  "sfUser" -> user,
  "sfPassword" -> password,
  "sfDatabase" -> "DEMO_DB",
  "sfSchema" -> "PUBLIC",
  "sfWarehouse" -> "COMPUTE_WH"
)

// COMMAND ----------

//spark.range(5).show()

// COMMAND ----------

// Generate a simple dataset containing five values and write the dataset to Snowflake.
spark.range(5).write
  .format("snowflake")
  .options(options)
  .option("dbtable", "DEMO_DB")
  .save()

// COMMAND ----------

val df: DataFrame = spark.read
  .format("snowflake")
  .options(options)
  .option("dbtable", "DEMO_DB") //DEMO_DB is the table name
  .load().sort("ID")

//display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.read.csv("/mnt/azureblob/snowflake/snowflakeFile.txt",header="true")
// MAGIC #df.show()

// COMMAND ----------

//Read DF using Spark
val df = spark.read.option("header",true)
   .csv("/mnt/azureblob/snowflake/snowflakeFile.txt")

//df.show()


// COMMAND ----------

df.write
  .format("snowflake")
  .options(options)
  .option("dbtable", "SNOWFLAKE_FILE")
  .mode("APPEND") //To append data. If you need to create a table, then comment this line out
  .save()
