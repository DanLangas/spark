# Databricks notebook source
df = spark.read.load("/databricks-datasets/airlines/part-00000",format="csv",sep=",",inferSchema="true",header="true" )

# COMMAND ----------

df.write.parquet("airlines.parquet")

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/airlines/

# COMMAND ----------

airlines = (spark.read
            .option("header",True)
            .option("inferSchema",True)
            .option("delimiter",",")
            .csv("dbfs:/databricks-datasets/airlines/part-00000"))

# COMMAND ----------

display(airlines.limit(15))

# COMMAND ----------

airlines.write.mode("overwrite").format("delta").save("dbfs:/airlines/")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/airlines/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/airlines/_delta_log/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS airlines_delta_table;
# MAGIC CREATE TABLE airlines_delta_table USING DELTA LOCATION "dbfs:/airlines/";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as count FROM airlines_delta_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM airlines_delta_table WHERE Month = '10'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE airlines_delta_table SET Dest = 'San Francisco' WHERE Dest = 'SFO'

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/airlines/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/airlines/_delta_log/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel
# MAGIC DESCRIBE HISTORY airlines_delta_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Return count of rows where Dest = 'SFO' in current version that is version 2
# MAGIC SELECT COUNT(*) FROM airlines_delta_table WHERE Dest = 'SFO'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Return count of rows where Dest = 'SFO' in version 1
# MAGIC SELECT COUNT(*) FROM airlines_delta_table VERSION AS OF 1 WHERE Dest = 'SFO'

# COMMAND ----------


