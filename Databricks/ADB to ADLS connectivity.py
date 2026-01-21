# Databricks notebook source
# DBTITLE 1,Step to be followed
# MAGIC %md
# MAGIC - configuration setup between adls & adb 
# MAGIC - povide adls name/endpoint = https://adlsadbtraining2026.dfs.core.windows.net/
# MAGIC - auth method key/sas/serviceprinciple = 
# MAGIC - access key = /8v1xyoo0RkGSaCcmw7SalZV77nT8gix3eps7yiMtSjmNiSjGqLSQ4yWV2jopQ4/qOX5hCvH31fL+AStl7d39w==
# MAGIC - SAS = sv=2024-11-04&ss=bfqt&srt=sco&sp=rwlacpx&se=2026-01-30T15:18:22Z&st=2026-01-20T07:03:22Z&spr=https&sig=I10PE9ZV1fIfmtWuq8LxbJoAKf3Zp56oBEhToal5bx8%3D
# MAGIC - Serivce Principle = Assigning RBAC
# MAGIC - Define the path =
# MAGIC **abfss://container_name@storage.endpoints** abfss://raw@adlsadbtraining2026.dfs.core.windows.net/api/2026/jan/19
# MAGIC - read data from define path

# COMMAND ----------

adlsname = dbutils.widgets.get("adls")
# dbutils.help()
print(adlsname)

# COMMAND ----------

# DBTITLE 1,storage configuration with databricks
spark.conf.set(f'fs.azure.account.key.{adlsname}.dfs.core.windows.net', '')

# COMMAND ----------

# DBTITLE 1,list all the file from storage
dbutils.fs.ls(f"abfss://raw@{adlsname}.dfs.core.windows.net/api/2026/jan/19")

# COMMAND ----------

path = f'abfss://raw@{adlsname}.dfs.core.windows.net/api/2026/jan/19/CustomerSourceDemo.csv'
print(path)
df=spark.read.format("csv").option("header", True).option("inferSchema", True).load(path)
display(df)

# COMMAND ----------

# DBTITLE 1,function in databricks & spark
from pyspark.sql.functions import current_timestamp, max, min, sum, avg
# df.printSchema()
# df.count()
# df.select("FirstName", "CompanyName", "Phone").display()
# df = df.drop("FirstName", "suffix", "CompanyName", "Phone")
# df.show(3) #--> spark function
# display(df)  #--> databricks function
# df = df.withColumnRenamed("Phone", "contact")
df = df.withColumn("insert_date", current_timestamp()).withColumnRenamed("Phone", "contact")

# # df.select("Gender").distinct().count()
# df.agg(max("Salary"), min("Salary"), sum("Salary"), avg("Salary")).display()
# df.groupby("Gender").agg(max("Salary"), min("Salary"), sum("Salary"), avg("Salary")).display()
# display(df['gender'])
display(df)  #--> databricks function
# FullName = FirstName+MiddleName+LastName
# df.select(df["CustomerID"].cast("string")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC - read file from adls
# MAGIC - cleaning -> renaming column, insert date
# MAGIC -  cast
# MAGIC - spark - sql -> tempview (spark session/cluster live)
# MAGIC - write -> file/table/view

# COMMAND ----------

df.createOrReplaceGlobalTempView("df2")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert into customers
# MAGIC select * from global_temp.df2

# COMMAND ----------

# DBTITLE 1,write with spark
dest_path = f'abfss://refined@{adlsname}.dfs.core.windows.net/api/2026/jan/21'
df.write.mode("overwrite").save(dest_path)  #--> load into file
df.write.mode("append").saveAsTable("customerdata")

# COMMAND ----------

# dbutils.fs.ls(dest_path)
df2=spark.read.format("delta").load(dest_path)
display(df2)

# COMMAND ----------

