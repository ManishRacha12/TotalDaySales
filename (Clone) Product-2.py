# Databricks notebook source
# Read CSV data from DBFS into a DataFrame
df = spark.read.option("header", "true").csv("dbfs:/mnt/manish/products.csv")

# Write DataFrame into a Delta table, overwriting existing data
df.write.format("delta").mode("overwrite").saveAsTable("products")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer
