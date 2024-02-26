# Databricks notebook source
dbutils.fs.ls('/')
dbutils.fs.mkdirs('dbfs:/mnt/manish')

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from faker import Faker

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Generate and Write Data to CSV") \
    .getOrCreate()

# Generate fake data for other attributes
fake = Faker()
data = [(fake.word(), fake.random_number(digits=3), fake.random_element(elements=('Electronics', 'Clothing', 'Books', 'Toys'))) for _ in range(100)]

# Create a DataFrame with sequential product IDs and other attributes
df = spark.createDataFrame([(i+1,) + data[i] for i in range(100)], ["ProductId", "ProductName", "ProductPrice", "Category"])

# Write the DataFrame to a CSV file in DBFS
csv_path = "dbfs:/mnt/manish/products.csv"
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)


# Check if the file is written successfully
df_csv = spark.read.csv(csv_path, header=True)
df_csv.show(5)


# COMMAND ----------

#testing the git process
