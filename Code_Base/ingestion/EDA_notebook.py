# Databricks notebook source
from pyspark.sql import functions as fn
from pyspark.sql.window import Window
from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

input_path = "file:/Workspace/Users/samvanderheijden01@gmail.com/MetyisCase/Sales_Data/raw/*.csv"

# COMMAND ----------

#ingest data and create timestamp column
df = spark.read.csv(input_path, header=True, inferSchema=True)

# COMMAND ----------

#Check for duplicates in the data
df.groupBy(*df.columns).agg((count("*")).alias('duplicate_count')).filter("duplicate_count > 1").display()

# COMMAND ----------

# Check for NaN values in the dataframe

df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()

#No NaN values found so we do not need to adress this

# COMMAND ----------

# Check for Null values in the dataframe

df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# 900 Order Id's found. These rows should be dropped

# COMMAND ----------

# Count the number of rows per product
product_counts = df.groupBy("Product").count()

display(product_counts)

#Not a good column for partitioning on due to heavy data skew and many small partitions

# COMMAND ----------

from pyspark.sql.functions import month, year

# Count the number of rows per month
monthly_counts = df.withColumn("Order Date", fn.to_timestamp(col("Order Date"), "MM/dd/yy HH:mm")) \
                    .groupBy(year("Order Date"), month("Order Date").alias("month")).count().orderBy("month")

display(monthly_counts)

#Altough there is still data skew the combination of year and month columns should be a good partitioning column. Users of the table will probably want to query by month or year often which should lead to improved query performance by using this partition strategy and reduced IO when ingestion data
