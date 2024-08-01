# Databricks notebook source
from pyspark.sql import functions as fn
from pyspark.sql.window import Window
from pyspark.sql.functions import isnan, when, count, col

# COMMAND ----------

input_path = "file:/Workspace/Users/samvanderheijden01@gmail.com/MetyisCase/Sales_Data/raw/*.csv"

output_path = "file:/Workspace/Users/samvanderheijden01@gmail.com/MetyisCase/Sales_Data/cleansed/"

# COMMAND ----------

# Load data from the .csv files by using the * operator. 
# Order Date column is converted to a timestamp.
df = spark.read.csv(input_path, header=True, inferSchema=True).withColumn("Order Date", fn.to_timestamp(col("Order Date"), "MM/dd/yy HH:mm"))

# COMMAND ----------

# Remove Null values in Order Id column (as seen from EDA notebook)
df = df.filter(col("Order ID").isNotNull())

# COMMAND ----------

#Create window function for deduping data. I do not understand why the assignment asks for this method since dropDuplicates has the same effect.
def deduplicate_data(df, timeColumn):
    windowSpec = Window.partitionBy(*df.columns).orderBy(timeColumn)
    outputDf = df.withColumn("row_number", fn.row_number().over(windowSpec)).where(col("row_number") == 1).drop("row_number")
    return outputDf

#create Bool which tests existence of duplicate ID's
def check_duplicates(df):
    duplicateCount = df.groupBy(*df.columns).count().filter(col("count") > 1).count()
    return duplicateCount == 0

# COMMAND ----------

#Execute the deduplication function in a try catch block
try:
    df = deduplicate_data(df, "Order Date")
    
    if not check_duplicates(df):
        raise ValueError("Duplicates found after deduplication.")
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

# Create the columns for partitioning the data on year and month
df = df.withColumn("year", fn.year("Order Date")) \
                    .withColumn("month", fn.month("Order Date"))

# COMMAND ----------

# Write DataFrame to Parquet with year and month partitions
try:
    df.write.partitionBy('year', 'month').format('parquet').mode('overwrite').save(output_path)

    # Verify if data is written by trying to read the first partition
    test_read = spark.read.parquet(f"{output_path}/year=*/month=*").limit(1)
    
    if test_read.count() == 0:
        raise ValueError("No data found after write operation.")
except Exception as e:
    print(f"An error occurred during write or verification: {e}")
