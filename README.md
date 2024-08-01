# MetyisCase
Technical Assesment for Metyis

## Introduction
In this repo the sales data for a retail company will be ingested from multiple .csv files. The files should be combined, cleaned, partitioned and written to the cleansed folder. Since I am using an old laptop I have chosen not to write the code in VS but in my personal Databricks dev workspace.

## Aproach
1. **Read Source Data**:
   - Use PySpark to read all CSV files from the source folder (`Sales_Data/raw`). Use the * operator to union all files on reading
   - Retail data should be a good opportunity to use streaming ingestion. However since we are receiving monthly batches this will not be the best option in this case

2. **Perform EDA on data**:
   - Create second notebooks to perform EDA on the data to find Nulls and duplicates
   
3. **Remove Nulls and Deduplicate Data**:
   - Use a Window function to deduplicate the data based on the Order ID, Product and Order Date columns.
   - Remove Nulls from Order ID column

4. **Create testing functions**:
   - Showcasing DRY coding I create some simple functions to check for duplicates and test the outcome using a try catch block

5. **Write to Cleansed Folder**:
   - Write the cleansed data to the target folder in Parquet format.
   - Parquet has multiple benefits such as its efficient storage due to compression, support for complex data types, and better performance with analytical queries.

## Data Partitioning
The data will be partitioned along the Year and Month values in the Order Date column. I have chosen this strategy since it will probably be a much used filter within analytical queries. There is a mild data skew when using this strategy as seen in the EDA queries.

## Implementation Steps
1. Create a cluster in Databricks.
2. Perform EDA in EDA_notebook
3. Use an orchestration tool such as Databricks workflows to create a monthly job to run the ETL_notebook to process and write the data to the cleansed folder in Parquet format, partitioned by year and month.

## Data Quality Checks
- Ensure no duplicate records exist in the output data.
- Check if the output has been written


4. The reasoning about the partition strategy is that it is the best partition strategy in most cases. Partitioning on date columns, 