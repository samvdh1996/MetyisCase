# MetyisCase
Technical Assesment for Metyis

##Introduction
In this repo the sales data for a retail company will be ingested from multiple .csv files. The files should be combined, cleaned, partitioned and written to the cleansed folder.

## Aproach
1. **Read Source Data**:
   - Use PySpark to read all CSV files from the source folder (`Sales_Data/raw`). Use the * operator to union all files on reading
   
2. **Combine and Deduplicate Data**:
   - Use a Window function to deduplicate the data based on the Order ID, Product and Order Date columns.

3. **Write to Cleansed Folder**:
   - Write the cleansed data to the target folder in Parquet format.
   - Since I am using an old laptop I have chosen not to write the code in VS but in my personal Databricks dev workspace. While Delta Parquet is the standard file format on Databricks I will use normal Parquet.
  - Parquet has multiple benefits such as its efficient storage due to compression, support for complex data types, and better performance with analytical queries.

## Data Partitioning
The data will be partitioned along the Year and Month values in the Order Date column. I have chosen this strategy since it will probably be a much used filter within analytical queries. There is a mild data skew when using this strategy as seen in the EDA queries.

## Implementation Steps
1. Create a cluster in Databricks.
2. Perform EDA in EDA_notebook
3. Use the ETL_notebook to process and write the data.
4. Ensure there are no NULL values and duplicates by using a Window function.
6. Partition the data by the Year and Month values obtained from the 'Order Date'
5. Write the data to the cleansed folder in Parquet format.

## Data Quality Checks
- Ensure no duplicate records exist in the output data.
- Check if the output has been written


4. The reasoning about the partition strategy is that it is the best partition strategy in most cases. Partitioning on date columns, 