# MetyisCase
Technical Assesment for Metyis

In this repo the sales data for a retail company will be ingested from multiple .csv files. These are stored in the 'raw' folder. The data should be cleaned and written to the 'cleansed' folder. 

1. For the code see ' ' notebook. In this notebook the different files are read and turned into Spark dataframes.

2. The format in the cleansed folder will be Delta Parquet. The reasoning behind this is as follows:
  - Since I am using an old laptop I have chosen not to write the code in VS but in my personal Databricks dev workspace. Delta Parquet is the standard file format on Databricks. It is normal parquet but with ACID gaurantees due to the added Delta file. This file contains all changes made on the existing compressed parquet files
  - If I would have ran my code on VS I would still have chosen normal Parquet as the file format. The columnar format Parquet is a much used format in big data solutions since it has fast reads (especialy when agregating data), fast writes and has a small file size due to compression.

3. Hash partitioning. 

4. The reasoning about the partition strategy is that it is the best partition strategy in most cases. Partitioning on date columns, 