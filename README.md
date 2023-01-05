The data validation scripts provide you 2 approaches to compare data between SQL Server and PostgreSQL DBs.

1 SQLFiles - Provide SQL and PostgreSQL specific queries in 2 files to download data and compare them.
2 Tables - Provide tables name as coma-separated values to download data and compare them.

  [1] dbconfig.properties File: This file is placeholder to provide database credentials for SQL Server and PostgreSQL DB and also used to specify which          download apporach you wanted to use (SQLFiles or Tables).
  [2] Parameter and its usage in this file. download_type=sqlfiles -> To download data using SQL Queries download_type=tables -> To download data using 

Tables
table_list: Provide required table names with coma-separated values to download data using tables. It will use these tables names only when download_type parameter is set to tables Eg. table_list = rep_research_document, rep_metadata

out_dir: This folder location is used to download data into CSV files and comparison. Please change the folder location to point to your folder. Please create new folder under "compare_files" folder in case you want download data for various iterations. Eg. out_dir = C:\Users\pborse\Documents\data_validation\compare_files\05032022\

All SQL Server CSV files will be placed under .\05032022\sqlserver folder. All PostgreSQL CSV files will be placed under .\05032022\postgres folder.

MAX_THREADS=3 This is used to start download process in multi-threaded environment based on your CPU usage.

[3] Run data-validation scripts

[a] python download_tables.py
    This will start download for either SQL Files or Tables apporach and
    create separate files per table name under sqlserver and postgres folders.
    This will create log-download-tables.log file for this process.
 
[b] python compare_data.py
    This will start comparison of downloaded CSV files.
    If any difference is found then it will create new file for postgres and sqlsever under .\05032022\result folder.
    This will create log-datavalidation-compare-files-results.log file for this process.
 
