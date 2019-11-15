# Snowflake JSON Data Pipeline
Building Json data pipeline within Snowflake using Streams and Tasks

Online documentation : https://docs.snowflake.net/manuals/user-guide/data-pipelines-intro.html

# Prerequisites
None, everything you need is in this github repo

# Introduction
Data pipelines automate many of the manual steps involved in transforming and optimizing continuous data loads.
Frequently, the “raw” data is first loaded temporarily into a staging table used for interim storage and then
transformed using a series of SQL statements before it is inserted into the destination reporting tables.
The most efficient workflow for this process involves transforming only data that is new or modified.

# Traditional approach
![](/Images/traditional-approach.png)

# Snowflake approach
![](/Images/snowflake-approach.png)

# What's in this tutorial ?
In this tutorial we are going to create the following Data Pipeline :
  * Manually ingest JSON files to a raw_json_table table without any transformation (only 1 VARIANT column)
  * Extract RAW json data from raw_json_table and structure into a destination table (transformed_json_table)
  * Transform data (using User Defined Function) from transformed_json_table to a final destination table (final_table)

## We will use 2 Warehouses :
  * load_wh for loading JSON data
  * task_wh to run the stored procedure that extract and transform the data between tables

## 2 steps Incremental Data Pipeline with Change Data Capture (CDC)
This continuous Data Pipeline is incremental (no need to reload the entire table on every update) thanks to our Change Data Capture feature (CDC) :
  * changes over raw_json_table are tracked by the stream raw_data_stream
  * changed over transformed_json_table are tracked by the stream transformed_data_stream

## Extract and transform JSON data with Stored procedure and tasks
All extract and transform operation are going to be processed through Stored Procedure called by a scheduled task :
  * The root task extract_json_data is scheduled every minute and call stored_proc_extract_json()
    * This stored procedure will look for INSERTS (from raw_data_stream) on the raw_json_table and then consume it to extract JSON data to transformed_json_table
  * The second task (aggregate_final_data) is not scheduled, as a workflow it run just after extract_json_data to call stored_proc_aggregate_final()
    * This stored procedure will look for INSERTS (from transformed_data_stream) on the transformed_json_table and then consume it to transform data to final_table

Mike Uzan - Senior SE (EMEA/France)
mike.uzan@snowflake.com
+33621728792
