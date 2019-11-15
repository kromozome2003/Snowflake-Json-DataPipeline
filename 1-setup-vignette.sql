/*--------------------------------------------------------------------------------

Full GitHub project available here : https://github.com/kromozome2003/Snowflake-Json-DataPipeline

Building Data Pipelines using Snowflake Streams and Tasks
Online documentation : https://docs.snowflake.net/manuals/user-guide/data-pipelines-intro.html

Data pipelines automate many of the manual steps involved in transforming and optimizing continuous data loads.
Frequently, the “raw” data is first loaded temporarily into a staging table used for interim storage and then
transformed using a series of SQL statements before it is inserted into the destination reporting tables.
The most efficient workflow for this process involves transforming only data that is new or modified.

In this tutorial we are going to create the following Data Pipeline :
  1- Manually ingest JSON files to a raw_json_table table without any transformation (only 1 VARIANT column)
  2- Extract RAW json data from raw_json_table and structure into a destination table (transformed_json_table)
  3- Aggregate data from transformed_json_table to a final destination table (final_table)

We will use 2 Warehouses :
  - load_wh for loading JSON data
  - task_wh to run the stored procedure that extract and transform the data between tables

This continuous Data Pipeline is incremental (no need to reload the entire table on every update) thanks to our Change Data Capture feature (CDC) :
  - changes over raw_json_table are tracked by the stream raw_data_stream
  - changed over transformed_json_table are tracked by the stream transformed_data_stream

All extract and transform operation are going to be processed through Stored Procedure called by a scheduled task :
  - The root task extract_json_data is scheduled every minute and call stored_proc_extract_json()
    - This stored procedure will look for INSERTS (from raw_data_stream) on the raw_json_table and then consume it to extract JSON data to transformed_json_table
  - The second task (aggregate_final_data) is not scheduled, as a workflow it run just after extract_json_data to call stored_proc_aggregate_final()
    - This stored procedure will look for INSERTS (from transformed_data_stream) on the transformed_json_table and then consume it to transform data to final_table

Mike Uzan - Senior SE (EMEA/France)
mike.uzan@snowflake.com
+33621728792

--------------------------------------------------------------------------------*/
-- Context setting
CREATE OR REPLACE DATABASE CDPST;
USE DATABASE CDPST;
USE SCHEMA CDPST.PUBLIC;
CREATE warehouse IF NOT EXISTS load_wh WITH warehouse_size = 'medium' auto_suspend = 60 initially_suspended = true;
CREATE warehouse IF NOT EXISTS task_wh WITH warehouse_size = 'medium' auto_suspend = 60 initially_suspended = true;
USE WAREHOUSE load_wh;

/*--------------------------------------------------------------------------------
  CLEAN UP
--------------------------------------------------------------------------------*/
DROP STREAM IF EXISTS raw_data_stream;
DROP STREAM IF EXISTS transformed_data_stream;
DROP TABLE IF EXISTS raw_json_table;
DROP TABLE IF EXISTS transformed_json_table;
DROP TABLE IF EXISTS final_table;
DROP PROCEDURE IF EXISTS stored_proc_extract_json();
DROP PROCEDURE IF EXISTS stored_proc_aggregate_final();
DROP TASK IF EXISTS extract_json_data;
DROP TASK IF EXISTS aggregate_final_data;
DROP FILE FORMAT IF EXISTS CDPST.PUBLIC.JSON;
DROP STAGE IF EXISTS CDPST.PUBLIC.cdpst_json_files;

-- Create a FILE FORMAT to parse JSON files
CREATE FILE FORMAT CDPST.PUBLIC.JSON
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE;

-- Create a STAGE where to put our json files
CREATE STAGE CDPST.PUBLIC.cdpst_json_files;

-- Lets put some Json files to your user stage : @~
-- Put some files (in json-files folder) using SnowSQL or the GUI to @~/
-- put 'file:///Path/to/your/github/project/json-files/weather*.json.gz' @CDPST.PUBLIC.cdpst_json_files;

-- Let's list the json files we uploaded
LIST @CDPST.PUBLIC.cdpst_json_files;
SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather1.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON) LIMIT 5;

-- Create the RAW TRANSFORMED & FINAL tables
CREATE OR REPLACE TABLE raw_json_table (v variant);
CREATE OR REPLACE TABLE transformed_json_table (
  date timestamp_ntz,
  country string,
  city string,
  id string,
  temp_kel float,
  temp_min_kel float,
  temp_max_kel float,
  conditions string,
  wind_dir float,
  wind_speed float
);
CREATE OR REPLACE TABLE final_table (
  date timestamp_ntz,
  country string,
  city string,
  id string,
  temp_cel float,
  temp_min_cel float,
  temp_max_cel float,
  conditions string,
  wind_dir float,
  wind_speed float
);

-- Create the Stream to monitor Data Changes against RAW table
CREATE OR REPLACE STREAM raw_data_stream ON TABLE raw_json_table;

-- Create the Stream to monitor Data Changes against TRANSFORMED table
CREATE OR REPLACE STREAM transformed_data_stream ON TABLE transformed_json_table;

-- List the streams
SHOW STREAMS; -- see the stream in the list

-- Read the content of the newly created stream (should be empty as no data has been inserted yet)
SELECT * FROM raw_data_stream;
SELECT * FROM transformed_data_stream;

-- Create a stored procedure that will read the content of raw_data_stream and extract inserted json data to transformed_json_table
create or replace procedure stored_proc_extract_json()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO transformed_json_table (date,country,city,id,temp_kel,temp_min_kel,temp_max_kel,conditions,wind_dir,wind_speed)";
    sql_command += "    SELECT";
    sql_command += "        convert_timezone('UTC', 'Europe/Paris', v:time::timestamp_ntz) date,";
    sql_command += "        v:city.country::string country,";
    sql_command += "        v:city.name::string city,";
    sql_command += "        v:city.id::string id,";
    sql_command += "        v:main.temp::float temp_kel,";
    sql_command += "        v:main.temp_min::float temp_min_kel,";
    sql_command += "        v:main.temp_max::float temp_max_kel,";
    sql_command += "        v:weather[0].main::string conditions,";
    sql_command += "        v:wind.deg::float wind_dir,";
    sql_command += "        v:wind.speed::float wind_speed";
    sql_command += "    FROM raw_data_stream";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "JSON extracted.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

-- Create a stored procedure that will read the content of the transformed_data_stream data and transform inserted data to aggregated_final_table
CREATE OR REPLACE PROCEDURE stored_proc_aggregate_final()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = "INSERT INTO final_table (date,country,city,id,temp_cel,temp_min_cel,temp_max_cel,conditions,wind_dir,wind_speed)";
    sql_command += "    SELECT";
    sql_command += "        date,";
    sql_command += "        country,";
    sql_command += "        city,";
    sql_command += "        id,";
    sql_command += "        temp_kel-273.15 temp_cel,";
    sql_command += "        temp_min_kel-273.15 temp_min_cel,";
    sql_command += "        temp_max_kel-273.15 temp_max_cel,";
    sql_command += "        conditions,";
    sql_command += "        wind_dir,";
    sql_command += "        wind_speed";
    sql_command += "    FROM transformed_data_stream";
    sql_command += "    WHERE metadata$action = 'INSERT';";
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "TRANSFORMED JSON - AGGREGATED.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;

-- Create a task to look for newly inserted RAW data every 1 minute
CREATE OR REPLACE TASK extract_json_data warehouse = task_wh SCHEDULE = '1 minute' WHEN system$stream_has_data('raw_data_stream') AS CALL stored_proc_extract_json();

-- Create a sub-task to run after RAW data has been extracted (RUN AFTER)
CREATE OR REPLACE TASK aggregate_final_data warehouse = task_wh AFTER extract_json_data WHEN system$stream_has_data('transformed_data_stream') AS CALL stored_proc_aggregate_final();

-- Resume task to make it run
ALTER TASK extract_json_data RESUME;
ALTER TASK aggregate_final_data RESUME;
SHOW TASKS;

-- SWITCH TO "Stream & Tasks (DEMO)" VIGNETTE
