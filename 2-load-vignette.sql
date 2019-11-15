/*--------------------------------------------------------------------------------

Full GitHub project available here : https://github.com/kromozome2003/Snowflake-Json-DataPipeline

In this vignette you will insert data into the Continuous Data Pipeline and demonstrate that both extraction and aggregation are automatic and incremental.

Mike Uzan - Senior SE (EMEA/France)
mike.uzan@snowflake.com
+33621728792

--------------------------------------------------------------------------------*/

-- Context setting
USE DATABASE CDPST;
USE SCHEMA CDPST.PUBLIC;
CREATE warehouse IF NOT EXISTS load_wh WITH warehouse_size = 'medium' auto_suspend = 60 initially_suspended = true;
CREATE warehouse IF NOT EXISTS task_wh WITH warehouse_size = 'medium' auto_suspend = 60 initially_suspended = true;
USE WAREHOUSE load_wh;

-- Show that 2 tasks exists to run this pipeline (1 root task and the second one is dependent)
SHOW TASKS;

-- How long do we have to wait for next run ?
SELECT timestampdiff(second, CURRENT_TIMESTAMP, scheduled_time) AS next_run, scheduled_time, CURRENT_TIMESTAMP, name, state
FROM TABLE(information_schema.task_history()) WHERE state = 'SCHEDULED' ORDER BY completed_time DESC;

-- Insert some data to the raw_json_table (you can repeat the COPY with others weather json files in the json-files folder)
COPY INTO raw_json_table FROM (SELECT $1 FROM @CDPST.PUBLIC.cdpst_json_files/weather1.json.gz (FILE_FORMAT => CDPST.PUBLIC.JSON) LIMIT 10);

-- Read the content of the newly created stream (should contains our 10 inserted records)
SELECT * FROM raw_data_stream;
SELECT * FROM transformed_data_stream;

-- Check when those 2 tables will be populated (automatically)
SELECT * FROM transformed_json_table;
SELECT * FROM final_table;
