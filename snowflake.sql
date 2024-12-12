-- Create a new database 
CREATE DATABASE DE_PROJECT;

-- Switch to the newly created database
USE DATABASE DE_PROJECT;

-- Create table to load CSV data
CREATE OR REPLACE TABLE weather_table (
    feelslike_c FLOAT,
    temp FLOAT,
    city STRING,
    humidity FLOAT,
    wind_speed FLOAT,
    time TIMESTAMP,
    visibility_km FLOAT,
    wind_dir STRING,
    pressure_mb FLOAT
);


--Create integration object for external stage
create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'SNOWFLAKE_role'
  storage_allowed_locations = ('s3://weather-data-def/snowflake/');
  
  
--Describe integration object to fetch external_id and to be used in s3
DESC INTEGRATION s3_int;

create or replace file format csv_format
                    type = csv
                    field_delimiter = ','
                    skip_header = 1
                    null_if = ('NULL', 'null')
                    empty_field_as_null = true;

create or replace stage ext_csv_stage
  URL = 's3://weather-data-def/snowflake/'
  STORAGE_INTEGRATION = s3_int
  file_format = csv_format;

DESC STAGE ext_csv_stage;
LIST @ext_csv_stage

--create pipe to automate data ingestion from s3 to snowflake
create or replace pipe mypipe auto_ingest=true as
copy into weather_table
from @ext_csv_stage
on_error = CONTINUE;

show pipes;

select * from weather_table;