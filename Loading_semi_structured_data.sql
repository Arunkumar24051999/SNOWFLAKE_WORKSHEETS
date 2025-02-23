USE DATABASE MYDB;
CREATE OR REPLACE SCHEMA MYDB.external_stages;
CREATE OR REPLACE SCHEMA MYDB.STAGE_TBLS;
CREATE OR REPLACE SCHEMA MYDB.INTG_TBLS;

--Creating file format object
CREATE OR REPLACE FILE FORMAT MYDB.file_formats.FILE_FORMAT_JSON
 TYPE = JSON;

--Creating stage object
CREATE STAGE MYDB.EXTERNAL_STAGES.STAGE_JSON
    STORAGE_INTEGRATION = s3_int
    URL = 's3://awss3arun/json/';

    DESC storage integration s3_int;
    
--Listing files in the stage
LIST @MYDB.external_stages.STAGE_JSON;

--Creating Stage Table to store RAW Data 
CREATE OR REPLACE TABLE MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW 
(raw_file variant);

drop STAGE MYDB.external_stages.STAGE_JSON;
--Copy the RAW data into a Stage Table
COPY INTO MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW 
    FROM @MYDB.external_stages.STAGE_JSON
    file_format= MYDB.file_formats.FILE_FORMAT_JSON
    FILES=('employees.json');

--View RAW table data
SELECT * FROM MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW
limit 5;

--Extracting single column
SELECT raw_file, typeof(raw_file) FROM MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW LIMIT 5;
-- since the file type is array we need to extract each individual as follows
SELECT raw_file[0]:"id"::NUMBER AS ID 
FROM MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW;

-- To extract all elements in array
SELECT 
    json_data.value:id::NUMBER AS ID,
    json_data.value:first_name::STRING AS first_name
FROM MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW,
LATERAL FLATTEN(input => raw_file) json_data;

/*Extracting nested data
--SELECT raw_file:Name::string as Name,
       raw_file:Address."House Number"::string as House_No,
       raw_file:Address.City::string as City,
       raw_file:Address.State::string as State
FROM MYOWN_DB.STAGE_TBLS.PETS_DATA_JSON_RAW;*/

--Parsing entire file
SELECT json_data.value:id::string as id,
       json_data.value:first_name::string as first_name,
       json_data.value:last_name::string as last_name,
       json_data.value:email::string as email,
       json_data.value:phone::string as phone,
    json_data.value:gender::string as gender,
    json_data.value:age::string as age,
    json_data.value:job_title::string as job_title,
    json_data.value:years_of_experience::number as years_of_experience,
    json_data.value:salary::number as salary,
    json_data.value:department::string as department
    
from MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW,
LATERAL flatten(input => raw_file) json_data;


--Creating/Loading parsed data to another table
CREATE OR REPLACE  TABLE MYDB.INTG_TBLS.EMPLOYEES_DATA
AS
SELECT json_data.value:id::string as id,
       json_data.value:first_name::string as first_name,
       json_data.value:last_name::string as last_name,
       json_data.value:email::string as email,
       json_data.value:phone::string as phone,
    json_data.value:gender::string as gender,
    json_data.value:age::string as age,
    json_data.value:job_title::string as job_title,
    json_data.value:years_of_experience::number as years_of_experience,
    json_data.value:salary::number as salary,
    json_data.value:department::string as department
    
from MYDB.STAGE_TBLS.EMPLOYEES_JSON_RAW,

LATERAL flatten(input => raw_file) json_data;

--Viewing final data
SELECT * from MYDB.INTG_TBLS.EMPLOYEES_DATA;

--Truncate and Reload by using flatten

TRUNCATE TABLE MYDB.INTG_TBLS.EMPLOYEES_DATA;

--Viewing final data
SELECT * from MYDB.INTG_TBLS.EMPLOYEES_DATA;
