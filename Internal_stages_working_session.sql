

USE DATABASE MYDB;

USE SCHEMA PUBLIC;

SELECT * FROM employees;


// USER STAGE
//Put your files into user internal stage
//If your put command is failing with 403 forbidden error, practice this session after AWS-Snowflake integration session(next video in the play list) then it will work.

put file://C:\Users\ARUNKUMAR\Downloads.employees.csv @~/staged;

put file://C:\Users\ARUNKUMAR\Downloads.employees.json @~/staged;

list @~/staged;


// TABLE STAGE
//Put your files into table internal stage
put file://C:\Users\ARUNKUMAR\Downloads.employees.csv @%employees;

//Create customer_data_table to load files from internal stages
CREATE OR REPLACE TABLE mydb.public.employees (
First_Name STRING
,Last_Name STRING
,Email STRING
,Phone STRING
,Gender STRING
,Age STRING
,Job_Title STRING
,Years_Of_Experience STRING
,Salary STRING
,Department STRING


);

put file://C:\Users\ARUNKUMAR\Downloads\employees.csv @%employees;

list @%employees;


//Named Stage
// Create a schema for internal stages
CREATE SCHEMA IF NOT EXISTS mydb.internal_stages

//Create a named stage
CREATE OR REPLACE STAGE mydb.internal_stages.employees;

CREATE OR REPLACE STAGE mydb.internal_stages.salaries;


show stages in mydb.internal_stages;

//Put your files into named internal stage
put file://C:\Users\ARUNKUMAR\Downloads\employees.csv @mydb.internal_stages.employees;

list @mydb.internal_stages.employees;


// Load all files data to the table
//Copy all these files to table customer_data_table

COPY INTO mydb.public.employees
FROM @~/staged/employees.csv
file_format = (type = csv field_delimiter = ',' skip_header = 1  error_on_column_count_mismatch=false )
ON_ERROR= CONTINUE;
--FORCE=TRUE  //loads data even the file is being loaded previously
--purge=true  // after loading the file will be deleted from stage;
--load_uncertain_files= true|false // if file status is unknown i.e., last modified is older than 64 days

COPY INTO mydb.public.employees
FROM @%employees/employees.csv
file_format = (type = csv field_delimiter = ',' skip_header = 1 error_on_column_count_mismatch=false);

COPY INTO mydb.public.employees
FROM @mydb.internal_stages.employees/employees.csv
file_format = (type = csv field_delimiter = ',' skip_header = 1 error_on_column_count_mismatch=false)

FORCE= TRUE;


//Validate the data
SELECT * FROM mydb.public.employees;
                            