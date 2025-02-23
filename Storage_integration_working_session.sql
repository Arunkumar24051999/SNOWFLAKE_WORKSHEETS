// Create storage integration object
create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::597088015338:role/aws_s3_snowflake_integration'
  STORAGE_ALLOWED_LOCATIONS = ('s3://awss3arun/csv/', 's3://awss3arun/json/')
  COMMENT = 'Integration with aws s3 buckets' ;
   
   
// Get external_id and update it in S3
DESC integration s3_int;

// ARN -- Amazon Resource Names
// S3 -- Simple Storage Service
// IAM -- Identity and Access Management

-----------------------------------
// Create database and schema
CREATE DATABASE IF NOT EXISTS MYDB;
CREATE SCHEMA IF NOT EXISTS MYDB.file_formats;

// Create file format object
CREATE OR REPLACE file format mydb.file_formats.csv_fileformat1
    type = csv
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = TRUE
    error_on_column_count_mismatch=false;
    
// Create stage object with integration object & file format object
CREATE OR REPLACE STAGE mydb.external_stages.aws_s3_csv
    URL = 's3://awss3arun/csv/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = mydb.file_formats.csv_fileformat1 ;

//Listing files under your s3 buckets
list @mydb.external_stages.aws_s3_csv;


// Create a table first
CREATE OR REPLACE TABLE mydb.public.customer_data (
Customer_id STRING
,First_Name	STRING
,Last_Name	STRING
,Company STRING
,City	STRING
,Country	STRING
,Phone_1	STRING
,Phone_2 STRING	
,Email	STRING
,Subscription_Date STRING
,Website STRING
);
select * from  mydb.public.customer_data;




// Use Copy command to load the files
COPY INTO mydb.public.customer_data(Customer_id 
,First_Name	
,Last_Name	
,Company 
,City	
,Country	
,Phone_1	
,Phone_2 
,Email	
,Subscription_Date 
,Website 
)
    FROM @mydb.external_stages.aws_s3_csv/customers-100.csv;
    
 
//Validate the data
SELECT * FROM mydb.public.customer_data;

//create an internal stage 
USE DATABASE MYDB;
CREATE OR REPLACE SCHEMA INTERNAL_STAGES;
DROP STAGE INTERNAL_STAGES;

------------------------------------------------------------------
select * from mydb.public.employees;

CREATE TABLE MYDB.Public.employees(
First_Name	STRING
,Last_Name	STRING
,Email	STRING
,Phone	STRING
,Gender	STRING
,Age STRING
,Job STRING
,Title	STRING
,Experience	STRING
,Salary	STRING
,Department STRING
);
drop table MYDB.Public.employees;


----------------------------------------------------
CREATE STAGE MYDB.INTERNAL_STAGES.EMPLOYEES;
CREATE STAGE MYDB.INTERNAL_STAGES.SALARIES;

select * from mydb.public.employees;
use mydb.public;
COPY INTO MYDB.PUBLIC.employees
                            from @%employees/employees.csv
                            file_format=(type= csv field_delimiter= "," skip_header= 1 error_on_column_count_mismatch=
                            false);
LIST @%employees;  
COPY INTO MYDB.PUBLIC.EMPLOYEES
                            FROM @mydb.internal_stages.employees
                            file_format=(type= csv field_delimiter= ',' skip_header= 1 error_on_column_count_mismatch=false);   
                            



