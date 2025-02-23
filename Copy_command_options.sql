// Create required Database/Schemas
use database MYDB;
use  MYDB.EXTERNAL_STAGES;

//VALIDATION_MODE
-------------
// Create table
CREATE OR REPLACE TABLE  MYDB.PUBLIC.TBL_ORDERS(
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

// Case 1: Files without errors
// Create Stage Object
CREATE OR REPLACE STAGE MYDB.EXTERNAL_STAGES.sample_aws_stage
    url='s3://snowflakebucket-copyoption/size/';
 
LIST @MYDB.EXTERNAL_STAGES.sample_aws_stage;  
    
 //Load data using copy command
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Orders.*'
    VALIDATION_MODE = RETURN_ERRORS;    
    
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
   VALIDATION_MODE = RETURN_10_ROWS ;
   
// Case 2: Files with errors
// Create Stage Object
CREATE OR REPLACE STAGE MYDB.EXTERNAL_STAGES.sample_aws_stage2
    url='s3://snowflakebucket-copyoption/returnfailed/';
 
LIST @MYDB.EXTERNAL_STAGES.sample_aws_stage2;  
    
 //Load data using copy command
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage2
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;    
    
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage2
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
   VALIDATION_MODE = RETURN_10_ROWS ;   
   
--RETURN_FAILED_ONLY
------------------
//Create table with above DDL

// Create Stage Object
CREATE OR REPLACE STAGE MYDB.EXTERNAL_STAGES.sample_aws_stage
    url='s3://snowflakebucket-copyoption/returnfailed/';
  
LIST @MYDB.EXTERNAL_STAGES.sample_aws_stage  ;
    
//Load data using copy command
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    RETURN_FAILED_ONLY = TRUE;

--ON_ERROR
-------
// Create table with above DDL
 
// Create Stage Object
CREATE OR REPLACE STAGE MYDB.EXTERNAL_STAGES.sample_aws_stage
    url='s3://snowflakebucket-copyoption/returnfailed/';
  
LIST @MYDB.EXTERNAL_STAGES.sample_aws_stage;

// First try without ON_ERROR property
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';
 
// Now try with ON_ERROR=CONTINUE
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    ON_ERROR = CONTINUE;
 
--FORCE
--------
// Create table with above DDL

// Create Stage Object
CREATE OR REPLACE STAGE MYDB.EXTERNAL_STAGES.sample_aws_stage
    url='s3://snowflakebucket-copyoption/size/';
  
LIST @MYDB.EXTERNAL_STAGES.sample_aws_stage;  
    
 //Load data using copy command
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';

// Try to load same file, copy command will not fail but just skips the file
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';
   
SELECT * FROM TBL_ORDERS;    

// Try Using the FORCE option, the file will be loaded again
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    FORCE = TRUE;
    
SELECT * FROM PUBLIC.TBL_ORDERS;

--TRUNCATE COLUMNS
---------------
CREATE OR REPLACE TABLE  MYDB.PUBLIC.TBL_ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(10),
    SUBCATEGORY VARCHAR(30));

// Create Stage Object
CREATE OR REPLACE STAGE MYDB.EXTERNAL_STAGES.sample_aws_stage
    url='s3://snowflakebucket-copyoption/size/';
  
LIST @MYDB.EXTERNAL_STAGES.sample_aws_stage;  
    
 //Load data using copy command
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';

//With TRUNCATECOLUMNS property
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    TRUNCATECOLUMNS = TRUE; 
    
SELECT * FROM PUBLIC.TBL_ORDERS;

--SIZE_LIMIT
---------
// Create table, stage object and then run below query

//Load data using copy command

drop table PUBLIC.TBL_ORDERS;
COPY INTO MYDB.PUBLIC.TBL_ORDERS
    FROM @sample_aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    TRUNCATECOLUMNS = TRUE
    SIZE_LIMIT=30000;