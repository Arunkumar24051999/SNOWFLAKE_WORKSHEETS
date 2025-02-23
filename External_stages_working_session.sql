use database MYDB;
desc stage MYDB.EXTERNAL_STAGES.AWS_EXT_STAGE;
list @EXTERNAL_STAGES.AWS_EXT_STAGE;
desc stage MYDB.EXTERNAL_STAGES.AWS_EXT_STAGE/OrderDetails;

// Creating ORDERS table
CREATE OR REPLACE TABLE MYDB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));
    
SELECT * FROM MYDB.PUBLIC.ORDERS;

//Load data using copy command

// Copy command with specified file(s)

COPY INTO MYDB.PUBLIC.ORDERS
    FROM @MYDB.external_stages.AWS_EXT_STAGE
    file_format = (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails.csv');
    
SELECT * FROM MYDB.PUBLIC.ORDERS;


// Copy command with pattern for file names

COPY INTO MYDB.PUBLIC.ORDERS
    FROM @MYDB.external_stages.AWS_EXT_STAGE
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';
    
SELECT * FROM MYDB.PUBLIC.ORDERS;
 
============
File Formats
=============

// Creating schema to keep file formats
CREATE SCHEMA IF NOT EXISTS MYDB.file_formats;

// Creating file format object
CREATE file format MYDB.file_formats.csv_file_format;
// See properties of file format object
DESC file format MYDB.file_formats.csv_file_format;  


// Creating table
CREATE OR REPLACE TABLE MYDB.PUBLIC.ORDERS_EX (   
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
);

// Using file format object in Copy command       
COPY INTO MYDB.PUBLIC.ORDERS_EX
    FROM @MYDB.external_stages.AWS_EXT_STAGE
    file_format= (FORMAT_NAME = MYDB.file_formats.csv_file_format)
    files = ('OrderDetails.csv');

    
// Altering file format object
ALTER file format MYDB.file_formats.csv_file_format
    SET SKIP_HEADER = 1;
    
DESC file format MYDB.file_formats.csv_file_format;

 
// Using file format object in Copy command       
COPY INTO MYDB.PUBLIC.ORDERS_EX
    FROM @MYDB.external_stages.AWS_EXT_STAGE/OrderDetails.csv
    file_format= (FORMAT_NAME=MYDB.file_formats.csv_file_format);
   
 drop table MYDB.PUBLIC.ORDERS_EX;
select * from MYDB.PUBLIC.ORDERS_EX;
