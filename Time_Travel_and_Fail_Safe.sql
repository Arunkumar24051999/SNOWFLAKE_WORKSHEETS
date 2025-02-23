SHOW TABLES in SCHEMA  mydb.public;
SHOW SCHEMAS in DATABASE mydb;
SHOW DATABASES;

// how to set this at the time of table creation
CREATE OR REPLACE TABLE mydb.public.timetravel_ex(id number, name string);

SHOW TABLES like 'timetravel_ex%';

CREATE OR REPLACE TABLE mydb.public.timetravel_ex(id number, name string)
DATA_RETENTION_TIME_IN_DAYS = 10;

SHOW TABLES like 'timetravel_ex%';

// setting at schema level
CREATE SCHEMA mydb.abcxyz DATA_RETENTION_TIME_IN_DAYS = 10;
SHOW SCHEMAS like 'abcxyz';

CREATE OR REPLACE TABLE mydb.abcxyz.timetravel_ex2(id number, name string);
SHOW TABLES like 'timetravel_ex2%';

CREATE OR REPLACE TABLE mydb.abcxyz.timetravel_ex2(id number, name string) DATA_RETENTION_TIME_IN_DAYS = 20;
SHOW TABLES like 'timetravel_ex2%';


// dont forget to change your schema back to public on right top corner
// how to alter retention period later?
ALTER TABLE mydb.public.timetravel_ex 
SET DATA_RETENTION_TIME_IN_DAYS = 15;

SHOW TABLES like 'timetravel_ex%';


// Querying history data

// Updating some data first

// Case1: update some data in customer table

SHOW TABLES LIKE 'CUSTOMER%';
SELECT * FROM mydb.public.customer_data;

SELECT * FROM mydb.public.customer_data WHERE CUSTOMER_ID=1;

UPDATE mydb.public.customer_data SET FIRST_NAME='TONY' WHERE CUSTOMER_ID=1;

SELECT * FROM mydb.public.customer_data WHERE CUSTOMER_ID=1;

//=============

// Case2: delete some data from emp_data table

SELECT * FROM mydb.public.customer_data;

SELECT * FROM mydb.public.customer_data where customer_id=2;

DELETE FROM mydb.public.customer_data where customer_id=2;

SELECT CURRENT_TIMESTAMP; --2025-02-19 08:57:50.543 -0800

SELECT * FROM mydb.public.customer_data where customer_id=2;

//==============

// Case3: update some data in customer table

SELECT * FROM mydb.public.customer_data;

SELECT * FROM mydb.public.customer_data WHERE last_name='Roy';

UPDATE mydb.public.customer_data SET last_name='Royal' WHERE CUSTOMER_ID=3; -- 01a57b69-0004-25d4-0015-ab8700024536

SELECT * FROM mydb.public.customer_data WHERE CUSTOMER_ID=3;

//==============

// Case1: retrieve history data by using AT OFFSET

SELECT * FROM mydb.public.customer_data WHERE CUSTOMER_ID=1;

SELECT * FROM mydb.public.customer_data AT (offset => -60*30)
WHERE CUSTOMER_ID=1;

// Case2: retrieve history data by using AT TIMESTAMP
SELECT * FROM mydb.public.customer_data where customer_id=2;

SELECT * FROM mydb.public.customer_data AT(timestamp => '2025-02-19 08:57:50.543 -0800'::timestamp)
WHERE customer_id=2;


// Case3: retrieve history data by using BEFORE STATEMENT
SELECT * FROM mydb.public.customer_data
WHERE CUSTOMER_ID=3;

SELECT * FROM mydb.public.customer_data 
before(statement => '01ba803a-0002-7012-0007-52f2000386ee')
WHERE customer_id=3;

//=================

// Restoring Tables
SHOW TABLEs like 'customer%';
DROP TABLE mydb.public.customer_data;
SHOW TABLEs like 'customer%';
UNDROP TABLE mydb.public.customer_data;
SHOW TABLEs like 'customer%';

// Restoring Schemas
SHOW SCHEMAS in DATABASE mydb;
DROP SCHEMA ABCXYZ;
SHOW SCHEMAS in DATABASE mydb;
UNDROP SCHEMA ABCXYZ;
SHOW SCHEMAS in DATABASE mydb;

//====================
// Time Travel Cost

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_NAME = 'CUSTOMER_DATA';

SELECT  ID, 
  TABLE_NAME, 
  TABLE_SCHEMA,
        TABLE_CATALOG,
  ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
  TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_NAME = 'CUSTOMER_LARGE'
ORDER BY STORAGE_USED_GB DESC,TIME_TRAVEL_STORAGE_USED_GB DESC;