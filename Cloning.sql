// Cloning a Table
use database mydb;
CREATE TABLE mydb.public.customer_data_clone
CLONE mydb.public.customer_data;

SELECT * FROM mydb.public.customer_data;
SELECT * FROM mydb.public.customer_data_clone;

// Cloning Schema
CREATE SCHEMA mydb.copy_of_file_formats
CLONE mydb.file_formats;


// Cloning Database
CREATE DATABASE mydb_copy
CLONE mydb;


//Update data in source and cloned objects and observer both the tables

select * from mydb.public.customer_data where customer_id=1;;
UPDATE mydb.public.customer_data SET FIRST_NAME='ARUN' WHERE CUSTOMER_ID=1;
select * from mydb.public.customer_DATA where customer_id=1;
select * from mydb.public.customer_data_clone where customer_id=1;

select * from mydb.public.customer_data_clone where customer_id=1;
UPDATE mydb.public.customer_data_clone SET CITY='HYDERABAD' WHERE CUSTOMER_ID=1;
select * from mydb.public.customer_data_clone where customer_id=1;
select * from mydb.public.customer_data where customer_id=1;


//Dropping cloned objects
DROP DATABASE mydb_copy;
DROP SCHEMA mydb.copy_of_file_formats;
DROP TABLE mydb.public.customer_data_clone;


// Clone using Time Travel

SELECT * FROM mydb.public.customer_data;
DELETE FROM mydb.public.customer_data;

CREATE OR REPLACE TABLE mydb.PUBLIC.customer_tt_clone
CLONE mydb.public.customer_data at (OFFSET => -60*5);

SELECT * FROM mydb.public.customer_tt_clone;

