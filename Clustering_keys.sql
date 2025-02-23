// Create a Database
CREATE DATABASE IF NOT EXISTS MYDB;

//Create a table with no cluster keys
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_NONCLUSTER (
 C_CUSTKEY NUMBER(38,0),
 C_NAME VARCHAR(25),
 C_ADDRESS VARCHAR(40),
 C_NATIONKEY NUMBER(38,0),
 C_PHONE VARCHAR(15),
 C_ACCTBAL NUMBER(12,2),
 C_MKTSEGMENT VARCHAR(10),
 C_COMMENT VARCHAR(117)
);

// Insert data into above non-clustered table
INSERT INTO PUBLIC.CUSTOMER_NONCLUSTER
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;

//Create a table with cluster key
CREATE OR REPLACE TABLE PUBLIC.CUSTOMER_CLUSTER (
 C_CUSTKEY NUMBER(38,0),
 C_NAME VARCHAR(25),
 C_ADDRESS VARCHAR(40),
 C_NATIONKEY NUMBER(38,0),
 C_PHONE VARCHAR(15),
 C_ACCTBAL NUMBER(12,2),
 C_MKTSEGMENT VARCHAR(10),
 C_COMMENT VARCHAR(117)
 )
 cluster by (C_NATIONKEY);


// Insert data into above clustered table
INSERT INTO PUBLIC.CUSTOMER_CLUSTER
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;

// Observe time taken and no.of partitions scanned
SELECT * FROM PUBLIC.CUSTOMER_NONCLUSTER WHERE C_NATIONKEY=2; --  11 sec -- 420/420 mp scanned
SELECT * FROM PUBLIC.CUSTOMER_CLUSTER WHERE C_NATIONKEY=2; -- 11 sec -- 22/482 mp scanned

-----------------------

CREATE OR REPLACE TABLE PUBLIC.ORDERS_NONCLUSTER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

CREATE OR REPLACE TABLE PUBLIC.ORDERS_CLUSTER
AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS;

// Add Cluster key to the table
ALTER TABLE PUBLIC.ORDERS_CLUSTER CLUSTER BY (YEAR(O_ORDERDATE));

// Observe time taken and no.of partitions scanned
SELECT * FROM PUBLIC.ORDERS_NONCLUSTER WHERE YEAR(O_ORDERDATE) = 1995; -- 15 sec -- 86/233 mps
SELECT * FROM PUBLIC.ORDERS_CLUSTER WHERE YEAR(O_ORDERDATE) = 1995; -- 16 sec -- 38/242 mps

// Alter Table to add multiple cluster keys
ALTER TABLE PUBLIC.ORDERS_CLUSTER CLUSTER BY (YEAR(O_ORDERDATE), O_ORDERPRIORITY);

// Observe time taken and no.of partitions scanned
SELECT * FROM PUBLIC.ORDERS_NONCLUSTER WHERE YEAR(O_ORDERDATE) = 1996 and O_ORDERPRIORITY = '1-URGENT'; -- 4.1sec -- 70/233 
SELECT * FROM PUBLIC.ORDERS_CLUSTER WHERE YEAR(O_ORDERDATE) = 1996 and O_ORDERPRIORITY = '1-URGENT'; -- 4.2sec -- 12/242

// To Turn-off results cache
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

//To look at clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('ORDERS_CLUSTER');

