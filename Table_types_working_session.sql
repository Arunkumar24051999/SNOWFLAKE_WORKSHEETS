use database mydb;

SHOW TABLES in SCHEMA PUBLIC;

CREATE OR REPLACE TRANSIENT TABLE mydb.public.tran_table(id number);
CREATE OR REPLACE TEMPORARY TABLE mydb.public.temp_table(name string);

SHOW TABLES in SCHEMA PUBLIC;

//============================
 --Transient Tables/Schemas
//============================
// Create transient schema
CREATE OR REPLACE TRANSIENT SCHEMA mydb.tran_schema;

SHOW SCHEMAS in DATABASE mydb;

// Create trans table under this trans schema without trans keyword
CREATE OR REPLACE TABLE mydb.tran_schema.tran_table1(name string);

SHOW TABLES in SCHEMA mydb.tran_schema;

// Can we alter retention period for trans tables?
ALTER TABLE mydb.tran_schema.tran_table1
SET DATA_RETENTION_TIME_IN_DAYS = 2;

// We can restore trans tables within 24 hours
DROP TABLE mydb.tran_schema.tran_table1;

SHOW TABLES in SCHEMA mydb.tran_schema;

UNDROP TABLE mydb.tran_schema.tran_table1;

SHOW TABLES in SCHEMA mydb.tran_schema;

//===================
-- Temporary Tables
//===================
// Can we Create temporary schemas/databases - No
CREATE OR REPLACE TEMPORARY SCHEMA mydb.temp_schema;

// Create temp tables and insert sample data
CREATE OR REPLACE TEMPORARY TABLE mydb.public.temp_table2(name string);

INSERT INTO mydb.public.temp_table2 values('Ravi');
INSERT INTO mydb.public.temp_table2 values('Gopal');
INSERT INTO mydb.public.temp_table2 values('Harsha');

// Run this in 2 diff worksheets and observe results
SELECT * FROM mydb.public.temp_table2;

// Recreate the table and check the data
CREATE OR REPLACE TEMPORARY TABLE mydb.public.temp_table2(name string);

SELECT * FROM mydb.public.temp_table2;

// Try to restore - but can't restore with same name
UNDROP TABLE mydb.public.temp_table2;

// Rename current table and undrop table - will get dropped table
ALTER TABLE mydb.public.temp_table2 RENAME TO mydb.public.temp_table3;

UNDROP TABLE mydb.public.temp_table2;

SELECT * FROM mydb.public.temp_table2;



// Create a temp table with same name as Perm table
CREATE TEMPORARY TABLE mydb.public.emp_data(id number);
INSERT INTO mydb. public.emp_data VALUES(20);
INSERT INTO mydb.public.emp_data VALUES(47);
INSERT INTO mydb.public.emp_data VALUES(35);

// Execute this in two diff worksheets and observer results
SELECT * FROM mydb.public.emp_data;