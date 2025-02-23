// CHAGNING THE DELIMITER AND TESTING THE DATA LOADING
CREATE OR REPLACE FILE FORMAT MYDB.FILE_FORMATS.FILE_FORMAT_PIPE
TYPE= CSV
FIELD_DELIMITER= ','
SKIP_HEADER=1 
EMPTY_FIELD_AS_NULL= TRUE
error_on_column_count_mismatch=false;

//RUN COPY COMMAND TO TEST DATA LOADING
COPY INTO MYDB.PUBLIC.EMPLOYESS_PIPE 
FROM @MYDB.EXTERNAL_STAGES.AWS_STG_PIPE
PATTERN= '.*employees.*';

//STEP 1 IS CHECK PIPE STATUS
SELECT SYSTEM$PIPE_STATUS('MYDB.PIPE.AWS_PIPES');

ALTER PIPE MYDB.PIPE.AWS_PIPES SET PIPE_EXECUTION_PAUSED= FALSE;

/*{"executionState":"RUNNING","pendingFileCount":0,"lastReceivedMessageTimestamp":"2025-02-17T18:34:58.949Z","lastForwardedMessageTimestamp":"2025-02-17T18:34:59.813Z"*/

--THESE FIELDS ARE VERY MUCH IMPORTANT WE NEED TO CHECK THE LastReceivedMessageTimeStamp and lastForwardedMessageTimestamp.
--if they are not same as the event message notification timestamp and time stamp at which snowflake forwards the create object to pipe then we need to check arn and paths at noth stage and the notification channel.

--NOW LETS PUT A NEW EMPLOYEE FILE AT SNOWFLAKE AND CHECK THE PIPE STATUS
SELECT SYSTEM$PIPE_STATUS('MYDB.PIPE.AWS_PIPES');

--{"executionState":"RUNNING","pendingFileCount":0,"lastReceivedMessageTimestamp":"2025-02-18T02:52:18.948Z","lastForwardedMessageTimestamp":"2025-02-18T02:52:19.404Z"
-- Since the the pipe ran smoothly, the timestamps got updated and data loaded to table successfully.

//STEP 2 IS VIEWING THE COPY HISTORY, this will show the status of table and first error message

SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY
 (
 TABLE_NAME  => 'MYDB.PUBLIC.EMPLOYESS_PIPE',
  START_TIME => DATEADD(HOUR, -1 ,CURRENT_TIMESTAMP()))
);

//step 3 is validate the pipe, which will show the errors in the data file 


SELECT * FROM TABLE(INFORMATION_SCHEMA.VALIDATE_PIPE_LOAD
 (PIPE_NAME  => 'MYDB.PIPE.AWS_PIPES',
  START_TIME => DATEADD(HOUR, -1 ,CURRENT_TIMESTAMP()))
);

// what ever files were not uploaded we need to run the copy command manually for the first run and from next on pipe will take care of the load after fixing the error
COPY INTO MYDB.PUBLIC.EMPLOYESS_PIPE 
FROM @MYDB.EXTERNAL_STAGES.AWS_STG_PIPE
PATTERN= '.*employees.*';

// How to see pipes?
DESC PIPE employee_pipe;

SHOW PIPES;
SHOW PIPES like '%employee%';
SHOW PIPES in database mydb;
SHOW PIPES in schema mydb.pipes;
SHOW PIPES like '%employee%' in Database mydb;

// How to pause a pipe
ALTER PIPE mydb.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = true;


// Want to modify the copy command, before this we need make sure that pipe is paused.
CREATE OR REPLACE pipe mydb.pipes.employee_pipe
auto_ingest = TRUE
AS
COPY INTO mydb.public.emp_data2
FROM @mydb.external_stages.stage_aws_pipes
pattern = '.*employee.*'; 


// How to resume the pipe
ALTER PIPE mydb.pipes.employee_pipe SET PIPE_EXECUTION_PAUSED = false;






