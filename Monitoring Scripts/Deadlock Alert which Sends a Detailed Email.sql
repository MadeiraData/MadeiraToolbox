/*========================================================================================================================
Description:	Create Agent Alert for Deadlock events and collect the XML information for that Deadlock
				from the XE system session and send it by email.

Written By:		Guy Yaakobovitch, Madeira Data Solutions
Created:		23/03/2021
Last Updated:	00/00/0000
Notes:			
=========================================================================================================================*/

-- Create a Procedure which collect the deadlock details and send it by email.
USE
	DBA
GO
CREATE OR ALTER PROCEDURE usp_Deadlock_Alert
AS
SET NOCOUNT ON;
WAITFOR DELAY '00:00:30.000';
DECLARE @XML NVARCHAR(MAX), 
		@DateTime DATETIME2,
		@Profile NVARCHAR(20),
		@EmailTo NVARCHAR(50),
		@tag VARCHAR (MAX), 
		@path VARCHAR(MAX),
		@ServerName sysname,
		@Subject NVARCHAR(200),
		@body NVARCHAR(MAX)
		;
DROP TABLE IF EXISTS #DeadlockReport;
DROP TABLE IF EXISTS  #errorlog;

CREATE TABLE #errorlog (
            LogDate DATETIME 
            , ProcessInfo VARCHAR(100)
            , [Text] VARCHAR(MAX)
            );

INSERT INTO #errorlog EXEC sp_readerrorlog;
SELECT @tag = text
FROM #errorlog 
WHERE [Text] LIKE 'Logging%MSSQL\Log%';
DROP TABLE #errorlog;
SET @path = SUBSTRING(@tag, 38, CHARINDEX('MSSQL\Log', @tag) - 29);
SELECT TOP 1
  CONVERT(xml, event_data).query('/event/data/value/child::*') AS DeadlockReport,
  CONVERT(xml, event_data).value('(event[@name="xml_deadlock_report"]/@timestamp)[1]', 'datetime') 
  AS Execution_Time_In_UTC
INTO #DeadlockReport
FROM sys.fn_xe_file_target_read_file(@path + '\system_health*.xel', NULL, NULL, NULL)
WHERE OBJECT_NAME like 'xml_deadlock_report'
ORDER BY CONVERT(xml, event_data).value('(event[@name="xml_deadlock_report"]/@timestamp)[1]', 'datetime');

SELECT @XML = CAST(DeadlockReport AS NVARCHAR(MAX)), @DateTime = Execution_Time_In_UTC 
FROM #DeadlockReport;

SET @XML = 'The following XML is an SQL deadlock graph which can be viewd by copy XML data to the SSMS editor and save it
	with the .xdl extention and then reopen the file using SSMS. Second option is to read it directly from the XML which can 
	be more convenient using the SSMS or Visual Studio.
	Here is a link on how to read that XML like a pro:
	https://www.sqlshack.com/understanding-the-xml-description-of-the-deadlock-graph-in-sql-server/
	' + @XML

SET @Profile = (select name from msdb.dbo.sysmail_profile WHERE name = 'ISRL-mail');
SET @EmailTo = (SELECT email_address FROM msdb.dbo.sysoperators WHERE name = 'eranf');
SET @ServerName = (SELECT @@SERVERNAME);
SET @Subject = N'A deadlock occured at ' + CAST(@DateTime AS NVARCHAR(25)) + ' at server: ' + @ServerName

-- Debug
--PRINT @Profile
--PRINT @EmailTo
--PRINT @ServerName
--PRINT @Subject
--SELECT @XML

EXEC msdb.dbo.sp_send_dbmail
	@profile_name = @profile,
	@recipients = @EmailTo,
	@body =  @XML,
	@Subject = @Subject

DROP TABLE #DeadlockReport;
GO

-- Create A Job that executed if a deadlock Alert occured which execute the procedure that collect the deadlock information and send it by email.
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Deadlock Alert', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'This job is executing by the deadlock alert there for not containing any scheduler configured.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Exec Store Procedure', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=2, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE usp_Deadlock_Alert;', 
		@database_name=N'DBA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

-- Create The Alert
USE [msdb]
GO
EXEC msdb.dbo.sp_add_alert @name=N'Deadlcok Detected - Error 1205', 
		@message_id=1205, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=1, 
		@category_name=N'[Uncategorized]', 
		@job_id=N'f3a7ed62-42d7-4387-9667-8a07ce8fadc4'
GO



