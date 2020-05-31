-- Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
-- Date: 2020-05-31
-- Description: Collect T-SQL Error Events using an Extended Events Buffer

SET NOCOUNT ON;

DECLARE
  @SourceLinkedServer SYSNAME
, @MinimumSeverity INT
, @BufferMaxMemoryMB INT
, @BufferMaxEventsCount INT
, @BufferXESessionName SYSNAME
, @PrintOnly BIT
, @FlushBuffer BIT

DECLARE @ProgramsToIgnore AS TABLE(appname SYSNAME);

/******************** CONFIGURATION ********************/

SET @SourceLinkedServer		= NULL			-- Optionally place a linked server name here. Set as NULL to monitor the local server.
SET @MinimumSeverity		= 11			-- Set minimum threshold of error severity to capture
SET @BufferMaxMemoryMB		= 16			-- Max memory in MB for the ring buffer
SET @BufferMaxEventsCount	= 50000			-- Max number of events to hold between flushes
SET @BufferXESessionName	= 'CaptureTSQLErrors'	-- Name of the extended events session
SET @FlushBuffer		= 1			-- Set to 1 to flush buffer between executions, 0 to keep data
SET @PrintOnly			= 0			-- Set to 1 to get the command in text as output, instead of running it

INSERT INTO @ProgramsToIgnore
SELECT 'Microsoft SQL Server Management Studio - Transact-SQL IntelliSense'
UNION ALL SELECT 'SQLServerCEIP'
UNION ALL SELECT 'check_mssql_health'
UNION ALL SELECT 'SQL Server Performance Investigator'

/******************** /CONFIGURATION ********************/

DECLARE @CMD NVARCHAR(MAX), @Filters NVARCHAR(MAX), @ProcFilters NVARCHAR(MAX), @Executor NVARCHAR(1000)
DECLARE @IsAzureSQLDB BIT, @IsNestedTransaction BIT

SET @Executor =  ISNULL(QUOTENAME(@SourceLinkedServer) + N'...', N'') + N'sp_executesql'
SET @CMD = N'SET @IsAzureSQLDB = CASE WHEN SERVERPROPERTY(''Edition'') = ''SQL Azure'' THEN 1 ELSE 0 END;'

EXEC @Executor @CMD, N'@IsAzureSQLDB BIT OUTPUT', @IsAzureSQLDB OUTPUT;

SET @Filters = N'([package0].[equal_boolean]([sqlserver].[is_system],(0)))
AND [severity]>=(' + CONVERT(nvarchar(max),@MinimumSeverity) + N') AND [sqlserver].[sql_text]<>N''''
'

SELECT @Filters = @Filters + CHAR(10) + N'AND (sqlserver.client_app_name <> ''' + REPLACE(appname, N'''', N'''''') + N''')'
FROM @ProgramsToIgnore

SetUpCommand:

IF @IsNestedTransaction = 1 OR (@IsAzureSQLDB = 1 AND @SourceLinkedServer IS NULL)
BEGIN
	SET @PrintOnly = 1
	RAISERROR(N'This database version is not supported for dynamic execution. Please take the generated script in attached results instead and run it directly.'
		, 10, 0);
END

IF @PrintOnly = 1
BEGIN
SET @CMD = N'-- Retrieve buffer contents
IF OBJECT_ID(''tempdb..#events'') IS NOT NULL DROP TABLE #events
CREATE TABLE #events (event_xml XML);
INSERT INTO #events
SELECT X.query(''.'')
FROM (SELECT xdata = CAST(xet.target_data AS xml)
FROM sys.dm_xe_' + CASE WHEN @IsAzureSQLDB = 1 THEN N'database_' ELSE N'' END + N'session_targets AS xet  
JOIN sys.dm_xe_' + CASE WHEN @IsAzureSQLDB = 1 THEN N'database_' ELSE N'' END + N'sessions AS xe ON (xe.address = xet.event_session_address)  
WHERE xe.name = ''' + @BufferXESessionName + N'''
AND target_name= ''ring_buffer''
) AS a
CROSS APPLY xdata.nodes (N''//event'') AS session_events (X)

-- Unfurl raw data
SELECT
[server_name]		= @@SERVERNAME,
[database_name]		= session_events.event_xml.value (N''(event/action[@name="database_name"]/value)[1]'' , N''SYSNAME'') ,
event_name		= session_events.event_xml.value (N''(event/@name)[1]'' , N''NVARCHAR(1000)'') ,
event_timestamp_utc	= session_events.event_xml.value (N''(event/@timestamp)[1]'' , N''DATETIME2(7)'') ,
session_id		= session_events.event_xml.value (N''(event/action[@name="session_id"]/value)[1]'' , N''INT'') ,
error_number		= session_events.event_xml.value (N''(event/data[@name="error_number"]/value)[1]'' , N''INT'') ,
severity		= session_events.event_xml.value (N''(event/data[@name="severity"]/value)[1]'' , N''INT'') ,
state			= session_events.event_xml.value (N''(event/data[@name="state"]/value)[1]'' , N''INT'') ,
category		= session_events.event_xml.value (N''(event/data[@name="category"]/value)[1]'' , N''INT'') ,
category_desc		= session_events.event_xml.value (N''(event/data[@name="category"]/text)[1]'' , N''NVARCHAR(MAX)'') ,
message			= session_events.event_xml.value (N''(event/data[@name="message"]/value)[1]'' , N''NVARCHAR(MAX)'') ,
client_app_name		= session_events.event_xml.value (N''(event/action[@name="client_app_name"]/value)[1]'' , N''NVARCHAR(1000)'') ,
client_host_name	= session_events.event_xml.value (N''(event/action[@name="client_hostname"]/value)[1]'' , N''NVARCHAR(1000)'') ,
client_process_id	= session_events.event_xml.value (N''(event/action[@name="client_pid"]/value)[1]'' , N''BIGINT'') ,
username		= session_events.event_xml.value (N''(event/action[@name="username"]/value)[1]'' , N''SYSNAME'') ,
sql_text		= session_events.event_xml.value (N''(event/action[@name="sql_text"]/value)[1]'' , N''NVARCHAR(MAX)'')
,event_xml
FROM #events AS session_events;
'
END
ELSE
BEGIN
SET @CMD = N'-- Retrieve buffer contents
SELECT CAST(xet.target_data AS varbinary(max))
FROM sys.dm_xe_' + CASE WHEN @IsAzureSQLDB = 1 THEN N'database_' ELSE N'' END + N'session_targets AS xet  
JOIN sys.dm_xe_' + CASE WHEN @IsAzureSQLDB = 1 THEN N'database_' ELSE N'' END + N'sessions AS xe ON (xe.address = xet.event_session_address)  
WHERE xe.name = ''' + @BufferXESessionName + N'''
AND target_name= ''ring_buffer''
'
END

IF @FlushBuffer = 1
BEGIN
SET @CMD = @CMD + N'
-- Recreate session to flush buffer
IF EXISTS (SELECT * FROM sys.' + CASE WHEN @IsAzureSQLDB = 1 THEN N'database' ELSE N'server' END + N'_event_sessions WHERE name = ''' + @BufferXESessionName + N''')
BEGIN
DROP EVENT SESSION ' + QUOTENAME(@BufferXESessionName) + N' ON ' + CASE WHEN @IsAzureSQLDB = 1 THEN N'DATABASE' ELSE N'SERVER' END + N';
END
'
END

SET @CMD = @CMD + N'
IF NOT EXISTS (SELECT * FROM sys.' + CASE WHEN @IsAzureSQLDB = 1 THEN N'database' ELSE N'server' END + N'_event_sessions WHERE name = ''' + @BufferXESessionName + N''')
BEGIN
-- Create the event session
CREATE EVENT SESSION ' + QUOTENAME(@BufferXESessionName) + N' ON ' + CASE WHEN @IsAzureSQLDB = 1 THEN N'DATABASE' ELSE N'SERVER' END + N'

ADD EVENT sqlserver.error_reported(
ACTION(
sqlserver.client_app_name,
sqlserver.client_hostname,
sqlserver.client_pid,
sqlserver.username,
sqlserver.database_name,
sqlserver.nt_username,
sqlserver.session_id,
sqlserver.sql_text
)
WHERE ' + @Filters + ISNULL(@ProcFilters, N'') + N'
)

ADD TARGET package0.ring_buffer(SET max_events_limit=(' + CONVERT(nvarchar, @BufferMaxEventsCount) + N'),max_memory=(' + CONVERT(nvarchar, @BufferMaxMemoryMB*1024) + N'))
WITH (MAX_MEMORY=' + CONVERT(nvarchar, @BufferMaxMemoryMB) + N' MB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=PER_CPU,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)
;
END

-- Start the event session
IF NOT EXISTS (SELECT * FROM sys.dm_xe' + CASE WHEN @IsAzureSQLDB = 1 THEN N'_database' ELSE N'' END + N'_sessions WHERE name = ''' + @BufferXESessionName + N''')
BEGIN
ALTER EVENT SESSION ' + QUOTENAME(@BufferXESessionName) + N' ON ' + CASE WHEN @IsAzureSQLDB = 1 THEN N'DATABASE' ELSE N'SERVER' END + N' STATE = START;
END'

IF @PrintOnly = 1
BEGIN
	SELECT @CMD AS GeneratedScript
END
ELSE
BEGIN
IF OBJECT_ID('tempdb..#xe') IS NOT NULL DROP TABLE #xe;
CREATE TABLE #xe (xdata VARBINARY(MAX));
BEGIN TRY
	-- Query the event session data
	INSERT INTO #xe
	EXEC @Executor @CMD
END TRY
BEGIN CATCH
	DECLARE @ERRNum INT = ERROR_NUMBER()
	PRINT CONCAT(N'Error ', @ERRNum, N', Line ', ERROR_LINE(), N': ', ERROR_MESSAGE());

	-- Error 574: statement cannot be used inside a user transaction.
	IF @ERRNum = 574
	BEGIN
		SET @PrintOnly = 1;
		SET @IsNestedTransaction = 1;
		GOTO SetUpCommand;
	END
END CATCH

IF OBJECT_ID('tempdb..#events') IS NOT NULL DROP TABLE #events
CREATE TABLE #events (event_xml XML);
INSERT INTO #events
SELECT X.query('.')
FROM (SELECT xdata = CONVERT(xml, xdata) FROM #xe) AS a
CROSS APPLY xdata.nodes (N'//event') AS session_events (X)

-- Unfurl raw data
SELECT
[server_name]		= @@SERVERNAME,
[database_name]		= session_events.event_xml.value (N'(event/action[@name="database_name"]/value)[1]' , N'SYSNAME') ,
event_name		= session_events.event_xml.value (N'(event/@name)[1]' , N'NVARCHAR(1000)') ,
event_timestamp_utc	= session_events.event_xml.value (N'(event/@timestamp)[1]' , N'DATETIME2(7)') ,
session_id		= session_events.event_xml.value (N'(event/action[@name="session_id"]/value)[1]' , N'INT') ,
error_number		= session_events.event_xml.value (N'(event/data[@name="error_number"]/value)[1]' , N'INT') ,
severity		= session_events.event_xml.value (N'(event/data[@name="severity"]/value)[1]' , N'INT') ,
state			= session_events.event_xml.value (N'(event/data[@name="state"]/value)[1]' , N'INT') ,
category		= session_events.event_xml.value (N'(event/data[@name="category"]/value)[1]' , N'INT') ,
category_desc		= session_events.event_xml.value (N'(event/data[@name="category"]/text)[1]' , N'NVARCHAR(MAX)') ,
message			= session_events.event_xml.value (N'(event/data[@name="message"]/value)[1]' , N'NVARCHAR(MAX)') ,
client_app_name		= session_events.event_xml.value (N'(event/action[@name="client_app_name"]/value)[1]' , N'NVARCHAR(1000)') ,
client_host_name	= session_events.event_xml.value (N'(event/action[@name="client_hostname"]/value)[1]' , N'NVARCHAR(1000)') ,
client_process_id	= session_events.event_xml.value (N'(event/action[@name="client_pid"]/value)[1]' , N'BIGINT') ,
username		= session_events.event_xml.value (N'(event/action[@name="username"]/value)[1]' , N'SYSNAME') ,
sql_text		= session_events.event_xml.value (N'(event/action[@name="sql_text"]/value)[1]' , N'NVARCHAR(MAX)')
,event_xml
FROM #events AS session_events;

DROP TABLE #events;
END