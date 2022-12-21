/*
Monitor for Application Reliability Errors and SQL Injection Attacks
====================================================================
Author: Eitan Blumin | https://madeiradata.com | https://eitanblumin.com
Date: 2022-10-18
Use this script for SQL Server IaaS/On-Prem/RDS/Managed Instances
*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
	 @MinutesBack int = 60
	,@MinimumEventsForFlush int = 100
	,@MinimumEventsForOutput int = 1

DECLARE @RCount bigint
SELECT @RCount = CAST(xet.target_data AS xml).value('(RingBufferTarget/@eventCount)[1]','bigint')
FROM sys.dm_xe_session_targets AS xet  
JOIN sys.dm_xe_sessions AS xe ON xe.address = xet.event_session_address
WHERE xe.name = 'DBSMART_Monitor_SuspiciousErrors'
AND target_name= 'ring_buffer'

IF @RCount IS NOT NULL RAISERROR(N'total events in ring buffer: %I64d',0,1,@RCount) WITH NOWAIT;

-- Retrieve buffer contents
IF OBJECT_ID('tempdb..#events') IS NOT NULL DROP TABLE #events
CREATE TABLE #events (event_xml XML);
INSERT INTO #events
SELECT X.query('.')
FROM (SELECT xdata = CAST(xet.target_data AS xml), CAST(xet.target_data AS xml).value('(RingBufferTarget/@totalEventsProcessed)[1]','int') AS evcount
FROM sys.dm_xe_session_targets AS xet  
JOIN sys.dm_xe_sessions AS xe ON xe.address = xet.event_session_address
WHERE xe.name = 'DBSMART_Monitor_SuspiciousErrors'
AND target_name= 'ring_buffer'
) AS a
CROSS APPLY xdata.nodes (N'//event') AS session_events (X)
WHERE session_events.X.value (N'(@timestamp)[1]' , N'datetime2(7)') > DATEADD(minute,-@MinutesBack,GETUTCDATE())
OPTION (RECOMPILE);

SET @RCount = ISNULL(@RCount, @@ROWCOUNT);
RAISERROR(N'recent events in ring buffer: %I64d',0,1,@RCount) WITH NOWAIT;

-- Unfurl raw data
SELECT
  server_name = CONVERT(sysname, SERVERPROPERTY('ServerName'))
, [database_name] = QUOTENAME([database_name])
, event_timestamp_from = MIN(event_timestamp_utc)
, event_timestamp_to = MAX(event_timestamp_utc)
, instance_count = COUNT(*)
, [error_number]
, [message]
, session_id
, client_app_name
, client_host_name
, client_process_id
, username
, sql_text
FROM (
SELECT
[database_name]  = session_events.event_xml.value (N'(event/action[@name="database_name"]/value)[1]' , N'NVARCHAR(1000)') ,
event_name  = session_events.event_xml.value (N'(event/@name)[1]' , N'NVARCHAR(1000)') ,
event_timestamp_utc = session_events.event_xml.value (N'(event/@timestamp)[1]' , N'datetime2(7)') ,
session_id  = session_events.event_xml.value (N'(event/action[@name="session_id"]/value)[1]' , N'NVARCHAR(1000)') ,
[error_number]  = session_events.event_xml.value (N'(event/data[@name="error_number"]/value)[1]' , N'NVARCHAR(1000)') ,
[message]   = session_events.event_xml.value (N'(event/data[@name="message"]/value)[1]' , N'NVARCHAR(MAX)') ,
client_app_name  = session_events.event_xml.value (N'(event/action[@name="client_app_name"]/value)[1]' , N'NVARCHAR(1000)') ,
client_host_name = session_events.event_xml.value (N'(event/action[@name="client_hostname"]/value)[1]' , N'NVARCHAR(1000)') ,
client_process_id = session_events.event_xml.value (N'(event/action[@name="client_pid"]/value)[1]' , N'NVARCHAR(1000)') ,
username  = session_events.event_xml.value (N'(event/action[@name="username"]/value)[1]' , N'NVARCHAR(1000)') ,
sql_text  = session_events.event_xml.value (N'(event/action[@name="sql_text"]/value)[1]' , N'NVARCHAR(1000)')
FROM #events AS session_events
) AS ev
WHERE @RCount >= @MinimumEventsForOutput
/* filter out non-critical apps */
AND (client_app_name NOT IN (
'Microsoft SQL Server Management Studio - Transact-SQL IntelliSense'
,'Microsoft SQL Server Management Studio - Query'
,'Microsoft SQL Server Management Studio'
,'SQLServerCEIP'
,'check_mssql_health'
,'DmvCollector'
,'SQL Server Performance Investigator'
,'DataGrip'))
AND (client_app_name NOT LIKE 'DBeaver%')
AND (client_app_name NOT LIKE 'azdata-%')
AND (client_app_name NOT LIKE 'SolarWinds%')
AND (client_app_name NOT LIKE 'Red Gate Software%')
AND (client_app_name NOT LIKE 'MonTarget%')
/* filter out known driver/ORM errors */
AND ([message] NOT LIKE '%Invalid object name ''dbo.__MigrationHistory''.%')
AND ([message] NOT LIKE '%Invalid object name ''dbo.EdmMetadata''.%')
AND ([message] NOT LIKE '%Invalid object name ''MSysConf''.%')
GROUP BY [database_name], [error_number], [message], session_id, client_app_name, client_host_name, client_process_id, username, sql_text
OPTION (RECOMPILE);

-- Recreate session to flush buffer
IF @RCount > @MinimumEventsForFlush AND EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = 'DBSMART_Monitor_SuspiciousErrors')
BEGIN
PRINT N'Dropping the event session...'
DROP EVENT SESSION [DBSMART_Monitor_SuspiciousErrors] ON SERVER;
END

IF NOT EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = 'DBSMART_Monitor_SuspiciousErrors')
BEGIN
PRINT N'Creating the event session...'
CREATE EVENT SESSION [DBSMART_Monitor_SuspiciousErrors] ON SERVER

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
WHERE ([package0].[equal_boolean]([sqlserver].[is_system],(0)))
AND [sqlserver].[sql_text]<>N''
AND ((
 error_number = (102)
 OR error_number = (105)
 OR error_number = (205)
 OR error_number = (207)
 OR error_number = (208)
 OR error_number = (245)
 OR error_number = (2812)
 --OR error_number = (18456)
 OR error_number = (15281)
    )
 OR
    (
    [severity]>(10) 
    AND (
    sqlserver.like_i_sql_unicode_string([message], N'%permission%')
    OR sqlserver.like_i_sql_unicode_string([message], N'%denied%')
     )
    )
 OR
    (
      error_number = (2628) AND
      (
       [message] LIKE 'String or binary data would be truncated in table ''%'', column ''%''. Truncated value: ''%''%union%'''
       OR [message] LIKE 'String or binary data would be truncated in table ''%'', column ''%''. Truncated value: ''%''%select%'''
       OR [message] LIKE 'String or binary data would be truncated in table ''%'', column ''%''. Truncated value: ''%''%when%'''
       OR [message] LIKE 'String or binary data would be truncated in table ''%'', column ''%''. Truncated value: ''%''%from%'''
       OR [message] LIKE 'String or binary data would be truncated in table ''%'', column ''%''. Truncated value: ''%''%case%'''
       OR [message] LIKE 'String or binary data would be truncated in table ''%'', column ''%''. Truncated value: ''%''%0x[0123456789]%'''
      )
    )
 )
/* filter out non-critical apps */
AND (sqlserver.client_app_name <> 'Microsoft SQL Server Management Studio - Transact-SQL IntelliSense')
AND (sqlserver.client_app_name <> 'Microsoft SQL Server Management Studio - Query')
AND (sqlserver.client_app_name <> 'Microsoft SQL Server Management Studio')
AND (sqlserver.client_app_name <> 'SQLServerCEIP')
AND (sqlserver.client_app_name <> 'check_mssql_health')
AND (sqlserver.client_app_name <> 'DmvCollector')
AND (sqlserver.client_app_name <> 'SQL Server Performance Investigator')
AND (sqlserver.client_app_name <> 'DataGrip')
AND (sqlserver.client_app_name NOT LIKE 'DBeaver%')
AND (sqlserver.client_app_name NOT LIKE 'azdata-%')
AND (sqlserver.client_app_name NOT LIKE 'SolarWinds%')
AND (sqlserver.client_app_name NOT LIKE 'Red Gate Software%')
AND (sqlserver.client_app_name NOT LIKE 'MonTarget%')
/* filter out known driver/ORM errors */
AND ([message] NOT LIKE '%Invalid object name ''dbo.__MigrationHistory''.%')
AND ([message] NOT LIKE '%Invalid object name ''dbo.EdmMetadata''.%')
AND ([message] NOT LIKE '%Invalid object name ''MSysConf''.%')
)
ADD TARGET package0.ring_buffer(SET max_events_limit=(200),max_memory=(4096))
WITH (MAX_MEMORY=4 MB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)
;
END

-- Start the event session
IF NOT EXISTS (SELECT * FROM sys.dm_xe_sessions WHERE name = 'DBSMART_Monitor_SuspiciousErrors')
BEGIN
PRINT N'Starting the event session...'
ALTER EVENT SESSION [DBSMART_Monitor_SuspiciousErrors] ON SERVER STATE = START;
END