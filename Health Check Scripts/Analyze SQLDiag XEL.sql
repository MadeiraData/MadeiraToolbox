/*

	!!! ATTENTION !!!
	
	THIS SCRIPT HAS BEEN DEPRECATED AND WILL NO LONGER BE MAINTAINED.
	Please use the new version at the new path:
	https://github.com/MadeiraData/MadeiraToolbox/blob/master/Health%20Check%20Scripts/Troubleshoot_high_THREADPOOL_waits_and_Deadlocked_Schedulers.sql

	!!! ATTENTION !!!


Analyze SQLDiag extended event files
=====================================
Author: Eitan Blumin , Madeira Data Solutions (https://www.madeiradata.com | https://www.eitanblumin.com)
Create Date: 2020-09-15
Description:
	This T-SQL script focuses on analyzing the "query_processing" components of SQLDiag files,
	or "sp_server_diagnostics_component_result" events in the "system_health" XE session.
	This can be useful for retroactively investigating Deadlocked Scheduler incidents, or high THREADPOOL waits.

	For more info:
	https://techcommunity.microsoft.com/t5/sql-server-support/the-tao-of-a-deadlock-scheduler-in-sql-server/ba-p/333991
	https://www.sqlskills.com/help/waits/threadpool/

Change Log:
	2020-09-29 Deprecated
	2020-09-27 Added support for querying from system_health instead of SQLDiag files; Added @PersistAllData, @ForceRingBuffer parameters.
	2020-09-24 Fixed backward compatibility bug, added @Verbose parameter.
	2020-09-21 Implemented backward-compatible version for SQL versions that don't support the AT TIME ZONE syntax.
	2020-09-21 Added #data and #alldata temp tables for persistence.
	2020-09-21 Added @LocalDateFrom and @LocalDateTo parameters.
	2020-09-21 Added aggregation with count on blockedProcesses and blockingProcesses using XQuery.
*/
DECLARE
	@FileTargetPath		NVARCHAR(256)	= '*_SQLDIAG_*.xel', -- Set to NULL to query from system_health instead.
	@Top			INT		= 1000, -- To avoid SSMS crashing, limit the output to the top X latest events.
	@LocalDateFrom		DATETIME	= NULL, -- Filter on a specific time range using the local or custom timezone.
	@LocalDateTo		DATETIME	= NULL, -- Filter on a specific time range using the local or custom timezone.
	@LocalTimeZone		VARCHAR(50)	= NULL, -- Set a custom timezone for the @LocalDateFrom and @LocalDateTo parameters. Supported in SQL 2016 and newer only.
	@Verbose		BIT		= 0,	-- Set to 1 to print out informational messages for debug purposes
	@PersistAllData		BIT		= 0,	-- Set to 1 to reuse previously created #alldata (if exists) instead of recreating it.
	@ForceRingBuffer	BIT		= 0	-- Set to 1 to force querying from system_health ring buffer, even if file target is available
	

/**********************************************************/
/******* NO NEED TO CHANGE ANYTHING BELOW THIS LINE *******/
/**********************************************************/

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
DECLARE
	@UtcDateFrom	DATETIME = NULL,
	@UtcDateTo	DATETIME = NULL,
	@CMD NVARCHAR(MAX);

-- If no @FileTargetPath was provided, try the system_health file target instead
IF @FileTargetPath IS NULL AND @ForceRingBuffer = 0
BEGIN
	SELECT @FileTargetPath = REPLACE(c.column_value, '.xel', '*.xel')
	FROM sys.dm_xe_sessions s
	JOIN sys.dm_xe_session_object_columns c
	ON s.address = c.event_session_address
	WHERE column_name = 'filename'
	AND s.name = 'system_health';

	IF @Verbose = 1 RAISERROR(N'Using system_health file target: %s',0,1,@FileTargetPath) WITH NOWAIT;
END

IF OBJECT_ID('tempdb..#alldata') IS NOT NULL AND @PersistAllData = 0 DROP TABLE #alldata;
IF OBJECT_ID('tempdb..#alldata') IS NULL
BEGIN
	CREATE TABLE #alldata
	(
		timestamp_utc DATETIME2,
		[object_name] SYSNAME,
		event_data_xml XML,
		[file_name] NVARCHAR(300),
		file_offset INT
	);
	CREATE CLUSTERED INDEX IX_TimeStamp ON #alldata (timestamp_utc DESC);
END
ELSE
	IF @Verbose = 1 RAISERROR(N'Re-using previously generated #alldata. To recreate, set @PersistAllData to 0.',0,1) WITH NOWAIT;


IF OBJECT_ID('tempdb..#data') IS NOT NULL DROP TABLE #data;
CREATE TABLE #data
(
	timestamp_utc DATETIME2,
	[object_name] SYSNAME,
	event_data_xml XML,
	[file_name] NVARCHAR(300),
	file_offset INT,
	timestamp_local AS (DATEADD(minute, DATEDIFF(minute, GETUTCDATE(), GETDATE()), timestamp_utc))
);
CREATE CLUSTERED INDEX IX_TimeStamp ON #data (timestamp_utc DESC);

-- If SQL 2016 and newer, use AT TIME ZONE syntax:
IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 13
BEGIN
	-- If no custom time zone specified, use the server local timezone
	IF @LocalTimeZone IS NULL
	BEGIN
		IF @Verbose = 1 RAISERROR(N'Getting local machine time zone',0,1) WITH NOWAIT;
		EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE',
		'SYSTEM\CurrentControlSet\Control\TimeZoneInformation',
		'TimeZoneKeyName',@LocalTimeZone OUT
	END

	IF @Verbose = 1 RAISERROR(N'Setting UTC parameters based on selected time zone "%s"',0,1, @LocalTimeZone) WITH NOWAIT;

	EXEC sp_executesql N'
	SET @UtcDateFrom = @LocalDateFrom AT TIME ZONE @LocalTimeZone AT TIME ZONE ''UTC'';
	SET @UtcDateTo = @LocalDateTo AT TIME ZONE @LocalTimeZone AT TIME ZONE ''UTC'';'
		, N'@LocalDateFrom DATETIME, @LocalDateTo DATETIME, @LocalTimeZone VARCHAR(50), @UtcDateFrom DATETIME OUTPUT, @UtcDateTo DATETIME OUTPUT'
		, @LocalDateFrom, @LocalDateTo, @LocalTimeZone, @UtcDateFrom OUTPUT, @UtcDateTo OUTPUT

	SET @CMD = N'ALTER TABLE #data DROP COLUMN timestamp_local;
	ALTER TABLE #data ADD timestamp_local AS (timestamp_utc AT TIME ZONE ''UTC'' AT TIME ZONE ' + QUOTENAME(@LocalTimeZone, '''') + N' )'
	IF @Verbose = 1 RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
	EXEC sp_executesql @CMD;

END
ELSE
BEGIN
	SET @UtcDateFrom = DATEADD(minute, DATEDIFF(minute, GETDATE(), GETUTCDATE()), @LocalDateFrom);
	SET @UtcDateTo = DATEADD(minute, DATEDIFF(minute, GETDATE(), GETUTCDATE()), @LocalDateTo);
END

-- If @FileTargetPath still not found, then query from system_health ring buffer
IF @FileTargetPath IS NULL OR @ForceRingBuffer = 1
BEGIN
	IF @Verbose = 1 RAISERROR(N'Querying from system_health ring buffer...',0,1) WITH NOWAIT;

	SET @CMD = N'IF OBJECT_ID(''tempdb..#ringbuffer'') IS NOT NULL DROP TABLE #ringbuffer;
CREATE TABLE #ringbuffer (event_data XML NULL);
INSERT INTO #ringbuffer
SELECT CAST(target_data AS XML) as event_data
FROM sys.dm_xe_session_targets AS st
INNER JOIN sys.dm_xe_sessions AS s ON s.[address] = st.event_session_address
WHERE [name] = ''system_health'';

SELECT @FileTargetPath = event_data.value(''(EventFileTarget/File/@name)[1]'', ''nvarchar(256)'')
FROM #ringbuffer
WHERE event_data.exist(''EventFileTarget/File'') = 1;

INSERT INTO #alldata
SELECT
  timestamp_utc
, [object_name] = ''sp_server_diagnostics_component_result''
, event_data_xml.query(''.'')
, [file_name] = @FileTargetPath
, file_offset = NULL
FROM #ringbuffer AS Data
CROSS APPLY event_data.nodes (''//RingBufferTarget/event'') AS XEventData (event_data_xml)
CROSS APPLY (SELECT timestamp_utc = event_data_xml.query(''.'').value(''(event/@timestamp)[1]'', ''datetime2'')) AS t
where event_data_xml.value(''@name'', ''varchar(4000)'') = ''sp_server_diagnostics_component_result''
AND event_data_xml.query(''.'').value(''(event/data[@name="component"]/text)[1]'', ''varchar(256)'') = ''query_processing''
OPTION(RECOMPILE);'
END
-- If SQL 2017 and newer, use timestamp_utc column from fn_xe_file_target_read_file:
ELSE IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 14
BEGIN
	IF @Verbose = 1 RAISERROR(N'Querying from file target using SQL2017+ syntax: %s',0,1,@FileTargetPath) WITH NOWAIT;

	SET @CMD = N'INSERT INTO #alldata
SELECT
  timestamp_utc
, [object_name]
, event_data_xml
, [file_name]
, file_offset
FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr
CROSS APPLY (SELECT event_data_xml = TRY_CONVERT(xml, event_data)) AS e
WHERE [object_name] IN (''sp_server_diagnostics_component_result'', ''component_health_result'', ''info_message'')
OPTION (RECOMPILE);'
END
ELSE
BEGIN
	IF @Verbose = 1 RAISERROR(N'Querying from file target using pre-SQL2017 syntax: %s',0,1,@FileTargetPath) WITH NOWAIT;

	SET @CMD = N'INSERT INTO #alldata
SELECT
  t.timestamp_utc
, [object_name]
, event_data_xml
, [file_name]
, file_offset
FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr
CROSS APPLY (SELECT event_data_xml = TRY_CONVERT(xml, event_data)) AS e
CROSS APPLY (SELECT timestamp_utc = event_data_xml.value(''(event/@timestamp)[1]'', ''datetime2'')) AS t
WHERE [object_name] IN (''sp_server_diagnostics_component_result'', ''component_health_result'', ''info_message'')
OPTION (RECOMPILE);'
END

IF NOT EXISTS (SELECT TOP 1 1 FROM #alldata)
BEGIN
	IF @Verbose = 1 RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
	EXEC sp_executesql @CMD, N'@FileTargetPath NVARCHAR(256)', @FileTargetPath;
	RAISERROR(N'Total diagnostic events found: %d',0,1,@@ROWCOUNT) WITH NOWAIT;
END

INSERT INTO #data
SELECT TOP (@Top)
  timestamp_utc
, [object_name]
, event_data_xml
, [file_name]
, file_offset
FROM #alldata
WHERE 
    (@UtcDateFrom IS NULL OR timestamp_utc >= @UtcDateFrom)
AND (@UtcDateTo IS NULL OR timestamp_utc <= @UtcDateTo)
AND ([object_name] = 'info_message'
	OR (
		[object_name] IN ('component_health_result','sp_server_diagnostics_component_result')
		AND
		'query_processing' IN (
			event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)'), -- SQLDiag version
			event_data_xml.value('(event/data[@name="component"]/text)[1]', 'varchar(256)') -- system_health version
			)
		)
	)
ORDER BY timestamp_utc DESC
OPTION (RECOMPILE);

RAISERROR(N'Filtered events found: %d',0,1,@@ROWCOUNT) WITH NOWAIT;

SELECT
  event_data_xml
, timestamp_utc
, timestamp_local
, [object_name]
, componentData		= COALESCE(event_data_xml.value('(event/data[@name="component"]/text)[1]', 'varchar(256)'), event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)'), event_data_xml.value('(event/data[@name="data"])[1]', 'varchar(256)'))
, maxWorkers		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@maxWorkers)[1]', 'int')
, workersCreated	= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@workersCreated)[1]', 'int')
, workersIdle		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@workersIdle)[1]', 'int')
, pendingTasks		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@pendingTasks)[1]', 'int')
, hasUnresolvableDeadlockOccurred = event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@hasUnresolvableDeadlockOccurred)[1]', 'int')
, hasDeadlockedSchedulersOccurred = event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@hasDeadlockedSchedulersOccurred)[1]', 'int')
, blockedProcesses = event_data_xml.query('
let $items := *//blocked-process-report/blocked-process/process/inputbuf
let $unique-items := distinct-values($items)
return
   <blocked totalCount="{count($items)}">   
      {
         for $item in $unique-items
		 let $count := count($items[. eq $item])
         return <item count="{$count}">{$item}</item>
      }
   </blocked>
')
, blockingProcesses = event_data_xml.query('
let $items := *//blocked-process-report/blocking-process/process/inputbuf
let $unique-items := distinct-values($items)
return
   <blocking totalCount="{count($items)}">   
      {
         for $item in $unique-items
		 let $count := count($items[. eq $item])
         return <item count="{$count}">{$item}</item>
      }
   </blocking>
')
, [file_name], file_offset
FROM #data
ORDER BY timestamp_utc DESC
