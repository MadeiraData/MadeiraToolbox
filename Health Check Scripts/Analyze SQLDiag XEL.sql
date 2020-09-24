/*
Analyze SQLDiag extended event files
=====================================
Author: Eitan Blumin , Madeira Data Solutions (https://www.madeiradata.com | https://www.eitanblumin.com)
Create Date: 2020-09-15
Description:
	This T-SQL script focuses on analyzing the "query_processing" components of SQLDiag files.
	This can be useful for investigating Deadlocked Scheduler incidents, or high THREADPOOL waits.

	For more info:
	https://techcommunity.microsoft.com/t5/sql-server-support/the-tao-of-a-deadlock-scheduler-in-sql-server/ba-p/333991
	https://www.sqlskills.com/help/waits/threadpool/

Change Log:
	2020-09-24 Fixed backward compatibility bug, added @Verbose parameter
	2020-09-21 Implemented backward-compatible version for SQL versions that don't support the AT TIME ZONE syntax.
	2020-09-21 Added #data and #alldata temp tables for persistence.
	2020-09-21 Added @LocalDateFrom and @LocalDateTo parameters.
	2020-09-21 Added aggregation with count on blockedProcesses and blockingProcesses using XQuery.
*/
DECLARE
	@FileTargetPath		NVARCHAR(256)	= '*_SQLDIAG_*.xel',
	@Top			    INT		= 1000,
	@LocalDateFrom		DATETIME	= NULL,
	@LocalDateTo		DATETIME	= NULL,
	@LocalTimeZone		VARCHAR(50)	= NULL, -- Supported in SQL 2016 and newer only
	@Verbose		BIT		= 0
	
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
DECLARE
	@UtcDateFrom	DATETIME = NULL,
	@UtcDateTo	DATETIME = NULL,
	@CMD NVARCHAR(MAX);

IF OBJECT_ID('tempdb..#alldata') IS NOT NULL DROP TABLE #alldata;
CREATE TABLE #alldata
(
	timestamp_utc DATETIME2,
	[object_name] SYSNAME,
	event_data_xml XML,
	[file_name] NVARCHAR(300),
	file_offset INT
);
CREATE CLUSTERED INDEX IX_TimeStamp ON #alldata (timestamp_utc DESC);

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

-- If SQL 2017 and newer, use timestamp_utc column from fn_xe_file_target_read_file:
IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 14
BEGIN
	SET @CMD = N'INSERT INTO #alldata
SELECT
  timestamp_utc
, [object_name]
, event_data_xml
, [file_name]
, file_offset
FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr
CROSS APPLY (SELECT event_data_xml = TRY_CONVERT(xml, event_data)) AS e
OPTION (RECOMPILE);'
END
ELSE
BEGIN
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
OPTION (RECOMPILE);'
END

IF @Verbose = 1 RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
EXEC sp_executesql @CMD, N'@FileTargetPath NVARCHAR(256)', @FileTargetPath;
RAISERROR(N'Total events found in SQLDiag files: %d',0,1,@@ROWCOUNT) WITH NOWAIT;

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
AND ([object_name] <> 'component_health_result'
	OR (
		[object_name] = 'component_health_result'
		AND
		event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)') = 'query_processing'
		)
	)
ORDER BY timestamp_utc DESC;

RAISERROR(N'Filtered events found: %d',0,1,@@ROWCOUNT) WITH NOWAIT;

SELECT
  event_data_xml
, timestamp_utc
, timestamp_local
, [object_name]
, componentData		= ISNULL(event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)'), event_data_xml.value('(event/data[@name="data"])[1]', 'varchar(256)'))
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