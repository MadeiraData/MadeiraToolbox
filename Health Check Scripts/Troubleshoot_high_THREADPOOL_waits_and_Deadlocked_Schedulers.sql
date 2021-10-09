/*
Troubleshoot high THREADPOOL waits and Deadlocked Schedulers
============================================================
Author: Eitan Blumin , Madeira Data Solutions (https://www.madeiradata.com | https://www.eitanblumin.com)
Create Date: 2020-09-15
Description:
	This T-SQL script focuses on analyzing the "query_processing" components of SQLDiag files, or
	the "sp_server_diagnostics_component_result" events (such as in the "system_health" XE session).
	This can be useful for retroactively investigating Deadlocked Scheduler incidents, or high THREADPOOL waits.

	This script is only supported on SQL Server versions 2012 and newer.

	For more info:
	https://eitanblumin.com/2020/10/05/how-to-troubleshoot-threadpool-waits-and-deadlocked-schedulers/
	https://techcommunity.microsoft.com/t5/sql-server-support/the-tao-of-a-deadlock-scheduler-in-sql-server/ba-p/333991
	https://www.sqlskills.com/help/waits/threadpool/
	https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-server-diagnostics-transact-sql
	https://docs.microsoft.com/en-us/sql/relational-databases/extended-events/use-the-system-health-session

Change Log:
	2021-10-09 Added proper check to distinguish between Azure SQL DB and Azure SQL Managed Instance
	2020-12-13 Added parameter @ForceRingBuffer for better support with Azure SQL servers
	2020-10-04 Added parameters @ExcludeClean and @MinPendingTasks
	2020-10-01 Added columns "blockedByNonSession" and "possibleHeadBlockers"
	2020-09-28 Added detailed explanations for every parameter and some additional info in comments.
	2020-09-28 Output improvements.
	2020-09-28 Added persistence of @FileTargetPath for smart validation of @PersistAllData.
	2020-09-27 Added support for querying from system_health instead of SQLDiag files; Added @PersistAllData.
	2020-09-24 Fixed backward compatibility bug, added @Verbose parameter.
	2020-09-21 Implemented backward-compatible version for SQL versions that don't support the AT TIME ZONE syntax.
	2020-09-21 Added #data and #alldata temp tables for persistence.
	2020-09-21 Added @LocalDateFrom and @LocalDateTo parameters.
	2020-09-21 Added aggregation with count on blockedProcesses and blockingProcesses using XQuery.
*/
DECLARE
	----------------------------------
	-- @FileTargetPath
	----------------------------------
	-- Specify the file path qualifier for the XEL files to read. You can use asterisk * as a wildcard.
	-- You can specify a specific path, for example: 'C:\MyLogs\SERVER1_SQLDIAG_*.xel'
	-- Or, you can ommit the folder path entirely to read from the SQL Server's configured LOG folder, for example: '*_SQLDIAG_*.xel'
	-- Or, you can set to NULL to query from the "system_health" session instead.
	 @FileTargetPath	NVARCHAR(256)	=
						NULL
						--'*_SQLDIAG_*.xel'
						--'C:\Temp\SQL-PROD-1*_SQLDIAG_*.xel'
	
	
	----------------------------------
	-- @ForceRingBuffer
	----------------------------------
	-- Force querying from the system_health ring buffer, even if it has a file target.
	,@ForceRingBuffer	BIT		= 0

	----------------------------------
	-- @Top
	----------------------------------
	-- Prevent SSMS from crashing by limiting the output to the top X latest events.
	-- Please be careful and do not set this value too high!
	,@Top			INT		= 1000

	----------------------------------
	-- @LocalDateFrom, @LocalDateTo
	----------------------------------
	-- Filter on a specific time range using the local server timezone or a custom timezone.
	-- You can also set either or both parameters to NULL to query all available time range.
	,@LocalDateFrom		DATETIME	= NULL
	,@LocalDateTo		DATETIME	= NULL

	----------------------------------
	-- @LocalTimeZone
	----------------------------------
	-- Optionally set a custom timezone for the @LocalDateFrom and @LocalDateTo parameters.
	-- For example: 'Israel Standard Time'
	-- Set to NULL to use the local server timezone.
	-- Using a custom timezone is supported in SQL Server 2016 and newer ONLY.
	,@LocalTimeZone		VARCHAR(50)	= NULL
	
	----------------------------------
	-- @ExcludeClean
	----------------------------------
	-- Optionally limit to only "warning" or "error" component states, thus ignoring the "clean" ones.
	-- Can be useful for pinpointing problematic incidents specifically.
	-- Set to 0 or NULL to show all data.
	,@ExcludeClean		BIT		= 0

	----------------------------------
	-- @MinPendingTasks
	----------------------------------
	-- Optionally limit results only to those with a minimal number of pendingTasks.
	-- Can be useful for pinpointing problematic incidents specifically.
	-- Set to 0 or NULL to show all data.
	,@MinPendingTasks	INT		= 0

	----------------------------------
	-- @PersistAllData
	----------------------------------
	-- This script first imports all available data (based on @FileTargetPath) into a temp table called #alldata.
	-- This first phase could take a long while if you have a lot of event data to read.
	-- You can set @PersistAllData to 1 if you want to reuse previously created #alldata (if exists) instead of recreating it.
	-- This could be useful when you want to traverse the same file(s) but filter on a different time range, for example.
	-- Thus, it would skip the first phase entirely and save you some valuable time.
	-- However, if you change the value of @FileTargetPath between executions, then #alldata will be recreated regardless.
	,@PersistAllData	BIT		= 1
	
	----------------------------------
	-- @Verbose
	----------------------------------
	-- Set this parameter to 1 to print out informational messages for debug purposes.
	,@Verbose		BIT		= 1


/**********************************************************/
/******* NO NEED TO CHANGE ANYTHING BELOW THIS LINE *******/
/**********************************************************/

SET NOEXEC OFF;
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @sqlmajorver INT
SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

IF @sqlmajorver < 11
BEGIN
	RAISERROR(N'Sorry, this script is only supported on SQL Server versions 2012 and newer.',16,1);
	SET NOEXEC ON;
END

DECLARE
	@UtcDateFrom	DATETIME = NULL,
	@UtcDateTo	DATETIME = NULL,
	@CMD		NVARCHAR(MAX);

IF @ForceRingBuffer = 0 AND CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
BEGIN
	SET @ForceRingBuffer = 1;
	SET @FileTargetPath = NULL;
	RAISERROR(N'This Azure SQL edition only supports ring_buffer targets.',0,1) WITH NOWAIT;
END

-- If @PersistAllData was enabled and a file target path specified
IF @PersistAllData = 1
BEGIN
	-- Check for an already persisted @FileTargetPath
	IF OBJECT_ID('tempdb..#fileTargetPersisted') IS NOT NULL
	BEGIN
		DECLARE @PersistedFileTargetPath NVARCHAR(255);

		SET @CMD = N'
		SELECT @PersistedFileTargetPath = FileTargetPath
		FROM #fileTargetPersisted
		OPTION (RECOMPILE);'
		EXEC sp_executesql @CMD, N'@PersistedFileTargetPath NVARCHAR(255) OUTPUT', @PersistedFileTargetPath OUTPUT;
		
		IF @FileTargetPath IS NULL AND (@PersistedFileTargetPath IS NULL OR @PersistedFileTargetPath LIKE 'system_health%')
		BEGIN
			RAISERROR(N'ATTENTION: You asked to query from system_health. But @PersistAllData was set to 1, so previously loaded data will be reused!',0,1);
			RAISERROR(N'If you want to read newly generated data from system_health, then please set @PersistAllData to 0.', 0, 1) WITH NOWAIT;
		END
		-- If it's the same file target path
		ELSE IF @PersistedFileTargetPath = @FileTargetPath
		BEGIN
			IF @Verbose = 1 RAISERROR(N'Reusing persisted data from the previously loaded @FileTargetPath: %s', 0, 1, @FileTargetPath) WITH NOWAIT;
		END
		ELSE
		BEGIN
			SET @PersistAllData = 0;
			EXEC (N'DROP TABLE #fileTargetPersisted');
			RAISERROR(N'ATTENTION: Recreating #alldata because current @FileTargetPath is different from the one previously used (%s)', 0, 1, @PersistedFileTargetPath) WITH NOWAIT;
		END
	END

	-- Persist the current @FileTargetPath
	IF OBJECT_ID('tempdb..#fileTargetPersisted') IS NULL
	BEGIN
		CREATE TABLE #fileTargetPersisted
		( FileTargetPath NVARCHAR(256) NULL );
		INSERT INTO #fileTargetPersisted VALUES(@FileTargetPath);
	END
END
ELSE
BEGIN
	IF OBJECT_ID('tempdb..#fileTargetPersisted') IS NOT NULL DROP TABLE #fileTargetPersisted;
END

-- If no @FileTargetPath was provided, try the system_health file target instead
IF @FileTargetPath IS NULL AND @ForceRingBuffer = 0
BEGIN
	SELECT @FileTargetPath = REPLACE(c.column_value, '.xel', '*.xel')
	FROM sys.dm_xe_sessions s
	JOIN sys.dm_xe_session_object_columns c
	ON s.address = c.event_session_address
	WHERE column_name = 'filename'
	AND s.name = 'system_health'
	OPTION (RECOMPILE);

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
IF @sqlmajorver >= 13
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

-- If @FileTargetPath still not found at this point, then query from the system_health ring buffer
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
ELSE IF @sqlmajorver >= 14
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
	EXEC sp_executesql @CMD, N'@FileTargetPath NVARCHAR(256) OUTPUT', @FileTargetPath OUTPUT;
	RAISERROR(N'Total diagnostic events loaded into #alldata: %d',0,1,@@ROWCOUNT) WITH NOWAIT;
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
			event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)'), -- SQLDIAG version
			event_data_xml.value('(event/data[@name="component"]/text)[1]', 'varchar(256)') -- system_health/sp_server_diagnostics_component_result version
			)
		)
	)
AND (ISNULL(@ExcludeClean,0) = 0 OR
		(
		[object_name] IN ('component_health_result','sp_server_diagnostics_component_result')
		AND
		'clean' <> COALESCE(
				  event_data_xml.value('(event/data[@name="state"]/text)[1]', 'varchar(50)') -- sp_server_diagnostics_component_result
				, event_data_xml.value('(event/data[@name="state_desc"])[1]', 'varchar(50)') -- query_processing in SQLDIAG
				)
		) 
	)
AND (ISNULL(@MinPendingTasks,0) <= 0 OR
	event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@pendingTasks)[1]', 'int') >= @MinPendingTasks
	)
ORDER BY timestamp_utc DESC
OPTION (RECOMPILE);

RAISERROR(N'Filtered events found: %d',0,1,@@ROWCOUNT) WITH NOWAIT;

SELECT
  timestamp_utc
, timestamp_local
, [object_name]
, componentData		= COALESCE(
				  event_data_xml.value('(event/data[@name="component"]/text)[1]', 'varchar(256)') -- sp_server_diagnostics_component_result
				, event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)') -- query_processing in SQLDIAG
				, event_data_xml.value('(event/data[@name="data"])[1]', 'varchar(256)') -- info_message in SQLDIAG
				)
, componentState	= COALESCE(
				  event_data_xml.value('(event/data[@name="state"]/text)[1]', 'varchar(50)') -- sp_server_diagnostics_component_result
				, event_data_xml.value('(event/data[@name="state_desc"])[1]', 'varchar(50)') -- query_processing in SQLDIAG
				)
, maxWorkers		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@maxWorkers)[1]', 'int')
, workersCreated	= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@workersCreated)[1]', 'int')
, workersIdle		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@workersIdle)[1]', 'int')
, pendingTasks		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@pendingTasks)[1]', 'int')
, hasUnresolvableDeadlockOccurred = event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@hasUnresolvableDeadlockOccurred)[1]', 'int')
, hasDeadlockedSchedulersOccurred = event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@hasDeadlockedSchedulersOccurred)[1]', 'int')

, blockedProcesses = CASE WHEN [object_name] <> 'info_message' 
			AND event_data_xml.exist('*//blocked-process-report/blocked-process/process/inputbuf') = 1 THEN
			event_data_xml.query('
let $items := *//blocked-process-report/blocked-process/process/inputbuf
let $unique-items := distinct-values($items)
return
   <blocked totalCount="{count($items)}">
      <summary>
      {
         for $item in $unique-items
  		 let $count := count($items[. eq $item])
  		 let $maxWait := max($items[. eq $item]/../@waittime)
  		 let $minWait := min($items[. eq $item]/../@waittime)
  		 let $avgWait := avg($items[. eq $item]/../@waittime)
         return <uniqueCommand count="{$count}" max-waittime="{$maxWait}" min-waittime="{$minWait}" avg-waittime="{$avgWait}">{$item}</uniqueCommand>
      }
      </summary>
      <details>
      {*//blocked-process-report}
      </details>
   </blocked>
') 
		END

, blockingProcesses = CASE WHEN [object_name] <> 'info_message' 
			AND event_data_xml.exist('*//blocked-process-report/blocking-process/process/inputbuf') = 1 THEN
			event_data_xml.query('
let $items := *//blocked-process-report/blocking-process/process/inputbuf
let $unique-items := distinct-values($items)
return
   <blocking totalCount="{count($items)}">   
      <summary>
      {
         for $item in $unique-items
		 let $count := count($items[. eq $item])
         return <uniqueCommand count="{$count}">{$item}</uniqueCommand>
      }
      </summary>
      <details>
      {*//blocked-process-report/blocking-process/process[not(empty(inputbuf/text()))]}
      </details>
   </blocking>
')
		END

, blockedByNonSession = CASE WHEN [object_name] <> 'info_message' 
			AND event_data_xml.exist('*//blocked-process-report/blocking-process/process/inputbuf') = 1 THEN
			event_data_xml.query('let $items := *//blocked-process-report/blocked-process/process[empty(../../blocking-process/process/inputbuf/text())]/../..
return
	<blocked-by-non-session totalCount="{count($items)}">
	{$items}
	</blocked-by-non-session>')
	END

, possibleHeadBlockers = CASE WHEN [object_name] <> 'info_message' 
			AND event_data_xml.exist('*//blocked-process-report/blocking-process/process/inputbuf') = 1 THEN
			event_data_xml.query('
let $items := distinct-values(*//blocked-process-report/blocking-process/process/@spid)
return
    <blockers totalCount="{count($items)}">
    {
       for $spid in $items
           let $blockedByResource := *//blocked-process-report/blocked-process/process[@spid = $spid and empty(../../blocking-process/process/inputbuf/text())]/../..
	   let $isBlockedByResource := not(empty(*//blocked-process-report/blocked-process/process[@spid = $spid]))
           return
	       <blocker spid="{$spid}" is-blocked-by-non-session="{$isBlockedByResource}">
	       {$blockedByResource}
	       </blocker>
    }
    </blockers>').query('let $items := *//blocker[not(@is-blocked-by-non-session) or not(empty(*//process/inputbuf/text()))]
return
	<head-blockers totalCount="{count($items)}">{$items}</head-blockers>')
    END

, [file_name]
, file_offset
, event_data_xml
FROM #data
ORDER BY timestamp_utc DESC

SET NOEXEC OFF;
