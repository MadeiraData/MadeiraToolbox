/*
Troubleshoot CDC Cleanup Latency
================================
Author: Eitan Blumin
Date: 2023-02-13
*/
DECLARE
	 @FilterByDBName			sysname		= NULL		-- Optionally filter by a specific database name. Leave NULL to check all accessible CDC-enabled databases.
	,@LatencyMinutesThreshold	bigint		= NULL --60*25	-- Optionally filter only for instances where the MinLSN is this many minutes behind the configured cleanup retention.


/***** NO NEED TO CHANGE ANYTHING BELOW THIS LINE *****/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

IF OBJECT_ID('msdb.dbo.cdc_jobs') IS NULL
BEGIN
	RAISERROR(N'%s: CDC is not enabled on this instance.',0,1,@@SERVERNAME);
	SET NOEXEC ON;
END

DECLARE @CurrDB sysname, @spExecuteSQL nvarchar(500);
DECLARE @Results AS TABLE ([database_id] int, capture_instance sysname, ct_table_object_id int, src_table_object_id int
, src_table_rows bigint, ct_table_rows bigint, ct_used_page_count bigint, src_used_page_count bigint
, start_lsn_time datetime, max_lsn_time datetime, min_lsn_time datetime)

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE is_cdc_enabled = 1
AND HAS_DBACCESS([name]) = 1
AND (@FilterByDBName IS NULL OR @FilterByDBName = [name])

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @Results
	EXEC @spExecuteSQL N'
SELECT DB_ID(), ct.capture_instance, object_id, source_object_id
, rcounts.source_table_rows
, rcounts.ct_table_rows
, ct_pstats.used_page_count
, source_pstats.used_page_count
, sys.fn_cdc_map_lsn_to_time(ct.start_lsn)
, sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn())
, sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_min_lsn(ct.capture_instance))
from cdc.change_tables AS ct
outer apply
(
select source_table_rows = (SELECT SUM(rows) FROM sys.partitions AS p WHERE p.object_id = ct.source_object_id AND p.index_id <= 1)
, ct_table_rows = (SELECT SUM(rows) FROM sys.partitions AS p WHERE p.object_id = ct.object_id AND p.index_id <= 1)
) as rcounts
outer apply
(
select used_page_count = SUM(used_page_count)
from sys.dm_db_partition_stats AS ps
where ps.object_id = ct.object_id
) AS ct_pstats
outer apply
(
select used_page_count = SUM(used_page_count)
from sys.dm_db_partition_stats AS ps
where ps.object_id = ct.source_object_id
) AS source_pstats'

	RAISERROR(N'%s: %d capture instance(s)',0,1,@CurrDB,@@ROWCOUNT) WITH NOWAIT;

END

CLOSE DBs;
DEALLOCATE DBs;


select ct.capture_instance
, ct_table = QUOTENAME(OBJECT_SCHEMA_NAME(ct_table_object_id, ct.[database_id])) + N'.' + QUOTENAME(OBJECT_NAME(ct_table_object_id, ct.[database_id]))
, source_table = QUOTENAME(OBJECT_SCHEMA_NAME(src_table_object_id, ct.[database_id])) + N'.' + QUOTENAME(OBJECT_NAME(src_table_object_id, ct.[database_id]))
, src_table_rows, ct_table_rows
, ct_rows_percent = ct_table_rows * 1.0 / src_table_rows * 100
, ct_used_mb = ct_used_page_count / 128
, source_used_mb = src_used_page_count / 128
, ct_used_percent = ct_used_page_count * 1.0 / src_used_page_count * 100
, start_lsn_time, max_lsn_time, min_lsn_time
, cleanup_retention_threshold = DATEADD(minute, -cleanup.retention, GETDATE())
, cleanup_latency_minutes = DATEDIFF(minute, min_lsn_time, DATEADD(minute, -cleanup.retention, GETDATE()))
, cleanup_job_startcommand = N'EXEC ' + QUOTENAME(DB_NAME()) + N'.sys.sp_cdc_start_job ''cleanup'''
, cleanup_manualcommand = N'DECLARE @result bit, @instance sysname = ''' + ct.capture_instance + N''';
EXEC ' + QUOTENAME(DB_NAME()) + N'.sys.sp_cdc_cleanup_change_table @capture_instance = @instance, @low_water_mark = NULL, @threshold = ' + CONVERT(nvarchar(max), cleanup.threshold) + N', @fCleanupFailed = @result OUTPUT;
SELECT @instance AS captureInstance, @result AS is_failure;'
from @Results AS ct
outer apply
(
select *
from msdb.dbo.cdc_jobs
where job_type = 'cleanup'
and database_id = ct.[database_id]
) as cleanup
WHERE @LatencyMinutesThreshold IS NULL OR DATEADD(minute, @LatencyMinutesThreshold, min_lsn_time) < DATEADD(minute, -cleanup.retention, GETDATE())
OPTION (RECOMPILE)

SET NOEXEC OFF;