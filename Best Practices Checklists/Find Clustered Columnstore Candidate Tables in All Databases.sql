/*
Find Clustered Columnstore Candidate Tables in All Databases
============================================================
Author: Eitan Blumin | https://www.madeiradata.com
Date: 2022-05-04
Description:
	Based on Azure SQL Tip #1290:
	https://github.com/microsoft/azure-sql-tips/wiki/Azure-SQL-Database-tips#tip_id-1290
Supported versions:
	SQL Server 2016 (16.x) SP1 and newer | Azure SQL Database | Azure SQL Managed Instance
*/
DECLARE
	  @CCICandidateMinSizeGB int = 10	-- minimum table size to check
	, @DaysBack		 int = 7	-- minimum number of days for the server uptime
	, @StrictMode		 bit = 1	-- optionally set to 0 to also allow tables with lookups, seeks, updates, and deletes


SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @results table ([database_name] sysname NULL, [schema_name] sysname NULL, [table_name] sysname NULL
, [columns_count] int NULL, [table_size_mb] float NULL, [partition_count] int NULL
, [insert_count] int NULL, [update_count] int NULL, [delete_count] int NULL, [singleton_lookup_count] int NULL
, [range_scan_count] int NULL, [seek_count] int NULL, [full_scan_count] int NULL, [lookup_count] int NULL
);
DECLARE @SQLVersion int = CONVERT(int, SERVERPROPERTY('ProductMajorVersion'))
DECLARE @SQLBuild int = CONVERT(int, SERVERPROPERTY('ProductBuild'))
DECLARE @IsVersionSupported bit = 1, @OutputMessage nvarchar(200)
DECLARE @CMD nvarchar(MAX);

-- Check for too recent server uptime
IF EXISTS (
	SELECT NULL
	FROM sys.dm_os_sys_info
	WHERE sqlserver_start_time > DATEADD(DAY, -@DaysBack, GETDATE())
	)
BEGIN
	SET @IsVersionSupported = 0;
	SELECT @OutputMessage = N'Server ' + @@SERVERNAME + N' uptime is since ' + CONVERT(nvarchar(MAX), sqlserver_start_time, 121)
	+ N' (time now: ' + CONVERT(nvarchar(MAX), GETDATE(), 121) + N')'
	FROM sys.dm_os_sys_info
	WHERE sqlserver_start_time > DATEADD(DAY, -@DaysBack, GETDATE())
	OPTION (RECOMPILE);
END
/* Check for unsupported versions */
ELSE IF (
	CONVERT(varchar(256), SERVERPROPERTY('Edition')) <> 'SQL Azure' /* on-prem */
	AND (
		@SQLVersion < 13 /* SQL 2014 and older */
		OR (@SQLVersion = 13 AND @SQLBuild < 4000) /* SQL 2016 before SP1 */
	    )
   )
BEGIN
	SET @IsVersionSupported = 0;
	SET @OutputMessage = N'Server ' + @@SERVERNAME + N' SQL major version is ' + CONVERT(nvarchar(MAX), CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff))
	+ N' ' + CONVERT(nvarchar(MAX), SERVERPROPERTY('ProductLevel')) + N', ' + CONVERT(nvarchar(max), SERVERPROPERTY('Edition'));
END
ELSE IF (
	CONVERT(varchar(256), SERVERPROPERTY('Edition')) = 'SQL Azure' /* Azure SQL */
	AND CONVERT(varchar(128), SERVERPROPERTY('EngineEdition')) <> 8 /* Not Azure SQL Managed Instance */
	AND OBJECT_ID('sys.database_service_objectives') IS NOT NULL /* we can check Azure SQL DB SLO */
   )
BEGIN
	-- columnstore indexes are available in Azure SQL Database Premium tiers, Standard tiers - S3 and above, and all vCore tiers
	EXEC sp_executesql N'
	SELECT @IsVersionSupported = 0
	, @OutputMessage = N''Database '' + QUOTENAME(DB_NAME()) + N'' is at Service Tier '' + service_objective COLLATE database_default
	FROM sys.database_service_objectives
	WHERE database_id = DB_ID() 
	AND (
		edition = ''Basic''
		OR (edition = ''Standard'' AND service_objective IN (''S0'', ''S1'', ''S2''))
	    )'
	, N'@IsVersionSupported bit OUTPUT, @OutputMessage nvarchar(200) OUTPUT'
	, @IsVersionSupported OUTPUT, @OutputMessage OUTPUT WITH RECOMPILE
END

IF @IsVersionSupported = 1
BEGIN
SET @CMD = N'WITH any_partition AS
(
SELECT p.object_id, p.index_id, p.partition_number, p.rows, p.data_compression_desc, ps.used_page_count * 8 / 1024. AS partition_size_mb,
MAX(IIF(p.data_compression_desc IN (''COLUMNSTORE'',''COLUMNSTORE_ARCHIVE''), 1, 0)) OVER (PARTITION BY p.object_id) AS object_has_columnstore_indexes,
MAX(IIF(p.rows >= 102400, 1, 0)) OVER (PARTITION BY p.object_id) AS object_has_columnstore_compressible_partitions
FROM sys.partitions AS p
INNER JOIN sys.dm_db_partition_stats AS ps
ON p.partition_id = ps.partition_id
AND p.object_id = ps.object_id
AND p.index_id = ps.index_id
WHERE -- restrict to objects that do not have column data types not supported for CCI
NOT EXISTS (
        SELECT 1
        FROM sys.columns AS c
        INNER JOIN sys.types AS t
        ON c.system_type_id = t.system_type_id
        WHERE c.object_id = p.object_id
        AND t.name IN (''text'',''ntext'',''image'',''timestamp'',''sql_variant'',''hierarchyid'',''geometry'',''geography'',''xml'')
        )
),
candidate_partition AS
(
SELECT object_id, index_id, partition_number, rows, partition_size_mb
FROM any_partition
WHERE data_compression_desc IN (''NONE'',''ROW'',''PAGE'')
AND object_has_columnstore_indexes = 0 -- an object with any kind of columnstore is not a candidate
AND object_has_columnstore_compressible_partitions = 1
),
table_operational_stats AS -- summarize operational stats for heap, CI, and NCI
(
SELECT cp.object_id,
       SUM(IIF(cp.index_id IN (0,1), partition_size_mb, 0)) AS table_size_mb, -- exclude NCI size
       SUM(IIF(cp.index_id IN (0,1), 1, 0)) AS partition_count,
       SUM(ios.leaf_insert_count) AS lead_insert_count,
       SUM(ios.leaf_update_count) AS leaf_update_count,
       SUM(ios.leaf_delete_count + ios.leaf_ghost_count) AS leaf_delete_count,
       SUM(ios.range_scan_count) AS range_scan_count,
       SUM(ios.singleton_lookup_count) AS singleton_lookup_count
FROM candidate_partition AS cp
CROSS APPLY sys.dm_db_index_operational_stats(DB_ID(), cp.object_id, cp.index_id, cp.partition_number) AS ios -- assumption: a representative workload has populated index operational stats for relevant tables
GROUP BY cp.object_id
)
SELECT [database_name]		= DB_NAME(),
       schema_name		= OBJECT_SCHEMA_NAME(t.object_id) COLLATE DATABASE_DEFAULT,
       table_name		= t.name COLLATE DATABASE_DEFAULT,
       columns_count		= (SELECT COUNT(*) FROM sys.columns AS c WHERE c.object_id = t.object_id AND c.is_computed = 0),
       table_size_mb		= tos.table_size_mb,
       partition_count		= tos.partition_count,
       insert_count		= tos.lead_insert_count,
       update_count		= tos.leaf_update_count,
       delete_count		= tos.leaf_delete_count,
       singleton_lookup_count	= tos.singleton_lookup_count,
       range_scan_count		= tos.range_scan_count,
       seek_count		= ISNULL(ius.user_seeks, 0),
       full_scan_count		= ISNULL(ius.user_scans, 0),
       lookup_count		= ISNULL(ius.user_lookups, 0)
FROM sys.tables AS t
INNER JOIN sys.indexes AS i ON t.object_id = i.object_id AND i.index_id <= 1 -- clustered index or heap
INNER JOIN table_operational_stats AS tos ON t.object_id = tos.object_id
LEFT JOIN sys.dm_db_index_usage_stats AS ius ON ius.database_id = DB_ID() AND t.object_id = ius.object_id AND i.index_id = ius.index_id
WHERE t.is_ms_shipped = 0
AND tos.table_size_mb > @CCICandidateMinSizeGB * 1024. -- consider sufficiently large tables only
AND (
    i.index_id = 0 OR 
	(
	    ISNULL(ius.user_scans, 0) > 0 -- require a CCI candidate to have some full scans'
	    + CASE WHEN @StrictMode = 1 THEN N'
-- conservatively require a CCI candidate to have no updates, seeks, or lookups
	AND ISNULL(ius.user_lookups, 0) = 0
	AND tos.leaf_update_count = 0
	AND tos.singleton_lookup_count = 0
	AND ISNULL(ius.user_seeks, 0) = 0'
		ELSE N'' END
	+ N'
	) 
    )'

DECLARE @CurrDB sysname, @spExecuteSql nvarchar(1000);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT name
FROM sys.databases
WHERE state = 0
AND HAS_DBACCESS(name) = 1
AND DATABASEPROPERTYEX(name, 'Updateability') = 'READ_WRITE'

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @results
	EXEC @spExecuteSql @CMD, N'@CCICandidateMinSizeGB int', @CCICandidateMinSizeGB WITH RECOMPILE;

	RAISERROR(N'Database "%s": %d finding(s)',0,1,@CurrDB,@@ROWCOUNT) WITH NOWAIT;
END

CLOSE DBs;
DEALLOCATE DBs;

END


-- Summary:

SELECT
	  Msg = CONCAT(N'Database ', QUOTENAME([database_name]) + N' has ', COUNT(*), N' potential clustered columnstore candidate(s)')
	, cci_candidate_count = COUNT(*)
FROM @results AS r
WHERE @IsVersionSupported = 1
GROUP BY [database_name]

UNION ALL

SELECT CONCAT(N'UNSUPPORTED: ', @OutputMessage), 0
WHERE @IsVersionSupported = 0

OPTION (RECOMPILE);


-- Details:
IF @IsVersionSupported = 1
BEGIN
	SELECT *
	FROM @results AS r
	WHERE @IsVersionSupported = 1
	OPTION (RECOMPILE);
END