DECLARE
  @MinAverageTotalUserCost int = 5
, @MinAverageUserImpact int = 65
, @MinSeeksOrScansPerDay int = 100
, @MinUniqueCompiles int = 20

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @daysUptime int;
SELECT @daysUptime = DATEDIFF(day,sqlserver_start_time,GETDATE()) FROM sys.dm_os_sys_info;

IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
CREATE TABLE #Results
(
index_advantage	float NULL,
last_user_seek	datetime NULL,
DatabaseName	sysname	 NULL,
[Database.Schema.Table]	nvarchar(1000)	 NULL,
SchemaName	sysname	 NULL,
TableName	sysname	 NULL,
index_handle int NULL,
missing_indexes_for_table	int NULL,
similar_missing_indexes_for_table	int NULL,
equality_columns	nvarchar(max) NULL,
inequality_columns	nvarchar(max) NULL,
included_columns	nvarchar(max) NULL,
unique_compiles	bigint NULL,
user_seeks	bigint NULL,
avg_total_user_cost	float NULL,
avg_user_impact	float NULL,
NonclusteredIndexesOnTable	int NULL,
UserUpdates	bigint NULL,
UserScans	bigint NULL,
LastUpdate	datetime     NULL,
LastScan	datetime     NULL,
TotalRows	bigint NULL
);

DECLARE @CMD NVARCHAR(MAX);
SET @CMD = N'SELECT 
CONVERT(decimal(18,2), migs.user_seeks * migs.avg_total_user_cost * (migs.avg_user_impact * 0.01)) AS [index_advantage],
FORMAT(migs.last_user_seek, ''yyyy-MM-dd HH:mm:ss'') AS [last_user_seek],
DB_NAME(mid.database_id) AS [Database], 
mid.[statement] AS [Database.Schema.Table],
OBJECT_SCHEMA_NAME(mid.object_id, mid.database_id) AS SchemaName,
OBJECT_NAME(mid.object_id, mid.database_id) AS TableName,
mig.index_handle,
COUNT(1) OVER(PARTITION BY mid.[statement]) AS [missing_indexes_for_table],
COUNT(1) OVER(PARTITION BY mid.[statement], mid.equality_columns) AS [similar_missing_indexes_for_table],
mid.equality_columns, mid.inequality_columns, mid.included_columns,
migs.unique_compiles, migs.user_seeks, 
CONVERT(decimal(18,2), migs.avg_total_user_cost) AS [avg_total_user_cost], migs.avg_user_impact 
, NonclusteredIndexesOnTable		= ISNULL(OtherIndexes.NonclusteredIndexes, 0) 
, UserUpdates		= ISNULL(UsageStats.user_updates, 0) 
, UserScans		= ISNULL(UsageStats.user_scans, 0) 
, LastUpdate		= UsageStats.last_user_update 
, LastScan		= UsageStats.last_user_scan 
, TotalRows		= ISNULL(PartitionStats.TotalRows, 0)
FROM sys.dm_db_missing_index_group_stats AS migs WITH (NOLOCK)
INNER JOIN sys.dm_db_missing_index_groups AS mig WITH (NOLOCK)
ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid WITH (NOLOCK)
ON mig.index_handle = mid.index_handle
OUTER APPLY
(
	SELECT
		us.user_updates ,
		us.user_scans ,
		us.last_user_update ,
		us.last_user_scan
	FROM sys.dm_db_index_usage_stats AS us
	WHERE
		us.database_id = DB_ID()
	AND	us.object_id = mid.object_id
	AND	us.index_id <= 1
) AS UsageStats
OUTER APPLY
(
	SELECT TotalRows = SUM(p.rows)
	FROM sys.partitions AS p
	WHERE p.object_id = mid.object_id AND p.index_id <= 1
) AS PartitionStats
OUTER APPLY
(
	SELECT NonclusteredIndexes = COUNT(*)
	FROM sys.indexes AS p
	WHERE p.object_id = mid.object_id AND p.index_id > 1
) AS OtherIndexes
WHERE migs.avg_total_user_cost >= @MinAverageTotalUserCost
AND migs.avg_user_impact >= @MinAverageUserImpact
AND (migs.user_seeks+migs.user_scans) / @daysUptime >= @MinSeeksOrScansPerDay
AND migs.unique_compiles >= @MinUniqueCompiles
AND database_id = DB_ID()
ORDER BY index_advantage DESC, migs.avg_user_impact DESC'


DECLARE @dbname sysname, @spExecuteSql NVARCHAR(1000);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR 
SELECT [name]
FROM sys.databases
where database_id > 4
AND HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE';

OPEN DBs;
WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @dbname;
	IF @@FETCH_STATUS <> 0 BREAK;
	SET @spExecuteSql = QUOTENAME(@dbname) + N'..sp_executesql'

	INSERT INTO #Results
	EXEC @spExecuteSql @CMD
		, N'@daysUptime int, @MinAverageTotalUserCost int, @MinAverageUserImpact int, @MinSeeksOrScansPerDay int, @MinUniqueCompiles int'
		, @daysUptime, @MinAverageTotalUserCost, @MinAverageUserImpact, @MinSeeksOrScansPerDay, @MinUniqueCompiles
		WITH RECOMPILE;
END
CLOSE DBs;
DEALLOCATE DBs;

SELECT *
, RunThisForMoreDetails = N'USE ' + QUOTENAME([DatabaseName])
+ N'; EXEC sp_help ' + QUOTENAME(QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName), '''')
+ N'; EXEC sp_spaceused ' + QUOTENAME(QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName), '''')
+ N'; EXEC sp_indexes_rowset @table_schema = ' + QUOTENAME(SchemaName, '''') + N', @table_name = ' + QUOTENAME(TableName, '''')
+ N';'
, RemediationScript = CONCAT(
 N'CREATE NONCLUSTERED INDEX [IX_rename_me_'
 , index_handle
 ,'] ON '
 ,[Database.Schema.Table]
 ,' ( '
 ,ISNULL(equality_columns, inequality_columns)
 ,' )'
 , CASE WHEN (equality_columns IS NOT NULL AND inequality_columns IS NOT NULL)
 THEN N' INCLUDE ( ' + inequality_columns
 ELSE N''
 END
 , CASE WHEN included_columns IS NOT NULL AND (equality_columns IS NOT NULL AND inequality_columns IS NOT NULL)
 THEN N', ' + included_columns
 WHEN included_columns IS NOT NULL
 THEN N' INCLUDE ( ' + included_columns
 ELSE N''
 END
 , CASE WHEN included_columns IS NOT NULL OR (equality_columns IS NOT NULL AND inequality_columns IS NOT NULL)
 THEN N' )'
 ELSE N''
 END
 )
FROM #Results
ORDER BY index_advantage DESC
OPTION(RECOMPILE);

--DROP TABLE #Results;