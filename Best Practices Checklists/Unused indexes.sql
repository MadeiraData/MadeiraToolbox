/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Description: Use this script to retrieve all unused indexes across all of your databases.
The data returned includes various index usage statistics and a corresponding drop command.
Supports both on-premise instances, as well as Azure SQL Databases.
*/
DECLARE
	 @MinimumRowsInTable INT = 200000
	,@MinimumUserUpdates INT = 100
	,@MinimumTableCreationDays INT = 30

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @CMD NVARCHAR(MAX);
SET @CMD = N'SELECT
 DB_NAME() AS DBName,
 OBJECT_SCHEMA_NAME(indexes.object_id) as SchemaName,
 OBJECT_NAME(indexes.object_id) AS Table_name,
 indexes.name AS Index_name,
 SUM(partitions.rows),
 SUM(partition_stats.reserved_page_count) * 8,
 SUM(ISNULL(usage_stats.user_updates, 0) + ISNULL(usage_stats.system_updates, 0)),
 tables.create_date,
 STATS_DATE(indexes.object_id, indexes.index_id) StatsDate,
 indexes.filter_definition,
 STUFF
((
SELECT '', '' + QUOTENAME(col.name) + CASE WHEN keyCol.is_descending_key = 1 THEN '' DESC'' ELSE '' ASC'' END
FROM sys.index_columns keyCol
inner join sys.columns col on keyCol.object_id = col.object_id AND keyCol.column_id = col.column_id
WHERE indexes.object_id = keyCol.object_id
AND indexes.index_id = keyCol.index_id
AND keyCol.is_included_column = 0
ORDER BY keyCol.key_ordinal
FOR XML PATH('''')), 1, 2, ''''),
	STUFF
((
SELECT '', '' + QUOTENAME(col.name)
FROM sys.index_columns keyCol
inner join sys.columns col on keyCol.object_id = col.object_id AND keyCol.column_id = col.column_id
WHERE indexes.object_id = keyCol.object_id
AND indexes.index_id = keyCol.index_id
AND keyCol.is_included_column = 1
ORDER BY keyCol.key_ordinal
FOR XML PATH('''')), 1, 2, '''')
FROM sys.indexes
INNER JOIN sys.tables ON indexes.object_id = tables.object_id 
INNER JOIN sys.partitions ON indexes.object_id = partitions.object_id AND indexes.index_id = partitions.index_id
LEFT JOIN sys.dm_db_index_usage_stats AS usage_stats ON indexes.index_id = usage_stats.index_id AND usage_stats.object_id = indexes.object_id AND usage_stats.database_id = DB_ID()
LEFT JOIN sys.dm_db_partition_stats AS partition_stats ON indexes.index_id = partition_stats.index_id AND partition_stats.object_id = indexes.object_id
WHERE
 ISNULL(usage_stats.user_updates, 0) + ISNULL(usage_stats.system_updates, 0) > ' + CAST(@MinimumUserUpdates AS NVARCHAR(MAX)) + N'
 AND ISNULL(usage_stats.user_lookups,0) = 0
 AND ISNULL(usage_stats.user_seeks,0) = 0
 AND ISNULL(usage_stats.user_scans,0) = 0
 AND tables.create_date < DATEADD(dd, -' + CAST(@MinimumTableCreationDays AS NVARCHAR(MAX)) + N', GETDATE())
 AND tables.is_ms_shipped = 0
 AND indexes.index_id > 1
 AND indexes.is_primary_key = 0
 AND indexes.is_unique = 0
 AND indexes.is_disabled = 0
 AND indexes.is_hypothetical = 0
GROUP BY
 indexes.object_id,
 tables.create_date,
 indexes.index_id,
 indexes.name,
 indexes.filter_definition
HAVING
 SUM(partitions.rows) > ' + CAST(@MinimumRowsInTable AS NVARCHAR(MAX))

IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (DBName SYSNAME, SchemaName SYSNAME, TableName SYSNAME, IndexName SYSNAME NULL, RowsCount BIGINT, IndexSizeKB BIGINT, UpdatesCount BIGINT NULL
, TableCreatedDate DATETIME NULL, LastStatsDate datetime
, IndexFilter nvarchar(MAX) NULL, KeyCols nvarchar(MAX) NULL, IncludeCols nvarchar(MAX) NULL);

DECLARE @CurrDB sysname, @spExecuteSql nvarchar(1000), @InstanceStartTime datetime;

IF OBJECT_ID('sys.dm_os_sys_info') IS NOT NULL
BEGIN
	SELECT @InstanceStartTime = sqlserver_start_time
	FROM sys.dm_os_sys_info
	OPTION (RECOMPILE);

	PRINT N'SQL Server instance "' + @@SERVERNAME + N'" is up since: ' + CONVERT(nvarchar(25), @InstanceStartTime, 121);
END

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND database_id > 4
AND state_desc = 'ONLINE'
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;
	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO #tmp
	EXEC @spExecuteSql @CMD;
	RAISERROR(N'Database "%s": %d unused indexes.',0,1, @CurrDB,@@ROWCOUNT) WITH NOWAIT;
END

CLOSE DBs;
DEALLOCATE DBs;

SELECT *,
DisableCmd = N'USE ' + QUOTENAME(DBName) + N'; IF INDEXPROPERTY(OBJECT_ID(''' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N'''), ''' + IndexName + N''', ''IsDisabled'') = 0 ALTER INDEX ' + QUOTENAME(IndexName) + N' ON ' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N' DISABLE;',
DropCmd = N'USE ' + QUOTENAME(DBName) + N'; IF INDEXPROPERTY(OBJECT_ID(''' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N'''), ''' + IndexName + N''', ''IndexID'') IS NOT NULL DROP INDEX ' + QUOTENAME(IndexName) + N' ON ' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N';'
FROM #tmp
ORDER BY
    DBName ASC, IndexSizeKB DESC, RowsCount DESC