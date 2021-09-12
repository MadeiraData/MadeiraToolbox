/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Description: Use this script to retrieve all unused indexes across all of your databases.
The data returned includes various index usage statistics and a corresponding drop command.
Supports both on-premise instances, as well as Azure SQL Databases.
*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @MinimumRowsInTable INT = 200000;
DECLARE @CMD NVARCHAR(MAX);
SET @CMD = N'PRINT DB_NAME();
SELECT
 db_name() AS DBNAme,
 OBJECT_SCHEMA_NAME(indexes.object_id) as SchemaName,
 OBJECT_NAME(indexes.object_id) AS Table_name,
 indexes.name AS Index_name,
 SUM(partitions.rows),
 SUM(partition_stats.reserved_page_count) * 8,
 STATS_DATE(indexes.object_id, indexes.index_id) StatsDate,
 tables.create_date,
 ISNULL(usage_stats.user_updates, 0) + ISNULL(usage_stats.system_updates, 0)
FROM
 sys.indexes
INNER JOIN sys.tables
 ON indexes.object_id = tables.object_id 
 AND tables.create_date < DATEADD(dd, -30, GETDATE())
 AND tables.is_ms_shipped = 0
 AND indexes.index_id > 1
 AND indexes.is_primary_key = 0
 AND indexes.is_unique = 0
 AND indexes.is_disabled = 0
 AND indexes.is_hypothetical = 0
INNER JOIN sys.partitions
 ON indexes.object_id = partitions.object_id
 AND indexes.index_id = partitions.index_id
LEFT JOIN sys.dm_db_index_usage_stats AS usage_stats
 ON indexes.index_id = usage_stats.index_id AND usage_stats.OBJECT_ID = indexes.OBJECT_ID
LEFT JOIN sys.dm_db_partition_stats AS partition_stats
 ON indexes.index_id = partition_stats.index_id AND partition_stats.OBJECT_ID = indexes.OBJECT_ID
WHERE
 usage_stats.user_updates > 100
 AND ISNULL(usage_stats.system_seeks,0) = 0
 AND ISNULL(usage_stats.user_seeks,0) = 0
 AND ISNULL(usage_stats.user_scans,0) = 0
GROUP BY
 indexes.object_id,
 tables.create_date,
 indexes.index_id,
 indexes.name,
 usage_stats.user_seeks,
 usage_stats.user_scans,
 usage_stats.user_updates,
 usage_stats.system_updates
HAVING
 SUM(partitions.rows) > ' + CAST(@MinimumRowsInTable AS NVARCHAR(MAX))

IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (DBName SYSNAME, SchemaName SYSNAME, TableName SYSNAME, IndexName SYSNAME NULL, RowsCount INT, IndexSizeKB INT, UpdatesCount INT NULL, TableCreatedDate DATETIME NULL, LastStatsDate DATETIME);

IF CONVERT(varchar(300),SERVERPROPERTY('Edition')) <> 'SQL Azure'
BEGIN
	SET @CMD = N'
IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND HAS_DBACCESS([name]) = 1 AND state_desc = ''ONLINE'' AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
BEGIN
 USE [?];' + @CMD + N'
END'
	EXEC sp_executesql N'IF (SELECT sqlserver_start_time FROM sys.dm_os_sys_info) < DATEADD(dd,-14,GETDATE())
BEGIN
 INSERT INTO #tmp(DBName, SchemaName, TableName, IndexName, RowsCount, IndexSizeKB, LastStatsDate, TableCreatedDate, UpdatesCount)
 EXEC sp_MSforeachdb @CMD 
END', N'@CMD nvarchar(max)', @CMD;
END
ELSE
BEGIN
	EXEC sp_executesql @CMD;
END

SELECT *,
DisableCmd = N'USE ' + QUOTENAME(DBName) + N'; ALTER INDEX ' + QUOTENAME(IndexName) + N' ON ' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N' DISABLE;',
DisableIfActiveCmd = N'USE ' + QUOTENAME(DBName) + N'; IF INDEXPROPERTY(OBJECT_ID(''' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N'''), ''' + IndexName + N''', ''IsDisabled'') = 0 ALTER INDEX ' + QUOTENAME(IndexName) + N' ON ' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N' DISABLE;',
DropCmd = N'USE ' + QUOTENAME(DBName) + N'; DROP INDEX ' + QUOTENAME(IndexName) + N' ON ' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName) + N';'
FROM #tmp
ORDER BY DBName ASC, IndexSizeKB DESC, RowsCount DESC