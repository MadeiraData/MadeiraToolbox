/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Description: Use this script to retrieve all unused ix across all of your databases.
The data returned includes various index usage statistics and a corresponding drop command.
Supports both on-premise instances, as well as Azure SQL Databases.
*/
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @CMD NVARCHAR(MAX);
SET @CMD = N'
 PRINT DB_NAME();
 SELECT
 db_name() AS DBNAme,
 OBJECT_SCHEMA_NAME(ix.object_id) as SchemaName,
 OBJECT_NAME(ix.object_id) AS Table_name,
 ix.name AS Index_name,
 SUM(p.rows),
 SUM(ps.reserved_page_count) * 8,
 ''USE '' + QUOTENAME(DB_NAME()) + N''; ALTER INDEX ''+QUOTENAME(ix.name)+'' ON ''+QUOTENAME(db_name())+''.''+ QUOTENAME(OBJECT_SCHEMA_NAME(ix.object_id))+''.''+QUOTENAME(OBJECT_NAME(ix.object_id)) + '' DISABLE'' as DisableCmd ,
 ''USE '' + QUOTENAME(DB_NAME()) + N''; DROP INDEX ''+QUOTENAME(ix.name)+'' ON ''+QUOTENAME(db_name())+''.''+ QUOTENAME(OBJECT_SCHEMA_NAME(ix.object_id))+''.''+QUOTENAME(OBJECT_NAME(ix.object_id)) as dropcmd ,
 STATS_DATE(ix.object_id, ix.index_id) StatsDate,
 t.create_date,
 ISNULL(us.user_updates, 0) + ISNULL(us.system_updates, 0)
 FROM
 sys.indexes ix
 INNER HASH JOIN sys.tables t
 ON ix.object_id = t.object_id 
 AND t.create_date < DATEADD(dd, -30, GETDATE())
 AND t.is_ms_shipped = 0
 AND ix.index_id > 1
 AND ix.is_primary_key = 0
 AND ix.is_unique = 0
 AND ix.is_disabled = 0
 AND ix.is_hypothetical = 0
 INNER JOIN sys.partitions p
 ON ix.object_id = p.object_id
 AND ix.index_id = p.index_id
 LEFT JOIN sys.dm_db_index_usage_stats AS us
 ON ix.index_id = us.index_id AND us.OBJECT_ID = ix.OBJECT_ID
 LEFT JOIN sys.dm_db_partition_stats AS ps
 ON ix.index_id = ps.index_id AND ps.OBJECT_ID = ix.OBJECT_ID
 WHERE
 us.user_updates > 100
 AND ISNULL(us.system_seeks,0) = 0
 AND ISNULL(us.user_seeks,0) = 0
 AND ISNULL(us.user_scans,0) = 0
 GROUP BY
 ix.object_id,
 t.create_date,
 ix.index_id,
 ix.name,
 us.user_seeks,
 us.user_scans,
 us.user_updates,
 us.system_updates
 HAVING
 SUM(p.rows) > 200000'

IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (DBName SYSNAME, SchemaName SYSNAME, TableName SYSNAME, IndexName SYSNAME NULL, RowsCount BIGINT, IndexSizeKB INT, UpdatesCount BIGINT NULL, DisableCMD NVARCHAR(MAX), DropCMD NVARCHAR(MAX), TableCreatedDate DATETIME NULL, LastStatsDate DATETIME);

IF CONVERT(int, SERVERPROPERTY('EngineEdition')) <> 5
BEGIN
	SET @CMD = N'
IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND name = ''?'' AND state_desc = ''ONLINE'' AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
BEGIN
 USE [?];' + @CMD + N'
END'
	EXEC sp_executesql N'IF (SELECT sqlserver_start_time FROM sys.dm_os_sys_info) < DATEADD(dd,-14,GETDATE())
BEGIN
 INSERT INTO #tmp(DBName, SchemaName, TableName, IndexName, RowsCount, IndexSizeKB, DisableCMD, DropCMD, LastStatsDate, TableCreatedDate, UpdatesCount)
 EXEC sp_MSforeachdb @CMD 
END', N'@CMD nvarchar(max)', @CMD;
END
ELSE
BEGIN

	EXEC sp_executesql @CMD;
END

SELECT *, '(''' + IndexName + ''')'
FROM #tmp
ORDER BY DBName ASC, IndexSizeKB DESC, RowsCount DESC
