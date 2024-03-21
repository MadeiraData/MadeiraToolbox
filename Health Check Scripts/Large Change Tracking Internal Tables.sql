/*
Check for large Change Tracking internal tables
Author: Eitan Blumin | Madeira Data Solutions | https://madeiradata.com
Date: 2024-03-21
Reference:
https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/cleanup-and-troubleshoot-change-tracking-sql-server
*/
DECLARE
	@TopPerDB			int		= 100,
	@MinimumRowCount	bigint	= 1,
	@MinimumSizeMB		bigint	= 1024,
	@FilterByDB			sysname = NULL -- NULL = All databases, DB_NAME() = Current database, 'DBName' = Filter by specific database

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @command NVARCHAR(MAX);
IF OBJECT_ID('tempdb..#TempResult') IS NOT NULL DROP TABLE #TempResult;
CREATE TABLE #TempResult (DatabaseName sysname, ObjectType sysname NULL, SchemaName sysname NULL, TableName sysname NULL
, RowCounts BIGINT NULL, TotalSpaceMB float NULL, UsedSpaceMB float NULL, UnusedSpaceMB float NULL);

SELECT @command = '
SELECT TOP (' + CONVERT(nvarchar(max), @TopPerDB) + N')
	DB_NAME() AS DatabaseName,
	it.internal_type_desc AS ObjectType,
	OBJECT_SCHEMA_NAME(p.object_id) AS SchemaName,
	OBJECT_NAME(p.object_id) AS TableName,
	SUM(p.rows) AS RowCounts,
	ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS TotalSpaceMB,
	ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS UsedSpaceMB, 
	ROUND(((SUM(a.total_pages) - SUM(a.used_pages)) * 8) / 1024.00, 2) AS UnusedSpaceMB
FROM 
	sys.system_internals_partitions p
INNER JOIN
	sys.internal_tables it on p.object_id = it.object_id
INNER JOIN 
	sys.allocation_units a ON p.partition_id = a.container_id
WHERE 
	p.rows >= ' + CONVERT(nvarchar(max), @MinimumRowCount) + N'
	AND it.internal_type_desc IN (''TRACKED_COMMITTED_TRANSACTIONS'', ''CHANGE_TRACKING'')
GROUP BY 
	p.object_id, it.internal_type_desc
HAVING
	SUM(a.total_pages) >= ' + CONVERT(nvarchar(max), @MinimumSizeMB) + N' * 1024.0 / 8
ORDER BY TotalSpaceMB DESC
OPTION(RECOMPILE);'

DECLARE @CurrDB sysname, @spExecuteSql nvarchar(1000)

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
AND database_id IN (SELECT database_id FROM sys.change_tracking_databases)
AND OBJECT_ID(QUOTENAME([name]) + N'.sys.system_internals_partitions') IS NOT NULL
AND (@FilterByDB IS NULL OR [name] LIKE @FilterByDB)

OPEN DBs

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO #TempResult
	EXEC @spExecuteSql @command
END

CLOSE DBs;
DEALLOCATE DBs;

SELECT *
, TrackedTableName = CASE WHEN ObjectType = 'CHANGE_TRACKING' THEN OBJECT_NAME(CONVERT(int, SUBSTRING(TableName, LEN('change_tracking_')+1, 256)), DB_ID(DatabaseName)) END
--Msg = CONCAT(QUOTENAME(DatabaseName),'.',QUOTENAME(SchemaName),'.',QUOTENAME(TableName), N' (', ObjectType, N'), row count: ', RowCounts, N', Used Space: ', UsedSpaceMB, N' MB'), TotalSpaceMB
FROM #TempResult
ORDER BY TotalSpaceMB DESC

DROP TABLE #TempResult;

