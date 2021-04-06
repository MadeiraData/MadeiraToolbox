DECLARE
	@TopPerDB		INT		= 100,
	@MinimumRowCount	INT		= 1000,
	@MinimumUnusedSizeMB	INT		= 1024,
	@MinimumUnusedSpacePct	INT		= 60,
	@RebuildIndexOptions	VARCHAR(MAX)	= 'ONLINE = ON, MAXDOP = 1'

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @command NVARCHAR(MAX);
DECLARE @TempResult AS TABLE (DatabaseName sysname, SchemaName sysname NULL, TableName sysname NULL, IndexName sysname NULL
, DatabaseID INT, ObjectId INT, IndexId INT
, CompressionType TINYINT, CompressionType_Desc AS (CASE CompressionType WHEN 0 THEN 'NONE' WHEN 1 THEN 'ROW' WHEN 2 THEN 'PAGE' WHEN 3 THEN 'COLUMNSTORE' WHEN 4 THEN 'COLUMNSTORE_ARCHIVE' ELSE 'UNKNOWN' END)
, RowCounts BIGINT NULL, TotalSpaceMB FLOAT NULL, UsedSpaceMB FLOAT NULL, UnusedSpaceMB FLOAT NULL
, UserSeeks INT NULL, UserScans INT NULL, UserLookups INT NULL, UserUpdates INT NULL);

SELECT @command = 'IF EXISTS (SELECT * FROM sys.databases WHERE state = 0 AND is_read_only = 0 AND database_id > 4 AND is_distributor = 0 AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
SELECT TOP (' + CONVERT(nvarchar(max), @TopPerDB) + N')
	DB_NAME() AS DatabaseName,
	s.name AS SchemaName,
	t.name AS TableName,
	i.name AS IndexName,
	DB_ID(), i.object_id, i.index_id,
	MAX(p.data_compression) AS CompressionType,
	SUM(p.rows) AS RowCounts,
	ROUND(SUM(a.total_pages) / 128.0, 2) AS TotalSpaceMB,
	ROUND(SUM(a.used_pages) / 128.0, 2) AS UsedSpaceMB, 
	ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.0, 2) AS UnusedSpaceMB,
	MAX(us.user_seeks) AS user_seeks,
	MAX(us.user_scans) AS user_scans,
	MAX(us.user_lookups) AS user_lookups,
	MAX(us.user_updates) AS user_updates
FROM 
	sys.tables t
INNER JOIN 
	sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN      
	sys.indexes i ON t.object_id = i.object_id
INNER JOIN 
	sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN 
	sys.allocation_units a ON p.partition_id = a.container_id
LEFT JOIN
	sys.dm_db_index_usage_stats AS us ON us.database_id = DB_ID() AND us.object_id = t.object_id AND us.index_id = i.index_id
WHERE 
	t.name NOT LIKE ''dt%''
	AND s.name <> ''sys''
	AND t.is_ms_shipped = 0
	AND i.object_id > 255 
	and p.rows >= ' + CONVERT(nvarchar(max), @MinimumRowCount) + N'
GROUP BY 
	t.Name, s.Name, i.name,
	i.object_id, i.index_id
HAVING
	ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.0, 2) >= ' + CONVERT(nvarchar(max), @MinimumUnusedSizeMB) + N'
	AND (SUM(a.used_pages) * 1.0) / SUM(a.total_pages) <= 1 - (' + CONVERT(nvarchar(max), @MinimumUnusedSpacePct) + ' / 100.0)
ORDER BY TotalSpaceMB DESC
END'

INSERT INTO @TempResult
EXEC sp_MSforeachdb @command

SELECT r.*
, UnusedSpacePercent = UnusedSpaceMB * 1.0 / TotalSpaceMB * 100.0
, frg.avg_fragmentation_in_percent
, frg.page_count
, RebuildCommand = 'ALTER INDEX ' + QUOTENAME(IndexName) + ' ON ' + QUOTENAME(SChemaName) + '.' + QUOTENAME(TableName) + ' REBUILD' + ISNULL(N' WITH(' + NULLIF(@RebuildIndexOptions,N'') + N')', N'')
FROM @TempResult AS r
OUTER APPLY sys.dm_db_index_physical_stats(DatabaseID, ObjectId, IndexId, NULL, 'LIMITED') AS frg
ORDER BY UnusedSpaceMB DESC
OPTION (MAXDOP 1)
