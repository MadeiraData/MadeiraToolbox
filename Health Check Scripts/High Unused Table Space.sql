DECLARE
    @TopPerDB int = 100,
    @MinimumRowCount int = 1000,
    @MinimumUnusedSizeMB int = 1024,
    @MinimumUnusedSpacePct int = 50

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @command NVARCHAR(MAX);
DECLARE @TempResult AS TABLE (
DatabaseName sysname, SchemaName sysname NULL, TableName sysname NULL, IndexName sysname NULL
, TotalSpaceMB money NULL, UnusedSpaceMB money NULL);

SELECT @command = 'IF EXISTS (SELECT * FROM sys.databases WHERE state = 0 AND is_read_only = 0 AND database_id > 4 AND is_distributor = 0 AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
SELECT TOP (' + CONVERT(nvarchar(max), @TopPerDB) + N')
    DB_NAME() AS DatabaseName,
    OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    ROUND(SUM(a.total_pages) / 128.0, 2) AS TotalSpaceMB,
    ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.0, 2) AS UnusedSpaceMB
FROM 
    sys.indexes i
INNER JOIN 
    sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN 
    sys.allocation_units a ON p.partition_id = a.container_id
WHERE 
    OBJECT_NAME(i.object_id) NOT LIKE ''dt%''
    AND OBJECT_SCHEMA_NAME(i.object_id) <> ''sys''
    AND i.object_id > 255 
    AND p.rows >= 1
GROUP BY 
    i.object_id, i.name
HAVING
    SUM(p.rows) >= ' + CONVERT(nvarchar(max), @MinimumRowCount) + N'
    AND (SUM(a.total_pages) - SUM(a.used_pages)) / 128 >= ' + CONVERT(nvarchar(max), @MinimumUnusedSizeMB) + N'
    AND (SUM(a.used_pages) * 1.0) / SUM(a.total_pages) <= 1 - (' + CONVERT(nvarchar(max), @MinimumUnusedSpacePct) + ' / 100.0)
ORDER BY UnusedSpaceMB DESC
END'

INSERT INTO @TempResult
EXEC sp_MSforeachdb @command

SELECT 
  N'In server ' + QUOTENAME(@@SERVERNAME) + N', table object: '
+ QUOTENAME(DatabaseName)
+ N'.' + QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName)
+ ISNULL(N'.' + QUOTENAME(IndexName), N' (heap)')
+ N', unused space: ' + CONVERT(nvarchar(max), UnusedSpaceMB, 1)
+ N' MB / ' + CONVERT(nvarchar(max), TotalSpaceMB, 1)
+ N' MB ( ' + CONVERT(nvarchar(max), UnusedSpaceMB / TotalSpaceMB * 100) + N' % )' AS Msg
, UnusedSpaceMB
FROM @TempResult AS r
ORDER BY UnusedSpaceMB DESC
OPTION (MAXDOP 1)
