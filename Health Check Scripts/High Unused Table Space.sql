DECLARE
    @TopPerDB INT = 100,
    @MinimumRowCount INT = 1000,
    @MinimumUnusedSizeMB INT = 1024,
    @MinimumUnusedSpacePct INT = 60

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @command NVARCHAR(MAX);
DECLARE @TempResult AS TABLE (
DatabaseName sysname, SchemaName sysname NULL, TableName sysname NULL, IndexName sysname NULL
, TotalSpaceMB MONEY NULL, UnusedSpaceMB MONEY NULL);

SELECT @command = 'IF EXISTS (SELECT * FROM sys.databases WHERE state = 0 AND is_read_only = 0 AND database_id > 4 AND is_distributor = 0 AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
SELECT TOP (' + CONVERT(nvarchar(max), @TopPerDB) + N')
    DB_NAME() AS DatabaseName,
    s.name AS SchemaName,
    t.name AS TableName,
    i.name AS IndexName,
    ROUND(SUM(a.total_pages) / 128.0, 2) AS TotalSpaceMB,
    ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.0, 2) AS UnusedSpaceMB
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
WHERE 
    t.name NOT LIKE ''dt%''
    AND s.name <> ''sys''
    AND t.is_ms_shipped = 0
    AND i.object_id > 255 
    AND p.rows >= 1
GROUP BY 
    t.Name, s.Name, i.name
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
