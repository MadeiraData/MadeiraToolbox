/*
Update Outdated Statistics in All Databases
-------------------------------------------
Author: Eitan Blumin | https://madeiradata.com
Date: 2020-11-01
Description:
Use this script if you need a quick-and-dirty something to update all outdated statistics across all databases.
We always strongly recommend using Ola Hallengren's maintenance solution instead:
https://ola.hallengren.com
*/
DECLARE
  @MinimumTableRows INT = 200000
, @MinimumModCountr INT = 100000
, @MinimumDaysOld INT = 6
, @MaxDOP INT = NULL -- set to 1 to reduce server workload

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
IF OBJECT_ID('tempdb..#tmpStats') IS NOT NULL DROP TABLE #tmpStats;
CREATE TABLE #tmpStats(
DBname SYSNAME,
DatabaseId INT,
ObjectId INT,
StatsName SYSNAME,
LastUpdate DATETIME,
ModCntr BIGINT,
TotalRows BIGINT
);

DECLARE @qry NVARCHAR(MAX);
SET @qry = N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT #tmpStats
SELECT
DB_NAME(),
DB_ID(),
stat.object_id,
stat.name,
MIN(sp.last_updated),
MAX(sp.modification_counter),
SUM(ps.rows)
FROM sys.tables AS t
INNER JOIN (
SELECT SUM(ps.rows) AS rows, ps.object_id
FROM sys.partitions ps 
WHERE ps.index_id <= 1 
GROUP BY ps.object_id
HAVING SUM(ps.rows) >= ' + CONVERT(nvarchar, @MinimumTableRows) + N'
) AS ps
ON t.object_id = ps.object_id 
INNER JOIN sys.stats AS stat ON t.object_id = stat.object_id
CROSS APPLY
(
SELECT modification_counter, last_updated
FROM sys.dm_db_stats_properties(stat.object_id, stat.stats_id)
WHERE modification_counter >= ' + CONVERT(nvarchar, @MinimumModCountr) + N'
AND last_updated < DATEADD(day, -' + CONVERT(nvarchar, @MinimumDaysOld) + N', GETDATE())
'
+ CASE WHEN OBJECT_ID('sys.dm_db_incremental_stats_properties') IS NULL THEN N'' ELSE 
N'UNION ALL
SELECT modification_counter, last_updated
FROM sys.dm_db_incremental_stats_properties(stat.object_id, stat.stats_id)
WHERE modification_counter >= ' + CONVERT(nvarchar, @MinimumModCountr) + N'
AND last_updated < DATEADD(day, -' + CONVERT(nvarchar, @MinimumDaysOld) + N', GETDATE())
' END
+ N') AS sp
GROUP BY stat.object_id,stat.name'

IF CONVERT(varchar(300),SERVERPROPERTY('Edition')) = 'SQL Azure'
BEGIN
exec (@qry)
END
ELSE
BEGIN
SET @qry = N'
IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND name = ''?'' AND state_desc = ''ONLINE'' AND DATABASEPROPERTYEX(name, ''Updateability'') = ''READ_WRITE'')
AND ''?'' NOT IN(''master'', ''model'', ''msdb'', ''tempdb'', ''ReportServerTempDB'', ''distribution'', ''SSISDB'')
BEGIN
USE [?];'
+ @qry + N'
END'
exec sp_MSforeachdb @qry
END

PRINT @qry

DECLARE Remediate CURSOR LOCAL FAST_FORWARD
FOR
SELECT
--[database_name] = DB_NAME(databaseId)
--, [schema_name] = OBJECT_SCHEMA_NAME(objectId, databaseId)
--, [table_name] = OBJECT_NAME(objectId, databaseId)
--, statsName
--, ModCntr
--, LastUpdate,
 RemediationCmd = N'USE ' + QUOTENAME(DB_NAME(databaseId)) + N'; UPDATE STATISTICS ' + QUOTENAME(DB_NAME(databaseId))
+ N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(objectId, databaseId))
+ N'.' + QUOTENAME(OBJECT_NAME(objectId, databaseId))
+ N' ' + QUOTENAME(statsName)
+ CASE WHEN @MaxDOP IS NULL THEN N'' ELSE N' WITH MAXDOP = ' + CONVERT(nvarchar, @MaxDOP) END
+ N';'
FROM #tmpStats
ORDER BY
ModCntr DESC,
LastUpdate ASC

OPEN Remediate
FETCH NEXT FROM Remediate INTO @qry

WHILE @@FETCH_STATUS = 0
BEGIN
	PRINT @qry;
	EXEC(@qry);
	FETCH NEXT FROM Remediate INTO @qry;
END

CLOSE Remediate
DEALLOCATE Remediate 