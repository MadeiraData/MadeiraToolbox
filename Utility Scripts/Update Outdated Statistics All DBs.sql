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
, @MinimumDaysOld INT = 35 -- adjust as needed
, @MaxDOP INT = NULL -- optionally force a specific MAXDOP option. set to 1 to reduce server workload.
, @SampleRatePercent INT = NULL -- set to number between 1 and 100 to force a specific sample rate, where 100 = FULLSCAN
, @ExecuteRemediation BIT = 0 -- set to 1 to execute the UPDATE STATISTICS remediation commands, otherwise print only
, @TimeLimitMinutes INT = 10 -- time limit in minutes to allow statistics to be updated

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
IF OBJECT_ID('tempdb..#tmpStats') IS NOT NULL DROP TABLE #tmpStats;
CREATE TABLE #tmpStats(
DBname SYSNAME NOT NULL,
DatabaseId INT NOT NULL,
ObjectId INT NOT NULL,
StatsName SYSNAME NOT NULL,
LastUpdate DATETIME NULL,
ModCntr BIGINT NULL,
TotalRows BIGINT NULL
);

DECLARE @qry NVARCHAR(MAX), @options NVARCHAR(MAX);

IF @MaxDOP IS NOT NULL
SET @options = ISNULL(@options + N', ', N' WITH ') + N'MAXDOP = ' + CONVERT(nvarchar(MAX), @MaxDOP)

IF @SampleRatePercent = 100
SET @options = ISNULL(@options + N', ', N' WITH ') + N'FULLSCAN'
ELSE IF @SampleRatePercent IS NOT NULL
SET @options = ISNULL(@options + N', ', N' WITH ') + N'SAMPLE ' + CONVERT(nvarchar(MAX), @SampleRatePercent) + N' PERCENT'

SET @qry = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT #tmpStats
SELECT
DB_NAME(),
DB_ID(),
stat.object_id,
stat.name,
MIN(sp.last_updated),
MAX(sp.modification_counter),
SUM(ps.rows)
FROM sys.objects AS t
INNER JOIN (
SELECT SUM(ps.rows) AS rows, ps.object_id
FROM sys.partitions ps 
WHERE ps.index_id <= 1 
GROUP BY ps.object_id
HAVING SUM(ps.rows) >= ' + CONVERT(nvarchar(MAX), @MinimumTableRows) + N'
) AS ps
ON t.object_id = ps.object_id 
INNER JOIN sys.stats AS stat ON t.object_id = stat.object_id
LEFT JOIN sys.indexes AS ix ON t.object_id = ix.object_id AND stat.stats_id = ix.index_id
OUTER APPLY
(
SELECT modification_counter, last_updated
FROM sys.dm_db_stats_properties(stat.object_id, stat.stats_id)
'
+ CASE WHEN OBJECT_ID('sys.dm_db_incremental_stats_properties') IS NULL THEN N'' ELSE 
N'UNION ALL
SELECT modification_counter, last_updated
FROM sys.dm_db_incremental_stats_properties(stat.object_id, stat.stats_id)
' END
+ N') AS sp
WHERE t.is_ms_shipped = 0
AND t.[type] = ''U''
AND (ix.index_id IS NULL OR (ix.is_disabled = 0 AND ix.is_hypothetical = 0 AND ix.type <= 2))
AND (sp.modification_counter IS NULL
	OR (sp.modification_counter >= ' + CONVERT(nvarchar(MAX), @MinimumModCountr) + N'
	  AND sp.last_updated < DATEADD(day, -' + CONVERT(nvarchar(MAX), @MinimumDaysOld) + N', GETDATE())
	  )
    )
GROUP BY stat.object_id,stat.name
OPTION (RECOMPILE, MAXDOP 1)' -- use MAXDOP 1 to avoid access violation bug

IF CONVERT(varchar(300),SERVERPROPERTY('Edition')) = 'SQL Azure'
BEGIN
exec (@qry)
END
ELSE
BEGIN
SET @qry = N'
IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND name = ''?'' AND state_desc = ''ONLINE'' AND DATABASEPROPERTYEX(name, ''Updateability'') = ''READ_WRITE'')
AND ''?'' NOT IN(''master'', ''model'', ''msdb'', ''tempdb'', ''ReportServerTempDB'', ''distribution'', ''SSISDB'')
AND HAS_DBACCESS(''?'') = 1
BEGIN
USE [?];'
+ @qry + N'
END'
exec sp_MSforeachdb @qry WITH RECOMPILE;
END

DECLARE @Msg NVARCHAR(4000), @StartTime DATETIME, @TimeLimitBreached BIT;

SET @Msg = N'-- ' + CONVERT(nvarchar(25), GETDATE(), 121) + N'  Found '
+ CONVERT(nvarchar(MAX),(SELECT COUNT(*) FROM #tmpStats))
+ N' statistic(s) to update.';
RAISERROR(N'%s',0,1,@Msg) WITH NOWAIT;

DECLARE Cmds CURSOR
LOCAL FAST_FORWARD
FOR
SELECT
Msg = N'ModCntr: ' + ISNULL(CAST(ModCntr as nvarchar(max)), N'(unknown)')
+ N', TotalRows: ' + CONVERT(nvarchar(MAX), ISNULL(TotalRows,0))
+ N', LastUpdate: ' + ISNULL(CONVERT(nvarchar(25), LastUpdate, 121), N'(never)')
, RemediationCmd = N'UPDATE STATISTICS ' + QUOTENAME(DB_NAME(databaseId)) COLLATE database_default
+ N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(objectId, databaseId)) COLLATE database_default
+ N'.' + QUOTENAME(OBJECT_NAME(objectId, databaseId)) COLLATE database_default
+ N' ' + QUOTENAME(statsName) COLLATE database_default
+ ISNULL(@options, N'')
+ N';'
FROM #tmpStats
ORDER BY
ModCntr DESC
, LastUpdate ASC

OPEN Cmds;
SET @TimeLimitBreached = 0;
SET @StartTime = GETDATE();

WHILE @TimeLimitBreached = 0
BEGIN
FETCH NEXT FROM Cmds INTO @Msg, @qry
IF @@FETCH_STATUS <> 0 BREAK;
IF DATEADD(minute, @TimeLimitMinutes, @StartTime) <= GETDATE()
BEGIN
	SET @TimeLimitBreached = 1;
	BREAK;
END

SET @Msg = CHAR(13) + CHAR(10) + N'-- ' + CONVERT(nvarchar(25), GETDATE(), 121) + N'  ' + @Msg
RAISERROR(N'%s
%s',0,1,@Msg,@qry) WITH NOWAIT;

IF @ExecuteRemediation = 1 EXEC (@qry);

END

CLOSE Cmds;
DEALLOCATE Cmds;

IF @TimeLimitBreached = 0
	SET @Msg = CHAR(13) + CHAR(10) + N'-- ' + CONVERT(nvarchar(25), GETDATE(), 121) + N'  Done.'
ELSE											      
	SET @Msg = CHAR(13) + CHAR(10) + N'-- ' + CONVERT(nvarchar(25), GETDATE(), 121) + N'  Forced stop due to time limit.';

RAISERROR(N'%s',0,1,@Msg) WITH NOWAIT;