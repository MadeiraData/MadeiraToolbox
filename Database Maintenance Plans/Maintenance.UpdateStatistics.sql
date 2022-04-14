USE [msdb]
GO
DECLARE @jobId BINARY(16)
DECLARE @owner sysname = SUSER_SNAME(0x01)
EXEC  msdb.dbo.sp_add_job @job_name=N'Maintenance.UpdateStatistics', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Author: Eitan Blumin', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@owner, @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'Maintenance.UpdateStatistics', @server_name = '(local)'
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'Maintenance.UpdateStatistics', @step_name=N'UpdateStats', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'/*
Update Outdated Statistics in All Databases
-------------------------------------------
Author: Eitan Blumin | https://madeiradata.com
Date: 2020-11-01
Description:
Use this script if you need a quick-and-dirty something to update all outdated statistics across all databases.
We always strongly recommend using Ola Hallengren''s maintenance solution instead:
https://ola.hallengren.com
*/
DECLARE
  @MinimumTableRows INT = 200000
, @MinimumModCountr INT = 100000
, @MinimumDaysOld INT = 14 -- adjust as needed
, @MaxDOP INT = NULL -- optionally force a specific MAXDOP option. set to 1 to reduce server workload.
, @SampleRatePercent INT = NULL -- set to number between 1 and 100 to force a specific sample rate, where 100 = FULLSCAN
, @ExecuteRemediation BIT = 1 -- set to 1 to execute the UPDATE STATISTICS remediation commands, otherwise print only
, @TimeLimitMinutes INT = 40 -- time limit in minutes to allow statistics to be updated

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
IF OBJECT_ID(''tempdb..#tmpStats'') IS NOT NULL DROP TABLE #tmpStats;
CREATE TABLE #tmpStats(
DBname SYSNAME NOT NULL,
databaseId INT NOT NULL,
objectId INT NOT NULL,
statsName SYSNAME NOT NULL,
LastUpdate DATETIME NULL,
ModCntr BIGINT NULL,
TotalRows BIGINT NULL
);

DECLARE @qry NVARCHAR(MAX), @options NVARCHAR(MAX);

IF @MaxDOP IS NOT NULL
SET @options = ISNULL(@options + N'', '', N'' WITH '') + N''MAXDOP = '' + CONVERT(nvarchar(MAX), @MaxDOP)

IF @SampleRatePercent = 100
SET @options = ISNULL(@options + N'', '', N'' WITH '') + N''FULLSCAN''
ELSE IF @SampleRatePercent IS NOT NULL
SET @options = ISNULL(@options + N'', '', N'' WITH '') + N''SAMPLE '' + CONVERT(nvarchar(MAX), @SampleRatePercent) + N'' PERCENT''

SET @qry = N''SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT #tmpStats
SELECT
DB_NAME(),
DB_ID(),
stat.object_id,
stat.name,
ISNULL(MIN(sp.last_updated), STATS_DATE(stat.object_id, stat.stats_id)),
ISNULL(MAX(sp.modification_counter),0),
SUM(ps.rows)
FROM sys.objects AS t
INNER JOIN (
SELECT SUM(ps.rows) AS rows, ps.object_id
FROM sys.partitions ps 
WHERE ps.index_id <= 1 
GROUP BY ps.object_id
HAVING SUM(ps.rows) >= '' + CONVERT(nvarchar(MAX), @MinimumTableRows) + N''
) AS ps
ON t.object_id = ps.object_id 
INNER JOIN sys.stats AS stat ON t.object_id = stat.object_id
LEFT JOIN sys.indexes AS ix ON t.object_id = ix.object_id AND stat.stats_id = ix.index_id
OUTER APPLY
(
SELECT modification_counter, last_updated
FROM sys.dm_db_stats_properties(stat.object_id, stat.stats_id)
''
+ CASE WHEN OBJECT_ID(''sys.dm_db_incremental_stats_properties'') IS NULL THEN N'''' ELSE 
N''UNION ALL
SELECT modification_counter, last_updated
FROM sys.dm_db_incremental_stats_properties(stat.object_id, stat.stats_id)
'' END
+ N'') AS sp
WHERE t.is_ms_shipped = 0
AND t.[type] = ''''U''''
AND (ix.index_id IS NULL OR (ix.is_disabled = 0 AND ix.is_hypothetical = 0 AND ix.type <= 2))
AND sp.modification_counter >= '' + CONVERT(nvarchar(MAX), @MinimumModCountr) + N''
AND ISNULL(sp.last_updated, STATS_DATE(stat.object_id, stat.stats_id)) < DATEADD(day, -'' + CONVERT(nvarchar(MAX), @MinimumDaysOld) + N'', GETDATE())
GROUP BY stat.object_id,stat.name,stat.stats_id
OPTION (RECOMPILE, MAXDOP 1)'' -- use MAXDOP 1 to avoid access violation bug

IF CONVERT(int, SERVERPROPERTY(''EngineEdition'')) = 5
BEGIN
exec (@qry)
END
ELSE
BEGIN
SET @qry = N''
IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND name = ''''?'''' AND state_desc = ''''ONLINE'''' AND DATABASEPROPERTYEX(name, ''''Updateability'''') = ''''READ_WRITE'''')
AND ''''?'''' NOT IN(''''master'''', ''''model'''', ''''msdb'''', ''''tempdb'''', ''''ReportServerTempDB'''', ''''distribution'''', ''''SSISDB'''')
AND HAS_DBACCESS(''''?'''') = 1
BEGIN
USE [?];''
+ @qry + N''
END''
exec sp_MSforeachdb @qry WITH RECOMPILE;
END

DECLARE @Msg NVARCHAR(4000), @StartTime DATETIME, @TimeLimitBreached BIT;

SET @Msg = N''-- '' + CONVERT(nvarchar(25), GETDATE(), 121) + N''  Found ''
+ CONVERT(nvarchar(MAX),(SELECT COUNT(*) FROM #tmpStats))
+ N'' statistic(s) to update.'';
RAISERROR(N''%s'',0,1,@Msg) WITH NOWAIT;

DECLARE Cmds CURSOR
LOCAL FAST_FORWARD
FOR
SELECT
Msg = N''ModCntr: '' + ISNULL(CAST(ModCntr as nvarchar(max)), N''(unknown)'')
+ N'', TotalRows: '' + CONVERT(nvarchar(MAX), ISNULL(TotalRows,0))
+ N'', LastUpdate: '' + ISNULL(CONVERT(nvarchar(25), LastUpdate, 121), N''(never)'')
, RemediationCmd = N''UPDATE STATISTICS '' + QUOTENAME(DB_NAME(databaseId)) COLLATE database_default
+ N''.'' + QUOTENAME(OBJECT_SCHEMA_NAME(objectId, databaseId)) COLLATE database_default
+ N''.'' + QUOTENAME(OBJECT_NAME(objectId, databaseId)) COLLATE database_default
+ N'' '' + QUOTENAME(statsName) COLLATE database_default
+ ISNULL(@options, N'''')
+ N'';''
FROM #tmpStats
WHERE LastUpdate < DATEADD(day, -@MinimumDaysOld, GETDATE()) OR LastUpdate IS NULL
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

SET @Msg = CHAR(13) + CHAR(10) + N''-- '' + CONVERT(nvarchar(25), GETDATE(), 121) + N''  '' + @Msg
RAISERROR(N''%s
%s'',0,1,@Msg,@qry) WITH NOWAIT;

IF @ExecuteRemediation = 1 EXEC (@qry);

END

CLOSE Cmds;
DEALLOCATE Cmds;

IF @TimeLimitBreached = 0
	SET @Msg = CHAR(13) + CHAR(10) + N''-- '' + CONVERT(nvarchar(25), GETDATE(), 121) + N''  Done.''
ELSE											      
	SET @Msg = CHAR(13) + CHAR(10) + N''-- '' + CONVERT(nvarchar(25), GETDATE(), 121) + N''  Forced stop due to time limit.'';

RAISERROR(N''%s'',0,1,@Msg) WITH NOWAIT;', 
		@database_name=N'master', 
		@flags=4
GO
USE [msdb]
GO
DECLARE @owner sysname = SUSER_SNAME(0x01)

EXEC msdb.dbo.sp_update_job @job_name=N'Maintenance.UpdateStatistics', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'Author: Eitan Blumin', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@owner, 
		@notify_email_operator_name=N'', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'Maintenance.UpdateStatistics', @name=N'1AM', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20220324, 
		@active_end_date=99991231, 
		@active_start_time=10000, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO
