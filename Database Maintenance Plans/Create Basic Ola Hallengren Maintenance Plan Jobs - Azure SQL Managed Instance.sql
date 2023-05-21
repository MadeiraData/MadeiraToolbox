/*
Create Basic Maintenance Plan Jobs
==================================
Author: Eitan Blumin
Date: 2022-03-07
Description:
Assuming that you have already installed Ola Hallengren's maintenance solution: https://ola.hallengren.com
But you don't necessarily have proper maintenance jobs created.

This script creates two basic jobs:

Maintenance.CleanupHistory:
	- sp_delete_backuphistory
	- sp_purge_jobhistory
	- sp_maintplan_delete_log
	- CommandLog Cleanup
	- Output File Cleanup

Maintenance.IntegrityAndIndex:
	- IntegrityChecks - SYSTEM
	- IntegrityChecks - USER DBs
	- IndexDefrag
	- UpdateStatistics

The script automatically detects the sa-equivalent login name and uses it as the job owner.
It also detects the default LOG folder and uses it as the step output file folder.
*/

USE msdb

DECLARE @DatabaseName sysname = DB_NAME() -- N'msdb'
DECLARE @jobOwner sysname = CASE WHEN IS_SRVROLEMEMBER('sysadmin') = 1 THEN SUSER_SNAME(0x01) ELSE SUSER_SNAME() END
DECLARE @LogDirectory nvarchar(4000)

IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 12
BEGIN
	SET @LogDirectory = N'$(ESCAPE_SQUOTE(SQLLOGDIR))'
END
ELSE IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 11
BEGIN
	SELECT @LogDirectory = [path]
	FROM sys.dm_os_server_diagnostics_log_configurations
	OPTION(RECOMPILE)
END
ELSE
BEGIN
	SET @LogDirectory = LEFT(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)),LEN(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max))) - CHARINDEX('\',REVERSE(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max))))) + N'\'
END

DECLARE @LogOutput nvarchar(4000)
DECLARE @jobId BINARY(16)
DECLARE @ReturnCode INT
SET @ReturnCode = 0

IF IS_SRVROLEMEMBER('sysadmin') = 0 OR SERVERPROPERTY('EngineEdition') = 8
	SET @LogOutput = NULL
ELSE IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 13
	SET @LogOutput = N'$(ESCAPE_SQUOTE(SQLLOGDIR))\$(ESCAPE_SQUOTE(JOBNAME))_$(ESCAPE_SQUOTE(STEPNAME))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt'
ELSE IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 12
	SET @LogOutput = N'$(ESCAPE_SQUOTE(SQLLOGDIR))\$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt'
ELSE
	SET @LogOutput = @LogDirectory + N'$(ESCAPE_SQUOTE(JOBID))_$(ESCAPE_SQUOTE(STEPID))_$(ESCAPE_SQUOTE(DATE))_$(ESCAPE_SQUOTE(TIME)).txt'

RAISERROR(N'Ola Hallengren Maintenance Solution Database: %s
sa login name: %s
SQL Log Directory: %s
SQL Log Output: %s',0,1,@DatabaseName, @jobOwner, @LogDirectory, @LogOutput) WITH NOWAIT;

BEGIN TRANSACTION
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
END

EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Maintenance.CleanupHistory', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Source: https://ola.hallengren.com', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@jobOwner, @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CommandLog Cleanup', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=2, 
		@retry_interval=5, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET DEADLOCK_PRIORITY LOW;
DELETE FROM [dbo].[CommandLog]
WHERE StartTime < DATEADD(dd,-30,GETDATE())', 
		@database_name=@DatabaseName, 
		@output_file_name=@LogOutput, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'sp_delete_backuphistory', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=2, 
		@retry_interval=5, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET DEADLOCK_PRIORITY LOW;
DECLARE @CleanupDate datetime
SET @CleanupDate = DATEADD(dd,-30,GETDATE())
EXECUTE dbo.sp_delete_backuphistory @oldest_date = @CleanupDate', 
		@database_name=N'msdb', 
		@output_file_name=@LogOutput, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'sp_purge_jobhistory', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=2, 
		@retry_interval=5, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET DEADLOCK_PRIORITY LOW;
DECLARE @CleanupDate datetime
SET @CleanupDate = DATEADD(dd,-30,GETDATE())
EXECUTE dbo.sp_purge_jobhistory @oldest_date = @CleanupDate', 
		@database_name=N'msdb', 
		@output_file_name=@LogOutput, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'sp_maintplan_delete_log', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=2, 
		@retry_interval=5, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET DEADLOCK_PRIORITY LOW;
DECLARE @dt datetime; 
SET @dt = DATEADD(DAY,-30,GETDATE());

EXECUTE msdb..sp_maintplan_delete_log null,null,@dt;', 
		@database_name=N'msdb', 
		@output_file_name=@LogOutput, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Midnight', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200520, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = @@SERVERNAME
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

SET @jobId = NULL;

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
END

EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Maintenance.IntegrityAndIndex', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Source: https://ola.hallengren.com', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@jobOwner, @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'IntegrityChecks - USER DBs', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE [dbo].[DatabaseIntegrityCheck]
@Databases = ''USER_DATABASES'',
@MaxDOP=4,
@LogToTable = ''Y''', 
		@database_name=@DatabaseName, 
		@output_file_name=@LogOutput, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'IndexDefrag', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE [dbo].[IndexOptimize]
    @Databases = ''USER_DATABASES'' ,
    @FragmentationLow = NULL,
    @FragmentationMedium = ''INDEX_REORGANIZE,INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE'',
    @FragmentationHigh = ''INDEX_REBUILD_ONLINE,INDEX_REORGANIZE,INDEX_REBUILD_OFFLINE'',
    @FragmentationLevel1 = 10,
    @FragmentationLevel2 = 40,
    @SortInTempdb = ''Y'',
    @PartitionLevel = ''Y'',
    @LogToTable = ''Y''', 
		@database_name=@DatabaseName, 
		@output_file_name=@LogOutput, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'UpdateStatistics', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE [dbo].[IndexOptimize]
    @Databases = ''USER_DATABASES'' ,
    @FragmentationLow = NULL ,
    @FragmentationMedium = NULL ,
    @FragmentationHigh = NULL ,
    @UpdateStatistics = ''ALL'' ,
    @OnlyModifiedStatistics = N''Y'' ,
    @LogToTable = ''Y'';', 
		@database_name=@DatabaseName, 
		@output_file_name=@LogOutput, 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'IndexAndIntegrityChecks', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=64, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20200613, 
		@active_end_date=99991231, 
		@active_start_time=10000, 
		@active_end_time=235959
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = @@SERVERNAME
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO