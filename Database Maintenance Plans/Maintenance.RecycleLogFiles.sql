-- Change the number of SQL Server log files to 30 and the max size of each file to 1GB

USE
	master;
GO


EXECUTE sys.xp_instance_regwrite
	N'HKEY_LOCAL_MACHINE' ,
	N'Software\Microsoft\MSSQLServer\MSSQLServer' ,
	N'NumErrorLogs' ,
	REG_DWORD ,
	30;
GO


EXECUTE sys.xp_instance_regwrite
	N'HKEY_LOCAL_MACHINE' ,
	N'Software\Microsoft\MSSQLServer\MSSQLServer' ,
	N'ErrorLogSizeInKb' ,
	REG_DWORD ,
	1048576;
GO



-- Create a job that runs every midnight and recycles the SQL Server log files
-- as well as the SQL Server Agent log files

USE
	msdb;
GO
DECLARE @saLogin sysname = SUSER_SNAME(0x01);

EXECUTE dbo.sp_add_job
	@job_name				= N'Maintenance.RecycleLogFiles' ,
	@enabled				= 1 ,
	@start_step_id			= 1 ,
	@notify_level_eventlog	= 0 ,
	@notify_level_email		= 2 ,
	@notify_level_netsend	= 2 ,
	@notify_level_page		= 2 ,
	@delete_level			= 0 ,
	@description			= N'This job runs every midnight and recylces the SQL Server log files as well as the SQL Server Agent log files' ,
	@category_name			= N'Database Maintenance' ,
	@owner_login_name		= @saLogin ,
	@job_id					= NULL;
GO


EXECUTE dbo.sp_add_jobserver
	@job_name		= N'Maintenance.RecycleLogFiles' ,
	@server_name	= N'(LOCAL)';
GO


EXECUTE dbo.sp_add_jobstep
	@job_name				= N'Maintenance.RecycleLogFiles' ,
	@step_name				= N'RecycleSQLServerLogFiles' ,
	@step_id				= 1 ,
	@cmdexec_success_code	= 0 ,
	@on_success_action		= 3 ,
	@on_fail_action			= 2 ,
	@retry_attempts			= 0 ,
	@retry_interval			= 0 ,
	@os_run_priority		= 0 ,
	@subsystem				= N'TSQL' ,
	@command				= N'DECLARE @LogSizeThresholdMB BIGINT = 50;
SET NOCOUNT ON;
DECLARE @Logs AS TABLE
(
 ArchiveNum SMALLINT,
 LastModified DATETIME,
 LogSizeBytes BIGINT
);

INSERT INTO @Logs
EXEC xp_enumerrorlogs 1

IF EXISTS(
    SELECT *
    FROM @Logs
    WHERE ArchiveNum = 0
    AND LogSizeBytes >= @LogSizeThresholdMB * 1024 * 1024.0
    )
EXECUTE sys.sp_cycle_errorlog;' ,
	@database_name			= N'master' ,
	@flags					= 0;
GO


EXECUTE dbo.sp_add_jobstep
	@job_name				= N'Maintenance.RecycleLogFiles' ,
	@step_name				= N'RecycleSQLServerAgentLogFiles' ,
	@step_id				= 2 ,
	@cmdexec_success_code	= 0 ,
	@on_success_action		= 1 ,
	@on_fail_action			= 2 ,
	@retry_attempts			= 0 ,
	@retry_interval			= 0 ,
	@os_run_priority		= 0 ,
	@subsystem				= N'TSQL' ,
	@command				= N'DECLARE @LogSizeThresholdMB BIGINT = 50;
SET NOCOUNT ON;
DECLARE @Logs AS TABLE
(
 ArchiveNum SMALLINT,
 LastModified DATETIME,
 LogSizeBytes BIGINT
);

INSERT INTO @Logs
EXEC xp_enumerrorlogs 2

IF EXISTS(
    SELECT *
    FROM @Logs
    WHERE ArchiveNum = 0
    AND LogSizeBytes >= @LogSizeThresholdMB * 1024 * 1024.0
    )
EXEC msdb.dbo.sp_cycle_agent_errorlog;' ,
	@database_name			= N'msdb' ,
	@flags					= 0;
GO


DECLARE @ActiveStartDate AS INT;

SET @ActiveStartDate = CAST (CONVERT (NCHAR(8) , SYSDATETIME () , 112) AS INT);

EXECUTE dbo.sp_add_jobschedule
	@job_name				= N'Maintenance.RecycleLogFiles' ,
	@name					= N'EveryMidnight' ,
	@enabled				= 1 ,
	@freq_type				= 4 ,	-- Daily
	@freq_interval			= 1 ,	-- Once
	@freq_subday_type		= 1 ,	-- At the specified time
	@freq_subday_interval	= 0 ,
	@freq_relative_interval	= 0 ,
	@freq_recurrence_factor	= 0 ,
	@active_start_date		= @ActiveStartDate ,
	@active_end_date		= 99991231 ,
	@active_start_time		= 0 ,
	@active_end_time		= 0 ,
	@schedule_id			= NULL;
GO
