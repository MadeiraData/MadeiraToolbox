USE [db_dba];
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
SET ANSI_PADDING ON;
GO

IF SCHEMA_ID('Mirroring') IS NULL 
	BEGIN 
		DECLARE @Command NVARCHAR(MAX) = N'CREATE SCHEMA [Mirroring]';
		EXEC (@Command);
	END;

GO
CREATE TABLE [Mirroring].[DBMirroringStateChanges](
	ID [INT] IDENTITY CONSTRAINT  [PK_DBMirroringStateChanges] PRIMARY KEY CLUSTERED,
	[EventTime] [DATETIME2] NOT NULL,
	[EventDescription] [varchar](max) NOT NULL,
	[NewState] [int] NOT NULL,
	[Database] [varchar](max) NOT NULL
) ON [PRIMARY];

GO
SET ANSI_PADDING OFF;
GO

-- Create job to execute when database mirroring event states change
-- The job does two things: 1)inserts a mirroring history 
-- row by calling sys.sp_dbmmonitorupdate and 2) records
-- information from the WMI event about the state change
-- Job: [DB Mirroring: Record State Changes]
BEGIN TRANSACTION
DECLARE @ReturnCode INT;
SELECT @ReturnCode = 0;

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories 
   WHERE name=N'Database Mirroring' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', 
   @type=N'LOCAL', @name=N'Database Mirroring';
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;

END;

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'DB Mirroring: Record State Changes', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Called by alert that responds to DBM state change events', 
		@category_name=N'Database Mirroring', 
		@owner_login_name=N'WIN-4N0UG99QPHC\MADEIRA', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Insert History Row]    Script Date: 11/4/2014 6:15:11 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Insert History Row', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=4, 
		@on_success_step_id=2, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC sys.sp_dbmmonitorupdate', 
		@database_name=N'msdb', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Record State Changes]    Script Date: 11/4/2014 6:15:11 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Record State Changes', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'INSERT INTO [Mirroring].[DBMirroringStateChanges] (
      [EventTime],
      [EventDescription],
      [NewState],
      [Database] )
   VALUES (
      SYSDATETIME(),
      ''$(ESCAPE_SQUOTE(WMI(TextData)))'',
      $(ESCAPE_SQUOTE(WMI(State))),
      ''$(ESCAPE_SQUOTE(WMI(DatabaseName)))'' )', 
		@database_name=N'DB_DBA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

-- The namespace must include the instance name from which the 
-- WMI events will originate. For example, if the instance is 
-- the default instance, use 'MSSQLSERVER'. If the instance is 
-- SVR1\INSTANCE, use 'INSTANCE'
DECLARE @namespace NVARCHAR(200);
IF (SERVERPROPERTY('InstanceName') IS NOT null)
BEGIN
   SELECT @namespace = N'\\.\root\Microsoft\SqlServer\ServerEvents\'
      + CONVERT(NVARCHAR(128), SERVERPROPERTY('InstanceName'));
END;
ELSE
BEGIN
   SELECT @namespace = N'\\.\root\Microsoft\SqlServer\ServerEvents\MSSQLSERVER';
END;
EXEC msdb.dbo.sp_add_alert @name=N'DB Mirroring: State Changes', 
   @message_id=0, 
   @severity=0, 
   @enabled=1, 
   @delay_between_responses=0, 
   @include_event_description_in=0, 
   @category_name=N'Database Mirroring', 
   @wmi_namespace=@namespace,
   @wmi_query=N'SELECT * FROM DATABASE_MIRRORING_STATE_CHANGE WHERE (state > 4 AND state != 13 AND  state != 11 )', 
   @job_id=@jobId;

Quit_Alert: