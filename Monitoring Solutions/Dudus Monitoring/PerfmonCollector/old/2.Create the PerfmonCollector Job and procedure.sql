USE DB_DBA
GO
---------------------------------------------
-----------Capture Snapshot script-----------
---------------------------------------------
IF EXISTS(SELECT TOP 1 1 FROM SYS.procedures WHERE  object_id = OBJECT_ID('Perfmon.usp_CounterCollector'))
	DROP PROCEDURE [Perfmon].[usp_CounterCollector]
GO 

CREATE PROCEDURE [Perfmon].[usp_CounterCollector]
AS 
BEGIN 
		BEGIN TRY 

			BEGIN TRAN 

				DECLARE @ProcesseId BIGINT 
				INSERT INTO [Perfmon].[CounterCollectorProcesses] (StartDateTime) VALUES (DEFAULT)

				SELECT @ProcesseId = SCOPE_IDENTITY()

				INSERT INTO [Perfmon].[CounterCollector](ProcesseId,CounterId,CounterValue)
				SELECT  @ProcesseId,[Counters].[Id],OPC.[cntr_value]
				FROM	sys.dm_os_performance_counters  OPC
				JOIN	[Perfmon].[Counters] ON [Counters].[CounterName] = [OPC].[counter_name] AND [Counters].[InstanceName] = [OPC].[instance_name] AND [Counters].[ObjectName] = [OPC].[object_name] AND [Counters].[TypeId] = 1 

				UPDATE [Perfmon].[CounterCollectorProcesses]
					SET EndDateTime = SYSDATETIME()
				WHERE Id = @ProcesseId

			COMMIT 

		END TRY 

		BEGIN CATCH 
			IF @@TRANCOUNT > 0 
				ROLLBACK;
			THROW;
		END CATCH;
END 
GO 

USE [msdb]
GO
DECLARE @jobId BINARY(16)
EXEC  msdb.dbo.sp_add_job @job_name=N'StatisticsCollector', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'StatisticsCollector', @server_name = N'WIN-4N0UG99QPHC\INSTANCE1'
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'StatisticsCollector', @step_name=N'PerfmonCollector', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [Perfmon].[usp_CounterCollector]', 
		@database_name=N'DB_DBA', 
		@flags=0
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'StatisticsCollector', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'WIN-4N0UG99QPHC\MADEIRA', 
		@notify_email_operator_name=N'', 
		@notify_netsend_operator_name=N'', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'StatisticsCollector', @name=N'Daily-5Min', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20141110, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO