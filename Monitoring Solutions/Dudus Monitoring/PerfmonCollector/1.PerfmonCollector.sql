USE [DB_DBA]
GO 
IF SCHEMA_ID('Perfmon') IS NULL 
	BEGIN 
		DECLARE @SQL NVARCHAR(MAX) = N'CREATE SCHEMA [Perfmon]'
		EXEC (@SQL)
	END 
GO 


IF OBJECT_ID('Perfmon.CounterCollector') IS NOT NULL 
	DROP TABLE [Perfmon].[CounterCollector]

IF OBJECT_ID('Perfmon.CounterCollectorProcesses') IS NOT NULL 
	DROP TABLE [Perfmon].[CounterCollectorProcesses]

IF OBJECT_ID('Perfmon.Counters') IS NOT NULL 
	DROP TABLE [Perfmon].[Counters]

IF OBJECT_ID('Perfmon.CountersType') IS NOT NULL 
	DROP TABLE [Perfmon].[CountersType]

IF OBJECT_ID('Perfmon.Conditions') IS NOT NULL 
	DROP TABLE [Perfmon].[Conditions]

IF OBJECT_ID('Perfmon.Operators') IS NOT NULL 
	DROP TABLE [Perfmon].[Operators]


CREATE TABLE [Perfmon].[CounterCollectorProcesses]
(
	Id BIGINT NOT NULL IDENTITY CONSTRAINT [PK_CounterCollectorProcesses] PRIMARY KEY CLUSTERED,
	StartDateTime DATETIME2 NOT NULL CONSTRAINT [DF_CounterCollectorProcesses_StartDateTime] DEFAULT (SYSDATETIME()) ,
	EndDateTime DATETIME2 NULL,
	DurationSec AS DATEDIFF(second,StartDateTime,EndDateTime)
)

CREATE TABLE [Perfmon].[CountersType]
(
	Id INT NOT NULL IDENTITY CONSTRAINT [PK_CountersType] PRIMARY KEY CLUSTERED,
	Origin NVARCHAR(4000)
)

SET IDENTITY_INSERT [Perfmon].[CountersType] ON
INSERT INTO [Perfmon].[CountersType] (Id,Origin) VALUES (1,N'sys.dm_os_performance_counters'),(2,'Perfmon')
SET IDENTITY_INSERT [Perfmon].[CountersType] OFF

CREATE TABLE [Perfmon].[Counters]
(
	Id BIGINT NOT NULL IDENTITY CONSTRAINT [PK_CounterList] PRIMARY KEY CLUSTERED,
	TypeId INT NOT NULL CONSTRAINT [FK_Counters_CountersType_TypeId_Id] FOREIGN KEY REFERENCES [Perfmon].[CountersType](Id),
	DisplayName NVARCHAR(4000) NOT NULL,
	CounterName NVARCHAR(4000) NOT NULL,
	InstanceName NVARCHAR(4000) NULL,
	ObjectName NVARCHAR(4000) NULL
)

SET IDENTITY_INSERT [Perfmon].[Counters] ON 
	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (1, 1,	N'Average Wait Time (ms)',	N'Average Wait Time (ms)',	N'_Total',		N'MSSQL$'+@@SERVICENAME+N':Locks')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (2, 1,	N'Buffer cache hit ratio',	N'Buffer cache hit ratio',	N'',			N'MSSQL$'+@@SERVICENAME+N':Buffer Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (3, 1,	N'SQL SERVER CPU usage %',	N'CPU usage %',				N'default',		N'MSSQL$'+@@SERVICENAME+N':Resource Pool Stats')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (4, 1,	N'Data File(s) Size (KB)',	N'Data File(s) Size (KB)',	N'_Total',		N'MSSQL$'+@@SERVICENAME+N':Databases')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (5, 1,	N'Free Memory (KB)',		N'Free Memory (KB)',		N'',			N'MSSQL$'+@@SERVICENAME+N':Memory Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (6, 1,	N'Log File(s) Size (KB)',	N'Log File(s) Size (KB)',	N'_Total',		N'MSSQL$'+@@SERVICENAME+N':Databases')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (7, 1,	N'Log Send Queue',			N'Log Send Queue',			N'_Total',		N'MSSQL$'+@@SERVICENAME+N':Database Replica')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (8, 1,	N'Memory Grants Pending',	N'Memory Grants Pending',	N'',			N'MSSQL$'+@@SERVICENAME+N':Memory Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (9, 1,	N'Open Connection Count',	N'Open Connection Count',	N'',			N'MSSQL$'+@@SERVICENAME+N':Broker/DBM Transport')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (10, 1,	N'Page life expectancy',	N'Page life expectancy',	N'',			N'MSSQL$'+@@SERVICENAME+N':Buffer Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (11, 1,	N'Processes blocked',		N'Processes blocked',		N'',			N'MSSQL$'+@@SERVICENAME+N':General Statistics')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (12, 1,	N'Queued requests',			N'Queued requests'	,		N'default',		N'MSSQL$'+@@SERVICENAME+N':Workload Group Stats')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [DisplayName], [CounterName], [InstanceName], [ObjectName]) VALUES (13, 1,	N'Transactions/sec',		N'Transactions/sec',		N'_Total',		N'MSSQL$'+@@SERVICENAME+N':Databases')
SET IDENTITY_INSERT [Perfmon].[Counters] OFF

SET IDENTITY_INSERT [Perfmon].[Counters] ON
INSERT INTO  [Perfmon].[Counters] (Id, TypeId, DisplayName, CounterName, InstanceName, ObjectName )
VALUES 
(14,2,N'CPU usage %',				N'% Processor Time',		N'_Total',		N'Processor'),
(15,2,N'Memory- Available MB',		N'Available MBytes',		N'',			N'Memory'),
(16,2,N'Memory- Pages Input/sec',	N'Pages Input/sec',			N'',			N'Memory'),
(17,2,N'Paging File Usage %',		N'% Usage',					N'_Total',		N'Paging File'),
(18,2,N'Paging File Usage Peak %',	N'% Usage Peak',			N'_Total',		N'Paging File')
SET IDENTITY_INSERT [Perfmon].[Counters] OFF


CREATE TABLE [Perfmon].[CounterCollector]
(
	Id BIGINT NOT NULL IDENTITY CONSTRAINT [PK_CounterCollector] PRIMARY KEY CLUSTERED,
	ProcesseId BIGINT NOT NULL CONSTRAINT [FK_CounterCollector_CounterCollectorProcesses_ProcesseId_Id] FOREIGN KEY REFERENCES [Perfmon].[CounterCollectorProcesses](Id),
	CounterId BIGINT NOT NULL CONSTRAINT [FK_CounterCollector_Counter_CounterId_Id] FOREIGN KEY REFERENCES [Perfmon].[Counters](Id),
	CounterValue DECIMAL(19,3) NOT NULL
)

GO 

CREATE TABLE [Perfmon].[Operators](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[definition] [nvarchar](1000) NOT NULL,
	[description] [nvarchar](1000) NOT NULL,
 CONSTRAINT [PK_Perfmon_Operators] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)
) ON [PRIMARY]

GO
SET IDENTITY_INSERT [Perfmon].[Operators] ON 

GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (1, N'= $#@[value]$#@', N'Equal')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (2, N'IN($#@[value]$#@)', N'IN')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (3, N'<> $#@[value]$#@', N'NotEqual')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (4, N'LIKE $#@[value]$#@', N'Like')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (5, N'> $#@[value]$#@', N'GreaterThan ')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (6, N'< $#@[value]$#@', N'LessThan ')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (7, N'>= $#@[value]$#@', N'GreaterThanOrEqual')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (8, N'<= $#@[value]$#@', N'LessThanOrEqual')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (9, N'NOT IN($#@[value]$#@)', N'NotIN')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (10, N'IS NOT NULL', N'NOTNULL')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (11, N'IS NULL', N'NULL')
GO
INSERT [Perfmon].[Operators] ([Id], [definition], [description]) VALUES (12, N'NOT LIKE $#@[value]$#@', N'NOT LIKE')
GO
SET IDENTITY_INSERT [Perfmon].[Operators] OFF
GO


CREATE TABLE [Perfmon].[Conditions](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[definition] [nvarchar](1000) NOT NULL,
	[description] [nvarchar](1000) NOT NULL,
 CONSTRAINT [PK_Perfmon_Conditions] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)) ON [PRIMARY]

GO
SET IDENTITY_INSERT [Perfmon].[Conditions] ON 

GO
INSERT [Perfmon].[Conditions] ([Id], [definition], [description]) VALUES (1, N'AVG($#@Expression$#@)', N'AVG')
GO
INSERT [Perfmon].[Conditions] ([Id], [definition], [description]) VALUES (2, N'MAX($#@Expression$#@)', N'MAX')
GO
INSERT [Perfmon].[Conditions] ([Id], [definition], [description]) VALUES (3, N'MIN($#@Expression$#@)', N'MIN')
GO
SET IDENTITY_INSERT [Perfmon].[Conditions] OFF
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
				SELECT		@ProcesseId,[Counters].[Id],T.[cntr_value]
				FROM		sys.dm_os_performance_counters  OPC
				JOIN		[Perfmon].[Counters] ON [Counters].[CounterName] = [OPC].[counter_name] AND [Counters].[InstanceName] = [OPC].[instance_name] AND [Counters].[ObjectName] = [OPC].[object_name] AND [Counters].[TypeId] = 1 
				LEFT JOIN	sys.dm_os_performance_counters  OP ON	OPC.object_name = OP.object_name AND OPC.instance_name = OP.instance_name AND OPC.cntr_type = 537003264 AND OP.cntr_type = 1073939712
																	AND ( RTRIM(OPC.counter_name) + N' Base' = OP.counter_name OR ( OPC.counter_name = N'Worktables From Cache Ratio' AND OP.counter_name = N'Worktables From Cache Base'))
				CROSS APPLY (SELECT cntr_value = CASE WHEN op.cntr_value IS NOT NULL THEN CAST (CAST (CAST (OPC.cntr_value  AS DECIMAL (38,5))/CAST (op.cntr_value AS DECIMAL (38,5)) AS DECIMAL (38,5)) * 100 AS DECIMAL (5,2)) ELSE opc.cntr_value END )T
				
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

IF EXISTS(SELECT TOP 1 1 FROM SYS.procedures WHERE  object_id = OBJECT_ID('Perfmon.usp_CounterCollectorOutCall'))
	DROP PROCEDURE [Perfmon].[usp_CounterCollectorOutCall]

IF EXISTS( SELECT * FROM sys.types WHERE name = 'CounterCollectorType' AND schema_id = SCHEMA_ID('Perfmon'))
DROP  TYPE [Perfmon].[CounterCollectorType]
GO 

CREATE TYPE [Perfmon].[CounterCollectorType] AS TABLE 
(
	[CounterId]		[bigint] NOT NULL,
	[CounterValue]	DECIMAL(19,3) NOT NULL
)
GO 

CREATE PROCEDURE [Perfmon].[usp_CounterCollectorOutCall]
	@Table [Perfmon].[CounterCollectorType] READONLY 
AS 
BEGIN 
		BEGIN TRY 

			BEGIN TRAN 

				DECLARE @ProcesseId BIGINT 
				INSERT INTO [Perfmon].[CounterCollectorProcesses] (StartDateTime) VALUES (DEFAULT)

				SELECT @ProcesseId = SCOPE_IDENTITY()

				INSERT INTO [Perfmon].[CounterCollector] ([ProcesseId],[CounterId],[CounterValue]) 
				SELECT @ProcesseId,[CounterId],[CounterValue] FROM @Table

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
		@owner_login_name=N'neworder\sqlservice', @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'StatisticsCollector', @server_name = @@SERVERNAME
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
DECLARE @owner_login_name NVARCHAR(1000)= 'neworder\sqlservice'
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
		@owner_login_name=@owner_login_name, 
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

