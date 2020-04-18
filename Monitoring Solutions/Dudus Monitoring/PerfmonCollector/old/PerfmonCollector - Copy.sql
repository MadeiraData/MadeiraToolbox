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
INSERT INTO [Perfmon].[CountersType] (Id,Origin) VALUES (1,N'sys.dm_os_performance_counters')
SET IDENTITY_INSERT [Perfmon].[CountersType] OFF

CREATE TABLE [Perfmon].[Counters]
(
	Id BIGINT NOT NULL IDENTITY CONSTRAINT [PK_CounterList] PRIMARY KEY CLUSTERED,
	TypeId INT NOT NULL CONSTRAINT [FK_Counters_CountersType_TypeId_Id] FOREIGN KEY REFERENCES [Perfmon].[CountersType](Id),
	CounterName NVARCHAR(4000) NOT NULL,
	InstanceName NVARCHAR(4000) NULL,
	ObjectName NVARCHAR(4000) NULL
)

SET IDENTITY_INSERT [Perfmon].[Counters] ON 
	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (1, 1, N'Average Wait Time (ms)', N'_Total', N'MSSQL$INSTANCE1:Locks')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (2, 1, N'Buffer cache hit ratio', N'', N'MSSQL$INSTANCE1:Buffer Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (3, 1, N'CPU usage %', N'default', N'MSSQL$INSTANCE1:Workload Group Stats')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (4, 1, N'Data File(s) Size (KB)', N'_Total', N'MSSQL$INSTANCE1:Databases')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (5, 1, N'Free Memory (KB)', N'', N'MSSQL$INSTANCE1:Memory Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (6, 1, N'Log File(s) Size (KB)', N'_Total', N'MSSQL$INSTANCE1:Databases')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (7, 1, N'Log Send Queue', N'_Total', N'MSSQL$INSTANCE1:Database Replica')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (8, 1, N'Memory Grants Pending', N'', N'MSSQL$INSTANCE1:Memory Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (9, 1, N'Open Connection Count', N'', N'MSSQL$INSTANCE1:Broker/DBM Transport')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (10, 1, N'Page life expectancy', N'', N'MSSQL$INSTANCE1:Buffer Manager')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (11, 1, N'Processes blocked', N'', N'MSSQL$INSTANCE1:General Statistics')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (12, 1, N'Queued requests', N'default', N'MSSQL$INSTANCE1:Workload Group Stats')

	INSERT [Perfmon].[Counters] ([Id], [TypeId], [CounterName], [InstanceName], [ObjectName]) VALUES (13, 1, N'Transactions/sec', N'_Total', N'MSSQL$INSTANCE1:Databases')
SET IDENTITY_INSERT [Perfmon].[Counters] OFF


CREATE TABLE [Perfmon].[CounterCollector]
(
	Id BIGINT NOT NULL IDENTITY CONSTRAINT [PK_CounterCollector] PRIMARY KEY CLUSTERED,
	ProcesseId BIGINT NOT NULL CONSTRAINT [FK_CounterCollector_CounterCollectorProcesses_ProcesseId_Id] FOREIGN KEY REFERENCES [Perfmon].[CounterCollectorProcesses](Id),
	CounterId BIGINT NOT NULL CONSTRAINT [FK_CounterCollector_Counter_CounterId_Id] FOREIGN KEY REFERENCES [Perfmon].[Counters](Id),
	CounterValue BIGINT NOT NULL
)

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

---------------------------------------------
-----------Generate report script------------
---------------------------------------------
USE DB_DBA
GO 


CREATE PROCEDURE [Perfmon].[usp_SendReport]
AS 
BEGIN 
		DECLARE @html NVARCHAR(MAX)

		WITH T AS (
		SELECT 
			DatesRangeId = DatesRange.Id,
			DatesRange = DatesRange.Name,
			[CounterCollector].[CounterId],
			A.[CounterFullName],
			MaxCounterValue = CAST(MAX([CounterValue]) AS DECIMAL(23,2)),
			MinCounterValue = CAST(MIN([CounterValue]) AS DECIMAL(23,2)),
			AVGCounterValue = CAST(AVG([CounterValue]) AS DECIMAL(23,2))
		FROM 
		[Perfmon].[CounterCollector]
		JOIN [Perfmon].[CounterCollectorProcesses] ON [CounterCollectorProcesses].[Id] = [CounterCollector].[ProcesseId]
		JOIN [Perfmon].[Counters] ON [Counters].[Id] =  [CounterCollector].[CounterId]
		JOIN	(
					VALUES	(1,N'Last 24 Hours',DATEADD(DAY,-1,SYSDATETIME()),SYSDATETIME()),
							(2,N'One Week Ago',DATEADD(DAY,-8,SYSDATETIME()),DATEADD(DAY,-7,SYSDATETIME())),
							(3,N'One Month Ago',DATEADD(DAY,-29,SYSDATETIME()),DATEADD(DAY,-28,SYSDATETIME())),
							(4,N'One Year Ago',DATEADD(DAY,-365,SYSDATETIME()),DATEADD(DAY,-364,SYSDATETIME()))
				)DatesRange(Id,Name,StartDateTime,EndDateTime) ON [CounterCollectorProcesses].StartDateTime >= DatesRange.StartDateTime AND [CounterCollectorProcesses].StartDateTime <= DatesRange.EndDateTime
		CROSS APPLY (SELECT [CounterFullName] = [Counters].[CounterName]+CASE WHEN [Counters].[InstanceName] = N'' OR [Counters].[InstanceName] IS NULL THEN N'' ELSE N'('+[Counters].[InstanceName]+N')' END) A
		GROUP BY [CounterCollector].[CounterId],A.[CounterFullName],DatesRange.Name,DatesRange.Id) 

		SELECT @html = CONVERT (VARCHAR(MAX),(
		SELECT 
			'td'=	a,'td'=	b,'td'=	c,'td'=	d,'td'=	e,'td'=	f,'td'=	g,'td'=	h,
			'td'=	i,'td'=	j,'td'=	k,'td'=	l,'td'=	m,'td'=	n
		FROM (
			SELECT 
				a=	CAST('<b>Counter Id</b>'					AS XML) , 
				b=	CAST('<b>Counter Name</b>'					AS XML) , 
				c=	CAST('<b>AVG Counter Value</b>'				AS XML) , 
				d=	CAST('<b>Max Counter Value</b>'				AS XML) , 
				e=	CAST('<b>Min Counter Value</b>'				AS XML) , 
				f=	CAST('<b>AVG Week Ago</b>'					AS XML) , 
				g=	CAST('<b>AVG Month Ago</b>'					AS XML) , 
				h=	CAST('<b>AVG Year Ago</b>'					AS XML) , 
				i=	CAST('<b>MAX Week Ago</b>'					AS XML) , 
				j=	CAST('<b>MAX Month Ago</b>'					AS XML) , 
				k=	CAST('<b>MAX Year Ago</b>'					AS XML) , 
				l=	CAST('<b>MIN Week Ago</b>'					AS XML) , 
				m=	CAST('<b>MIN Month Ago</b>'					AS XML) , 
				n=	CAST('<b>MIN Year Ago</b>'					AS XML) 
			UNION ALL 
			SELECT 
				ISNULL(CAST(Last24Hours.CounterId AS NVARCHAR(MAX))				,''),		--[Counter Id]
				ISNULL(CAST(Last24Hours.CounterFullName AS NVARCHAR(MAX))		,''),		--[Counter Name]
				ISNULL(CAST(Last24Hours.AVGCounterValue AS NVARCHAR(MAX))		,''),		--[AVG Counter Value]
				ISNULL(CAST(Last24Hours.MaxCounterValue AS NVARCHAR(MAX))		,''),		--[Max Counter Value]
				ISNULL(CAST(Last24Hours.MinCounterValue AS NVARCHAR(MAX))		,''),		--[Min Counter Value]
				ISNULL(CAST(Change.[AVGComparedToAWeekAgo]	 AS NVARCHAR(MAX))	,''),
				ISNULL(CAST(Change.[AVGComparedToAMonthAgo] AS NVARCHAR(MAX))	,''),	
				ISNULL(CAST(Change.[AVGComparedToAYearAgo]	 AS NVARCHAR(MAX))	,''),
				ISNULL(CAST(Change.[MAXComparedToAWeekAgo]	 AS NVARCHAR(MAX))	,''),
				ISNULL(CAST(Change.[MAXComparedToAMonthAgo] AS NVARCHAR(MAX))	,''),	
				ISNULL(CAST(Change.[MAXComparedToAYearAgo]	 AS NVARCHAR(MAX))	,''),
				ISNULL(CAST(Change.[MINComparedToAWeekAgo]	 AS NVARCHAR(MAX))	,''),
				ISNULL(CAST(Change.[MINComparedToAMonthAgo] AS NVARCHAR(MAX))	,''),	
				ISNULL(CAST(Change.[MINComparedToAYearAgo]	 AS NVARCHAR(MAX)) 	,'')

			FROM T AS Last24Hours
			LEFT JOIN T AS OneWeekAgo ON Last24Hours.CounterId = OneWeekAgo.CounterId AND OneWeekAgo.DatesRangeId = 2 
			LEFT JOIN T AS OneMonthAgo ON Last24Hours.CounterId = OneMonthAgo.CounterId AND OneMonthAgo.DatesRangeId = 3
			LEFT JOIN T AS OneYearAgo ON Last24Hours.CounterId = OneYearAgo.CounterId AND OneYearAgo.DatesRangeId = 4
			CROSS APPLY (SELECT 
							[AVGComparedToAWeekAgo]	= FORMAT(CASE WHEN OneWeekAgo.AVGCounterValue = 0 THEN NULL ELSE  (Last24Hours.AVGCounterValue-OneWeekAgo.AVGCounterValue)/OneWeekAgo.AVGCounterValue/100 END ,'p'),
							[AVGComparedToAMonthAgo]	= FORMAT(CASE WHEN OneMonthAgo.AVGCounterValue = 0 THEN NULL ELSE  (Last24Hours.AVGCounterValue-OneMonthAgo.AVGCounterValue)/OneMonthAgo.AVGCounterValue/100 END ,'p'),
							[AVGComparedToAYearAgo]	= FORMAT(CASE WHEN OneYearAgo.AVGCounterValue = 0 THEN NULL ELSE  (Last24Hours.AVGCounterValue-OneYearAgo.AVGCounterValue)/OneYearAgo.AVGCounterValue/100 END ,'p'),
								  
							[MAXComparedToAWeekAgo]	= FORMAT(CASE WHEN OneWeekAgo.MaxCounterValue = 0 THEN NULL ELSE  (Last24Hours.MaxCounterValue-OneWeekAgo.MaxCounterValue)/OneWeekAgo.MaxCounterValue/100 END ,'p'),
							[MAXComparedToAMonthAgo]	= FORMAT(CASE WHEN OneMonthAgo.MaxCounterValue = 0 THEN NULL ELSE  (Last24Hours.MaxCounterValue-OneMonthAgo.MaxCounterValue)/OneMonthAgo.MaxCounterValue/100 END ,'p'),
							[MAXComparedToAYearAgo]	= FORMAT(CASE WHEN OneYearAgo.MaxCounterValue = 0 THEN NULL ELSE  (Last24Hours.MaxCounterValue-OneYearAgo.MaxCounterValue)/OneYearAgo.MaxCounterValue/100 END ,'p'),
								  
							[MINComparedToAWeekAgo]	= FORMAT(CASE WHEN OneWeekAgo.MinCounterValue = 0 THEN NULL ELSE  (Last24Hours.MinCounterValue-OneWeekAgo.MinCounterValue)/OneWeekAgo.MinCounterValue/100 END ,'p'),
							[MINComparedToAMonthAgo]	= FORMAT(CASE WHEN OneMonthAgo.MinCounterValue = 0 THEN NULL ELSE  (Last24Hours.MinCounterValue-OneMonthAgo.MinCounterValue)/OneMonthAgo.MinCounterValue/100 END,'p'),
							[MINComparedToAYearAgo]	= FORMAT(CASE WHEN OneYearAgo.MinCounterValue = 0 THEN NULL ELSE  (Last24Hours.MinCounterValue-OneYearAgo.MinCounterValue)/OneYearAgo.MinCounterValue/100 END,'p')
						)Change 
			WHERE Last24Hours.DatesRangeId = 1 ) a 
		FOR XML RAW ('tr'),TYPE ,ELEMENTS ) ,1)


		SET @html = 
		'<html><header></header><body><table align="center" border=1 width="100%">'+@html+'</table></body></html>'
		
		select @html
		EXEC msdb.dbo.sp_send_dbmail 
			@body = @html
			, @recipients = 'dudu@madeira.co.il'
			, @subject = 'Performance Report'
			, @profile_name = 'sales.neworder gmail'
			, @body_format = 'HTML';
END 