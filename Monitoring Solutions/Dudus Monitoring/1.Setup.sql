IF DB_ID('DB_DBA') IS NULL
	CREATE DATABASE DB_DBA
GO 	

USE [DB_DBA]
GO 

IF SCHEMA_ID('Report') IS NULL 
	BEGIN 
		DECLARE @Command NVARCHAR(MAX) = N'CREATE SCHEMA [Report]';
		EXEC (@Command);
	END;

GO

IF OBJECT_ID('Report.SentAlerts') IS NOT NULL 
	DROP TABLE [Report].[SentAlerts]

IF OBJECT_ID('Report.Configuration') IS NOT NULL 
	DROP TABLE [Report].[Configuration]

IF OBJECT_ID('Report.MonitoringAlerts') IS NOT NULL 
	DROP TABLE [Report].[MonitoringAlerts]

------------------------------[Report].[Configuration]------------------------------


IF OBJECT_ID('Report.Configuration') IS NOT NULL 
	DROP TABLE [Report].[Configuration]

CREATE TABLE [Report].[Configuration]
(
	[Id]	INT IDENTITY NOT NULL,
	[Key]	NVARCHAR(1000) NOT NULL,
	[Value] NVARCHAR(1000) NOT NULL,

	CONSTRAINT [PK_Report_Configuration] PRIMARY KEY CLUSTERED 
	(
		[Id] ASC
	)
)

SET IDENTITY_INSERT [Report].[Configuration] ON 
GO
INSERT INTO [Report].[Configuration]  ([Id],[Key],[Value])
	VALUES
	(1,N'Daily Report Time',N'08:00:00'),
	(2,N'Continuous Alerts Send Interval (MINUTE)',N'10'),
	(3,N'Email Recipients',N'dudu@madeira.co.il;dudusinai@gmail.com'),
	(4,N'Send Profile Name',N'TestProfile'),
	(5,N'Daily Report Subject',N'New Order''s Daily Report'),
	(6,N'Madeira Company Logo',N'http://www.madeirasql.com/wp-content/uploads/copy-HomePage_new.jpg'),
	(7,N'Client Company Logo',N'http://www.neworder.co.il/Thumbnail.ashx?image=images/pages_images/44feaad1-ca84-42bd-847a-d3ec0337ad19.jpg&w=1000&h=1000&cp=1'),
	(8,N'Error Report Subject',N'SQL SERVER - Monitoring Alert')


SET IDENTITY_INSERT [Report].[Configuration] OFF 
------------------------------------------------------------------------------------

------------------------------[Report].[MonitoringAlerts]------------------------------

IF OBJECT_ID('Report.MonitoringAlerts') IS NOT NULL 
	DROP TABLE [Report].[MonitoringAlerts]

CREATE TABLE [Report].[MonitoringAlerts](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Name] [nvarchar](1000) NOT NULL,
	[ExecutionCommand] [nvarchar](max) NOT NULL,
	[Enabled] [bit] NOT NULL,
	[EvaluationIntervalMin] INT NOT NULL,
	[EnableContinuesMonitoring] BIT NOT NULL,
	[ContinuousAlertsSendIntervalMin] INT NOT NULL,
 CONSTRAINT [PK_Report_MonitoringAlerts] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
SET IDENTITY_INSERT [Report].[MonitoringAlerts] ON 

GO
INSERT [Report].[MonitoringAlerts] ([Id], [Name], [ExecutionCommand], [Enabled],[EvaluationIntervalMin],[EnableContinuesMonitoring]) VALUES (1, N'Blocked Requests', N'
EXEC [Report].[usp_DBBlockedRequestsReport]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT
', 1, 1, 1,0)
,
(2, N'DataBase Abnormal State', N'
EXEC [Report].[usp_DBStateReport]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT
', 1, 1, 1,10)
,
(3, N'Low Disk Space', N'
EXEC [Report].[usp_LowDiskSpace]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT,
	@Threshold = 30  -- free percentage
', 1, 1, 0,10)
,
(4, N'Low File Space', N'
EXEC [Report].[usp_LowFileSpace]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT,
	@FreeSpacePercentThreshold = 20  -- free percentage
', 1, 1, 0, 10)
,
(5, N'Identity Over flow', N'
EXEC [Report].[usp_CheckIdentityOverflow]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT,
	@MaxPercent = 85  
', 1, 1, 0, 10)
,
(6, N'Failed Jobs', N'
EXEC [Report].[usp_FailedJobs]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT
', 1, 1, 0, 10)
,
(7, N'Mirroring Report', N'
EXEC [Report].[usp_GenerateMirroringReport]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT
', 1, 1, 1, 10)
,
(8, N'Non Working Services', N'
EXEC [Report].[usp_NonWorkingServices]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT
', 1, 1, 1, 10)
,
(9, N'Performance Counters Report', N'
EXEC [Report].[usp_SendPerfmonCollectorReport]
	@DebugMode = 0,
	@HTMLTable = @HTMLTable OUTPUT
', 0, 1, 0, 10)
,
(10, N'High CPU usage', N'
EXEC  [Perfmon].[usp_AlertEvaluation]
	@HTMLTable =@HTMLTable OUTPUT,
	@LastNMinutes = 10,	--Last N minutes to test
	@CounterId = 14 ,		
	@Threshold = 80, 
	@ConditionId = 1, 
	@OperatorId =5,
	@Header =''High CPU usage in the last 10 minutes''
', 1, 1, 1, 10)
,
(11, N'Availability Group Failover Event', N'
EXEC	[Report].[usp_CheckForFailoverEvents]
		@HTMLTable = @HTMLTable OUTPUT'
, 1, 1, 1, 0)
,
(12, N'Availability Group Health Events', N'
EXEC	[Report].[usp_AvailabilityGroupHealthIssues]
		@HTMLTable = @HTMLTable OUTPUT'
, 1, 1, 1, 10)
GO
SET IDENTITY_INSERT [Report].[MonitoringAlerts] OFF
GO
---------------------------------------------------------------------------------------
------------------------------[Report].[SentAlerts]------------------------------


CREATE TABLE [Report].[SentAlerts]
(
	[Id]	INT IDENTITY NOT NULL,
	[MonitoringAlertsId]	INT NOT NULL CONSTRAINT [FK_Report_SentAlerts_MonitoringAlerts_Id] FOREIGN KEY REFERENCES [Report].[MonitoringAlerts]([Id]),
	[Description] NVARCHAR(MAX) NOT NULL,
	[EventDateTime] DATETIME2 NOT NULL CONSTRAINT [DF_Report_SentAlerts_EventDateTime] DEFAULT SYSDATETIME(),
	CONSTRAINT [PK_Report_SentAlerts] PRIMARY KEY CLUSTERED 
	(
		[Id] ASC
	)
)

------------------------------------------------------------------------------------