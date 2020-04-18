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