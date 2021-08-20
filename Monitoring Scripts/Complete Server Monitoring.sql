
--CREATE DATABASE Madeira_Monitoring
--GO
--USE Madeira_Monitoring
--GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Waits_WaitingTasks](
	[SampleDate] [datetime] NULL,
	[DBName] [nvarchar](128) NULL,
	[SessionID] [smallint] NULL,
	[WaitDuration_ms] [bigint] NULL,
	[WaitType] [nvarchar](60) NULL,
	[WaitResourceDescription] [nvarchar](2048) NULL,
	[ProgramName] [nvarchar](128) NULL,
	[StatementText] [nvarchar](max) NULL,
	[BatchText] [nvarchar](max) NULL,
	[BlockingSessionID] [smallint] NULL,
	[BlockingStatementText] [nvarchar](max) NULL,
	[BlockingBatchText] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Perfmon_PerformanceCounterValues]    Script Date: 01/27/2015 06:20:47 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Perfmon_PerformanceCounterValues](
	[SampleDate] [datetime] NULL,	
	[PageLifeExpectancy] [bigint] NULL,
	[MemoryGrantsPending] [smallint] NULL,
	[TotalServerMemoryGB] [decimal](5, 2) NULL,
	[TargetServerMemoryGB] [decimal](5, 2) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[IO_VirtualFileStats]    Script Date: 01/27/2015 06:20:47 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[IO_VirtualFileStats](
	[DatabaseName] [nvarchar](128) NULL,
	[SampleDate] [datetime] NOT NULL,
	[FileId] [smallint] NOT NULL,
	[FileName] [sysname] NOT NULL,
	[sample_ms] [bigint] NOT NULL,
	[num_of_reads] [bigint] NOT NULL,
	[num_of_bytes_read] [bigint] NOT NULL,
	[io_stall_read_ms] [bigint] NOT NULL,
	[num_of_writes] [bigint] NOT NULL,
	[num_of_bytes_written] [bigint] NOT NULL,
	[io_stall_write_ms] [bigint] NOT NULL,
	[io_stall] [bigint] NOT NULL,
	[size_on_disk_bytes] [bigint] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ExpensiveQueries_CurrentRunningQueries]    Script Date: 01/27/2015 06:20:47 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING OFF
GO
CREATE TABLE [dbo].[ExpensiveQueries_CurrentRunningQueries](
	[SampleDate] [datetime] NOT NULL,
	[StatementText] [nvarchar](max) NULL,
	[BatchText] [nvarchar](max) NULL,
	[session_id] [smallint] NOT NULL,
	[start_time] [datetime] NOT NULL,
	[status] [nvarchar](30) NOT NULL,
	[command] [nvarchar](16) NOT NULL,
	[blocking_session_id] [smallint] NULL,
	[wait_type] [nvarchar](60) NULL,
	[wait_time] [int] NOT NULL,
	[last_wait_type] [nvarchar](60) NOT NULL,
	[wait_resource] [nvarchar](256) NOT NULL,
	[cpu_time] [int] NOT NULL,
	[total_elapsed_time] [int] NOT NULL,
	[reads] [bigint] NOT NULL,
	[writes] [bigint] NOT NULL,
	[logical_reads] [bigint] NOT NULL,
	[deadlock_priority] [int] NOT NULL,
	[granted_query_memory] [int] NOT NULL,
	[query_hash] [binary](8) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET ANSI_PADDING OFF
GO
/****** Object:  StoredProcedure [dbo].[CollectPerformanceData_WaitStats]    Script Date: 01/27/2015 06:20:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[CollectPerformanceData_WaitStats] 
AS
SET NOCOUNT ON


INSERT INTO Waits_WaitingTasks
(SampleDate, DBName, SessionID, WaitDuration_ms, WaitType, WaitResourceDescription, ProgramName,
	StatementText, BatchText, BlockingSessionID, BlockingStatementText, BlockingBatchText)
	
SELECT
CAST(GETDATE() AS DATETIME) AS SampleDate, DB_NAME(R.database_id) DBName, WT.session_id, WT.wait_duration_ms, WT.wait_type, WT.resource_description,
    S.program_name, 
    SUBSTRING(ST.text, (R.statement_start_offset/2)+1, 
        ((Case R.statement_end_offset
              When -1 Then DATALENGTH(ST.text)
             Else R.statement_end_offset
         End - R.statement_start_offset)/2) + 1) AS StatementText,
    ST.text,    
    WT.blocking_session_id,    
    SUBSTRING(STBlocker.text, (RBlocker.statement_start_offset/2)+1, 
        ((Case RBlocker.statement_end_offset
              When -1 Then DATALENGTH(STBlocker.text)
             Else RBlocker.statement_end_offset
         End - RBlocker.statement_start_offset)/2) + 1) AS BlockingStatementText,
    STBlocker.text AS BlockerText
From sys.dm_os_waiting_tasks WT
Inner Join sys.dm_exec_sessions S on WT.session_id = S.session_id
Inner Join sys.dm_exec_requests R on R.session_id = WT.session_id
Outer Apply sys.dm_exec_sql_text(R.sql_handle) ST
Left Join sys.dm_exec_requests RBlocker on RBlocker.session_id = WT.blocking_session_id
Outer Apply sys.dm_exec_sql_text(RBlocker.sql_handle) STBlocker
Where R.status = 'suspended' 
And S.is_user_process = 1 -- Is a used process
And R.session_id <> @@spid 
AND WT.wait_type NOT IN
('CLR_SEMAPHORE','SQLTRACE_BUFFER_FLUSH','WAITFOR','REQUEST_FOR_DEADLOCK_SEARCH',
'XE_TIMER_EVENT','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_MANUAL_EVENT',
'CLR_AUTO_EVENT','FT_IFTS_SCHEDULER_IDLE_WAIT','XE_DISPATCHER_WAIT',
'XE_DISPATCHER_JOIN','BROKER_RECEIVE_WAITFOR')
And WT.wait_type Not Like '%sleep%' 
And WT.wait_type Not Like '%queue%' 
--AND WT.wait_type != 'CXPACKET'
And WT.wait_type Not Like 
    Case When SERVERPROPERTY('IsHadrEnabled') = 0 Then 'HADR%'
        Else 'zzzz' End
GO
/****** Object:  StoredProcedure [dbo].[CollectPerformanceData_PerformanceCounters]    Script Date: 01/27/2015 06:20:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[CollectPerformanceData_PerformanceCounters] 
AS
	SET NOCOUNT ON
	DECLARE @CMD NVARCHAR(4000)
	
	DECLARE @PerfmonValues TABLE(CounterName VARCHAR(200), CounterValue VARCHAR(200))
	DECLARE @Perfmon_PerformanceCounterValues TABLE
	
	(SampleDate DATETIME, BufferCacheHitRatio DECIMAL (5,2), PageLifeExpectancy BIGINT, 
	MemoryGrantsPending SMALLINT, TotalServerMemoryGB DECIMAL(5,2),TargetServerMemoryGB DECIMAL(5,2))
		
	INSERT INTO @PerfmonValues (CounterValue,CounterName)
	SELECT CONVERT(VARCHAR(200), GETDATE(), 109), 'SampleDate'	
	UNION ALL
	SELECT CAST(cntr_value AS VARCHAR(200)), 'PageLifeExpectancy'
	FROM sys.dm_os_performance_counters WITH (NOLOCK)
	WHERE [object_name] = N'SQLServer:Buffer Manager'
	AND counter_name = N'Page life expectancy'
	UNION ALL
	SELECT CAST(cntr_value AS VARCHAR(200)), 'MemoryGrantsPending'                                                                                                    
	FROM sys.dm_os_performance_counters WITH (NOLOCK)
	WHERE [object_name] LIKE N'%Memory Manager%' 
	AND counter_name = N'Memory Grants Pending' 
	UNION ALL
	SELECT CAST(cntr_value/1024/1024 AS VARCHAR(200)) , 'TotalServerMemoryGB'
	FROM sys.dm_os_performance_counters 
	WHERE counter_name = 'Total Server Memory (KB)'
	UNION ALL
	SELECT CAST(cntr_value/1024/1024 AS VARCHAR(200)), 'TargetServerMemoryGB' 
	FROM sys.dm_os_performance_counters 
	WHERE counter_name = 'Target Server Memory (KB)'
	
	
	
	INSERT @Perfmon_PerformanceCounterValues (SampleDate)
	SELECT CAST(CounterValue AS DATETIME)
		FROM @PerfmonValues
		WHERE CounterName = 'SampleDate'
		
	
	UPDATE @Perfmon_PerformanceCounterValues
	SET BufferCacheHitRatio = 
		(SELECT CounterValue
		FROM @PerfmonValues
		WHERE CounterName = 'BufferCacheHitRatio'
		)
		
	UPDATE @Perfmon_PerformanceCounterValues
	SET PageLifeExpectancy = 
		(SELECT CounterValue
		FROM @PerfmonValues
		WHERE CounterName = 'PageLifeExpectancy'
		)
		
	UPDATE @Perfmon_PerformanceCounterValues
	SET MemoryGrantsPending = 
		(SELECT CounterValue
		FROM @PerfmonValues
		WHERE CounterName = 'MemoryGrantsPending'
		)
		
	UPDATE @Perfmon_PerformanceCounterValues
	SET TotalServerMemoryGB = 
		(SELECT CounterValue
		FROM @PerfmonValues
		WHERE CounterName = 'TotalServerMemoryGB'
		)
		
	UPDATE @Perfmon_PerformanceCounterValues
	SET TargetServerMemoryGB = 
		(SELECT CounterValue
		FROM @PerfmonValues
		WHERE CounterName = 'TargetServerMemoryGB'
		)
	
	INSERT INTO Perfmon_PerformanceCounterValues
	(SampleDate, PageLifeExpectancy, MemoryGrantsPending, TotalServerMemoryGB, TargetServerMemoryGB)
	SELECT SampleDate, PageLifeExpectancy, MemoryGrantsPending, TotalServerMemoryGB, TargetServerMemoryGB 	
	FROM @Perfmon_PerformanceCounterValues
GO
/****** Object:  StoredProcedure [dbo].[CollectPerformanceData_IO]    Script Date: 01/27/2015 06:20:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[CollectPerformanceData_IO] 
AS
	SET NOCOUNT ON
	DECLARE @CMD NVARCHAR(4000)
	
	INSERT IO_VirtualFileStats (DatabaseName, SampleDate, FileId, FileName, sample_ms, num_of_reads, num_of_bytes_read, 
								io_stall_read_ms, num_of_writes, num_of_bytes_written, io_stall_write_ms, io_stall, size_on_disk_bytes)	
	SELECT
		DB_NAME(s.database_id) AS DatabaseName,
		GETDATE() SampleDate,
		s.file_id,
		m.name AS FileName,
		s.sample_ms,
		s.num_of_reads,
		s.num_of_bytes_read,
		s.io_stall_read_ms,
		s.num_of_writes,
		s.num_of_bytes_written,
		s.io_stall_write_ms,
		s.io_stall,
		s.size_on_disk_bytes		 	
	FROM sys.dm_io_virtual_file_stats(null,null) s
	INNER JOIN sys.master_Files m
	ON s.database_id = m.database_id AND s.file_id = m.file_id
GO
/****** Object:  StoredProcedure [dbo].[CollectPerformanceData_ExpensiveQueries]    Script Date: 01/27/2015 06:20:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[CollectPerformanceData_ExpensiveQueries] 
AS
	SET NOCOUNT ON
		
	INSERT ExpensiveQueries_CurrentRunningQueries
	(
		SampleDate,StatementText,BatchText,
		session_id,start_time,status,command,blocking_session_id,
		wait_type,wait_time,last_wait_type,wait_resource,cpu_time,
		total_elapsed_time,reads,writes,logical_reads,
		deadlock_priority,granted_query_memory,query_hash 
	)
	
	SELECT
	GETDATE() AS SampleDate, 
	SUBSTRING(text, statement_start_offset/2+1, 
			 ((CASE WHEN statement_end_offset = -1 THEN DATALENGTH(text) 
			 ELSE statement_end_offset 
			 END - statement_start_offset)/2) + 1) AS StatementText,
			 text AS BatchText,
	session_id,start_time,status,command,blocking_session_id,
	wait_type,wait_time,last_wait_type,wait_resource,cpu_time,
	total_elapsed_time,reads,writes,logical_reads,
	deadlock_priority,granted_query_memory,query_hash	
	FROM sys.dm_exec_requests r
	cross apply sys.dm_exec_sql_text(sql_handle) t
	--outer apply sys.dm_exec_text_query_plan(plan_handle, statement_start_offset, statement_end_offset) p
	where session_id <> @@spid
GO
/****** Object:  StoredProcedure [dbo].[CollectPerformanceData]    Script Date: 01/27/2015 06:20:49 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[CollectPerformanceData]
AS
SET NOCOUNT ON
DECLARE @CMD NVARCHAR(4000)


--IO Metrics
EXEC CollectPerformanceData_IO 
--Wait Stats Metrics
EXEC CollectPerformanceData_WaitStats 
--Expensive Queries
EXEC CollectPerformanceData_ExpensiveQueries 
--Performance Counters
EXEC CollectPerformanceData_PerformanceCounters
GO
