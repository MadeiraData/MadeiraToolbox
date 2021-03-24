/*========================================================================================================================
Description:	Create a Job which collect unused indexes. a Table which store the collected indexes along with the DROP index script
				and Index creation and send email (By a SP) that such an indexes was found. Second Job drops once in a day the one unused index. 
				All the automation process is record and managed from the table "UnusedIndexes".
Written By:		Guy Yaakobovitch, Madeira Data Solutions
Created:		23/03/2021
Last Updated:	00/00/0000
Notes:			WARNING! Removing unused indexes although they found as unused is a risky action that should be monitored 
				and tested in a test environment first. You should know what are the risks before implementing that solution.
				It is recommended for use in a non-critical and non-front-end environment where no DBA exists to manage it
				and the system can take the risk.

				You should go over and replace the variables values for your environment like: Job scheduling, Job owner, send an email details,
				DB to create the objects etc.
=========================================================================================================================*/

USE [DBA]
GO

/****** Object:  Table [dbo].[UnusedIndexes]    Script Date: 3/3/2021 2:33:13 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[UnusedIndexes]') AND type in (N'U'))
DROP TABLE [dbo].[UnusedIndexes]
GO

/****** Object:  Table [dbo].[UnusedIndexes]    Script Date: 3/3/2021 2:33:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[UnusedIndexes](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[SampleDate] [datetime2](7) NULL,
	[DBName] [sysname] NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[TableName] [sysname] NOT NULL,
	[IndexName] [sysname] NULL,
	[RowsCount] [int] NULL,
	[IndexSizeKB] [int] NULL,
	[UpdatesCount] [int] NULL,
	[DropCMD] [nvarchar](max) NULL,
	[TableCreatedDate] [datetime] NULL,
	[LastStatsDate] [datetime] NULL,
	[CreateCMD] [nvarchar](max) NULL,
	[IsDeployed] [bit] NULL,
	[DeploymetDateTime] [datetime2](7) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


USE [DBA]
GO

/****** Object:  StoredProcedure [dbo].[DropUnusedIndexes]    Script Date: 3/3/2021 2:33:41 PM ******/
--DROP PROCEDURE [dbo].[DropUnusedIndexes]
--GO

/****** Object:  StoredProcedure [dbo].[DropUnusedIndexes]    Script Date: 3/3/2021 2:33:41 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[DropUnusedIndexes]
AS
BEGIN
IF EXISTS (SELECT 1 FROM [DBA].[dbo].[UnusedIndexes] WHERE IsDeployed = 0 AND [DeploymetDateTime] IS NULL)
BEGIN
	DECLARE @DROPCMD NVARCHAR(MAX), @ID INT;
	SELECT TOP 1 @ID = ID, @DROPCMD = DropCMD FROM  [dbo].[UnusedIndexes] 
	WHERE IsDeployed = 0 AND [DeploymetDateTime] IS NULL ORDER BY SampleDate;
	--SELECT @DROPCMD;
	BEGIN TRAN
		EXEC(@DROPCMD)
		UPDATE [dbo].[UnusedIndexes] 
		SET [DeploymetDateTime] = GETDATE(), IsDeployed = 1
		WHERE ID = @ID
	COMMIT
END
END
GO

USE [DBA]
GO

/****** Object:  StoredProcedure [dbo].[usp_Capture_Unused_Indexes]    Script Date: 3/3/2021 2:34:10 PM ******/
DROP PROCEDURE [dbo].[usp_Capture_Unused_Indexes]
GO

/****** Object:  StoredProcedure [dbo].[usp_Capture_Unused_Indexes]    Script Date: 3/3/2021 2:34:10 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROC [dbo].[usp_Capture_Unused_Indexes]
AS
BEGIN

/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Description: Use this script to retrieve all unused indexes across all of your databases.
The data returned includes various index usage statistics and a corresponding drop command.
Supports both on-premise instances, as well as Azure SQL Databases.
*/
SET NOCOUNT ON;
DECLARE @Executor SYSNAME
DECLARE @CurrDB SYSNAME
DECLARE @SampleDate DATETIME2;
SET @SampleDate = GETDATE();
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @CMD NVARCHAR(MAX);
SET @CMD = N'

	PRINT DB_NAME();
 SELECT
	 db_name() AS DBNAme,
	 OBJECT_SCHEMA_NAME(indexes.object_id) as SchemaName,
	 OBJECT_NAME(indexes.object_id) AS Table_name,
	 indexes.name AS Index_name,
	 SUM(partitions.rows) AS RowsCount,
	 SUM(partition_stats.reserved_page_count) * 8 AS IndexSizeKB,
	 ''USE '' + QUOTENAME(DB_NAME()) + N''; DROP INDEX ''+QUOTENAME(indexes.name)+'' ON ''+QUOTENAME(db_name())+''.''+ QUOTENAME(OBJECT_SCHEMA_NAME(indexes.object_id))+''.''+QUOTENAME(OBJECT_NAME(indexes.object_id)) as dropcmd ,
	 STATS_DATE(indexes.object_id, indexes.index_id) StatsDate,
	 tables.create_date AS TableCreatedDate,
	 ISNULL(usage_stats.user_updates, 0) + ISNULL(usage_stats.system_updates, 0) AS UpdatesCount,
 CASE indexes.index_id WHEN 0 THEN N''/* No create statement (Heap) */''
 ELSE 
        CASE is_primary_key 
		WHEN 1 THEN
            N'' ALTER TABLE '' + QUOTENAME(sc.name) + N''.'' + QUOTENAME(tables.name) + N'' ADD CONSTRAINT '' + QUOTENAME(indexes.name) + N'' PRIMARY KEY '' +
                CASE WHEN indexes.index_id > 1 THEN N''NON'' ELSE N'''' END + N''CLUSTERED ''
            ELSE N'' USE '' + QUOTENAME(DB_NAME()) + N''; CREATE '' + 
                CASE WHEN indexes.is_unique = 1 then N''UNIQUE '' ELSE N'''' END +
                CASE WHEN indexes.index_id > 1 THEN N''NON'' ELSE N'''' END + N''CLUSTERED '' +
                N''INDEX '' + QUOTENAME(indexes.name) + N'' ON '' + QUOTENAME(sc.name) + N''.'' + QUOTENAME(tables.name) + N'' ''
        END +

		/* key def */ N''('' + key_definition + N'')'' +
        /* includes */ CASE WHEN include_definition IS NOT NULL THEN 
            N'' INCLUDE ('' + include_definition + N'')''
            ELSE N''''

        END +

		/* filters */ CASE WHEN indexes.filter_definition IS NOT NULL THEN 
            N'' WHERE '' + indexes.filter_definition ELSE N''''
        END +

		/* with clause - compression goes here */
        CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
            THEN N'' WITH ('' +
                CASE WHEN row_compression_partition_list IS NOT NULL THEN
                    N''DATA_COMPRESSION = ROW '' + CASE WHEN psc.name IS NULL THEN N'''' ELSE + N'' ON PARTITIONS ('' + row_compression_partition_list + N'')'' END
                ELSE N'''' END +
                CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL THEN N'', '' ELSE N'''' END +
                CASE WHEN page_compression_partition_list IS NOT NULL THEN
                    N''DATA_COMPRESSION = PAGE '' + CASE WHEN psc.name IS NULL THEN N'''' ELSE + N'' ON PARTITIONS ('' + page_compression_partition_list + N'')'' END
                ELSE N'''' END
            + N'')''
            ELSE N''''
        END +

		 /* ON where? filegroup? partition scheme? */
        '' ON '' + CASE WHEN psc.name is null 
            THEN ISNULL(QUOTENAME(fg.name),N'''')
            ELSE psc.name + N'' ('' + partitioning_column.column_name + N'')'' 
            END
		+ N'';''

 END AS CreateCMD
 FROM
	sys.indexes
 INNER JOIN 
	sys.tables
 ON 
	indexes.object_id = tables.object_id 
		AND tables.create_date < DATEADD(dd, -30, GETDATE())
		AND tables.is_ms_shipped = 0
		AND indexes.index_id > 1
		AND indexes.is_primary_key = 0
		AND indexes.is_unique = 0
		AND indexes.is_disabled = 0
		AND indexes.is_hypothetical = 0
 JOIN	
	sys.schemas AS sc 
 ON 
	tables.schema_id = sc.schema_id
 INNER JOIN 
	sys.partitions
 ON 
	indexes.object_id = partitions.object_id
	AND 
	indexes.index_id = partitions.index_id
 LEFT JOIN 
	sys.dm_db_index_usage_stats AS usage_stats
 ON 
	indexes.index_id = usage_stats.index_id AND usage_stats.OBJECT_ID = indexes.OBJECT_ID
 LEFT JOIN 
	sys.dm_db_partition_stats AS partition_stats
 ON 
	indexes.index_id = partition_stats.index_id AND partition_stats.OBJECT_ID = indexes.OBJECT_ID
 LEFT JOIN 
	sys.partition_schemes AS psc 
 ON 
		indexes.data_space_id = psc.data_space_id
 LEFT JOIN 
	sys.filegroups AS fg 
 ON indexes.data_space_id = fg.data_space_id

 /* Key list */ 
 OUTER APPLY ( SELECT STUFF (
    (SELECT N'', '' + QUOTENAME(c.name) +
        CASE ic.is_descending_key WHEN 1 then N'' DESC'' ELSE N'''' END
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = indexes.object_id
        and ic.index_id= indexes.index_id
        and ic.key_ordinal > 0
    ORDER BY ic.key_ordinal FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS keys ( key_definition )

 /* Include list */ OUTER APPLY ( SELECT STUFF (
    (SELECT N'', '' + QUOTENAME(c.name)
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = indexes.object_id
        and ic.index_id= indexes.index_id
        and ic.is_included_column = 1
    ORDER BY c.name FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS includes ( include_definition )

/* row compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N'', '' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = indexes.object_id
        and p.index_id= indexes.index_id
        and p.data_compression = 1
    ORDER BY p.partition_number FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS row_compression_clause ( row_compression_partition_list )


/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
    (SELECT N'', '' + CAST(p.partition_number AS VARCHAR(32))
    FROM sys.partitions AS p
    WHERE p.object_id = indexes.object_id
        and p.index_id= indexes.index_id
        and p.data_compression = 2
    ORDER BY p.partition_number FOR XML PATH(''''), TYPE).value(''.'', ''NVARCHAR(MAX)''),1,2,'''')) AS page_compression_clause ( page_compression_partition_list )

/* Partitioning Ordinal */ OUTER APPLY (
    SELECT MAX(QUOTENAME(c.name)) AS column_name
    FROM sys.index_columns AS ic 
    JOIN sys.columns AS c ON 
        ic.column_id=c.column_id  
        and ic.object_id=c.object_id
    WHERE ic.object_id = indexes.object_id
        and ic.index_id= indexes.index_id
        and ic.partition_ordinal = 1) AS partitioning_column

 WHERE
	 usage_stats.user_updates > 100
	 AND ISNULL(usage_stats.system_seeks,0) = 0
	 AND ISNULL(usage_stats.user_seeks,0) = 0
	 AND ISNULL(usage_stats.user_scans,0) = 0
 GROUP BY
	 indexes.object_id,
	 tables.create_date,
	 indexes.index_id,
	 indexes.name,
	 usage_stats.user_seeks,
	 usage_stats.user_scans,
	 usage_stats.user_updates,
	 usage_stats.system_updates,
	 is_primary_key,
	 sc.name,
	 tables.name,
	 indexes.is_unique,
	 key_definition,
	 include_definition,
	 indexes.filter_definition,
	 row_compression_clause.row_compression_partition_list,
	 page_compression_clause.page_compression_partition_list,
	 psc.name,
	 fg.name,
	 partitioning_column.column_name
HAVING
	 SUM(partitions.rows) > 200000'

IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (
	DBName SYSNAME, 
	SchemaName SYSNAME, 
	TableName SYSNAME, 
	IndexName SYSNAME NULL, 
	RowsCount INT, 
	IndexSizeKB INT, 
	DropCMD NVARCHAR(MAX), 
	LastStatsDate DATETIME ,
	TableCreatedDate DATETIME NULL,  
	UpdatesCount INT NULL ,
	CreateCMD NVARCHAR(MAX));

IF CONVERT(varchar(300),SERVERPROPERTY('Edition')) <> 'SQL Azure' AND (SELECT sqlserver_start_time FROM sys.dm_os_sys_info) < DATEADD(dd,-14,GETDATE())
BEGIN
	DECLARE DBs CURSOR FOR 
		SELECT name FROM sys.databases WHERE database_id > 4 AND state = 0 AND (DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE')
	OPEN DBs
	WHILE 1=1
	BEGIN
		FETCH NEXT FROM DBs INTO @CurrDB
		IF @@FETCH_STATUS <> 0 BREAK;
			SET @Executor = QUOTENAME(@CurrDB) + N'..sp_executesql'
			INSERT INTO #tmp 
			EXEC @Executor @CMD
	END
	CLOSE DBs
	DEALLOCATE DBs
END

INSERT INTO [dbo].[UnusedIndexes]
(
				SampleDate,DBName, SchemaName, TableName, IndexName, RowsCount, IndexSizeKB, UpdatesCount, DropCMD, TableCreatedDate, LastStatsDate, CreateCMD, IsDeployed
)
SELECT 
			  SampleDate = @SampleDate,  T.DBName, T.SchemaName, T.TableName, T.IndexName, T.RowsCount, T.IndexSizeKB, T.UpdatesCount, T.DropCMD, T.TableCreatedDate, T.LastStatsDate, T.CreateCMD, IsDeployed = 0
FROM #tmp T
LEFT JOIN
	[dbo].[UnusedIndexes] UI
ON 
	T.DBName = UI.DBName AND
	T.SchemaName = UI.SchemaName AND
	T.TableName = UI.TableName AND
	T.IndexName = UI.IndexName
WHERE UI.DBName IS NULL AND UI.SchemaName IS NULL AND UI.IndexName IS NULL
ORDER BY T.DBName ASC, T.IndexSizeKB DESC, T.RowsCount DESC
END
GO

USE [DBA]
GO

/****** Object:  StoredProcedure [dbo].[usp_Deadlock_Alert]    Script Date: 1/13/2021 1:22:44 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
/*

Get the optional @Profile values- 
	select name from msdb.dbo.sysmail_profile;

Get the optional @OperatorName values-
	SELECT name FROM msdb.dbo.sysoperators;

Execution Example: 
EXEC [dbo].[usp_SendEmail] 
	@Profile = N'ISRL-mail',
	@OperatorName = N'israel-labs-group', --N'eranf'
	@Subject = N'The Job DatabaseIntegrityCheck - USER_DATABASES" was ended with success ' ,
	@body = NULL,
	@ServerName = N'ILIHA1-DB06' -- N'ILIHA1-DB08'

*/

CREATE OR ALTER PROCEDURE [dbo].[usp_SendEmail]
(
	@Profile NVARCHAR(20),
	@OperatorName NVARCHAR(50),
	@Subject NVARCHAR(200),
	@body NVARCHAR(MAX) = NULL,
	@ServerName sysname
)
AS
SET NOCOUNT ON;

DECLARE @DateTime DATETIME2,
		@recipients NVARCHAR(200)
		;
SET @DateTime = GETDATE();
SET @Profile = (select name from msdb.dbo.sysmail_profile WHERE name = @Profile);
SET @recipients = (SELECT email_address FROM msdb.dbo.sysoperators WHERE name = @OperatorName);
SET @Subject = @Subject + ' ' + CAST(@DateTime AS NVARCHAR(25)) + ' at server: ' + @ServerName

EXEC msdb.dbo.sp_send_dbmail
	@profile_name = @profile,
	@recipients = @recipients,
	@body =  @body,
	@Subject = @Subject
GO



USE [msdb]
GO

/****** Object:  Job [Capture Unused Indexes]    Script Date: 3/3/2021 2:34:57 PM ******/
--EXEC msdb.dbo.sp_delete_job @job_id=N'b839a701-8fbb-4405-8171-5f9959b3cbff', @delete_unused_schedule=1
GO

/****** Object:  Job [Capture Unused Indexes]    Script Date: 3/3/2021 2:34:57 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 3/3/2021 2:34:57 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Capture Unused Indexes', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'eranf', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execte Store Procedure]    Script Date: 3/3/2021 2:34:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execte Store Procedure', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC usp_Capture_Unused_Indexes;', 
		@database_name=N'DBA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Send Email If Unused Indexes Was Found]    Script Date: 3/3/2021 2:34:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Send Email If Unused Indexes Was Found', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'IF EXISTS (SELECT 1 FROM [DBA].[dbo].[UnusedIndexes] WHERE [SampleDate] > DATEADD(MINUTE,-5,GETDATE()))
BEGIN
	EXEC [dbo].[usp_SendEmail] 
		@Profile = N''ISRL-mail'',
		@OperatorName = N''xxxxxx'', --N''israel-labs-group'', 
		@Subject = N''Unused Indexes was found '' ,
		@body = ''Based on the last check, unused indexes was found and should be considered to be droped.
		which means no query is seeking or scanning the index but, data is inserted to the index. 
		use the following query to view the last sample and to get the drop commands:
		SELECT * FROM  [dbo].[UnusedIndexes] ORDER BY SampleDate DESC.
		You should save the index creation script as a rollback script before dropping any index.
		drop one index at a time and verify no performance issues or application break
		occurring before dropping the next one'' ,		
		@ServerName = N''xxxxxxxxxxxx''  
END', 
		@database_name=N'DBA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Delete Old Data - Old than 3 months]    Script Date: 3/3/2021 2:34:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Delete Old Data - Old than 3 months', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DELETE FROM [DBA].[dbo].[UnusedIndexes]
WHERE [SampleDate] < DATEADD(MONTH,-3,GETDATE())', 
		@database_name=N'DBA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Monthly On Fri 17:00', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=32, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210125, 
		@active_end_date=99991231, 
		@active_start_time=170000, 
		@active_end_time=235959, 
		@schedule_uid=N'd41ba582-5de7-4e86-851a-97979cb6a922'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


USE [msdb]
GO

/****** Object:  Job [Drop Unused Indexes Daily]    Script Date: 3/3/2021 2:34:36 PM ******/
--EXEC msdb.dbo.sp_delete_job @job_id=N'34a41781-4593-42fa-827a-60a8cf74c3c1', @delete_unused_schedule=1
GO

/****** Object:  Job [Drop Unused Indexes Daily]    Script Date: 3/3/2021 2:34:36 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 3/3/2021 2:34:36 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Drop Unused Indexes Daily', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Procedure]    Script Date: 3/3/2021 2:34:37 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Procedure', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC  DropUnusedIndexes', 
		@database_name=N'DBA', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily At 07:00 AM', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=31, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20210224, 
		@active_end_date=99991231, 
		@active_start_time=70000, 
		@active_end_time=235959, 
		@schedule_uid=N'd1c2e92a-d698-4ed4-9e31-b959f16d19f6'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO





