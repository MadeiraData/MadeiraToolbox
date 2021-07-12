/*
Asynchronous Ledger Demo
========================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2021-07-03
Description:
This script demonstrates a use case of a high-throughput table
which serves as a "hot-spot" for inserts and updates and queries.
This causes performance problems due to long lock chains, possible deadlocks,
and sometimes even worker thread starvation.

The script then creates a solution for this problem in the form of
an "Asynchronous Ledger" method utilizing ALTER TABLE SWITCH TO while
separating between a "buffer" and a "staging" table, to eliminate any
contention between the high-throughput INSERTs and any other operations.

More details: https://eitanblumin.com/?p=2204
*/

/*************************************************/
/************** Synchronous Demo *****************/
/*************************************************/
IF OBJECT_ID('MyHotTable') IS NOT NULL DROP TABLE MyHotTable;
GO
CREATE TABLE MyHotTable (
  SomeIdentifier int NOT NULL,
  SomeOtherIdentifier int NOT NULL,
  SomeText nchar(100) NULL,
  SomeOtherText nchar(100) NULL,
  SomeCounter int NOT NULL,
  CONSTRAINT PK_MyHotTable PRIMARY KEY CLUSTERED (SomeIdentifier, SomeOtherIdentifier)
);

CREATE NONCLUSTERED INDEX IX_MyHotTable_SomeOtherIdentifier ON MyHotTable (SomeOtherIdentifier) INCLUDE (SomeCounter, SomeText, SomeOtherText);
GO
WITH Level1
AS
(
	SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
	FROM sys.all_columns a CROSS JOIN sys.all_columns b
),
Level2
AS
(
	SELECT TOP (10000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
	FROM sys.all_columns a CROSS JOIN sys.all_columns b
)
INSERT INTO MyHotTable (SomeIdentifier, SomeOtherIdentifier, SomeText, SomeOtherText, SomeCounter)
SELECT Level1.n, Level2.n, NEWID(), NEWID(), ABS(CHECKSUM(NEWID())) % 10000
FROM Level1 CROSS JOIN Level2
GO
CREATE OR ALTER PROCEDURE UpsertHotTable
  @SomeIdentifier int,
  @SomeOtherIdentifier int
AS
SET NOCOUNT ON;
UPDATE MyHotTable SET SomeCounter = SomeCounter + 1
WHERE SomeIdentifier = @SomeIdentifier AND SomeOtherIdentifier = @SomeOtherIdentifier

IF @@ROWCOUNT = 0
BEGIN
  INSERT INTO MyHotTable (SomeIdentifier, SomeOtherIdentifier, SomeText, SomeOtherText, SomeCounter)
  VALUES (@SomeIdentifier, @SomeOtherIdentifier, NEWID(), NEWID(), 1)
END
GO
	-- UPSERT thread:
	DECLARE
	    @SomeIdentifier int = ABS(CHECKSUM(NEWID())) % 2 + 100
	  , @SomeOtherIdentifier int = ABS(CHECKSUM(NEWID())) % 5 + 10000

	EXEC UpsertHotTable @SomeIdentifier, @SomeOtherIdentifier
GO

	-- Query thread:
	IF OBJECT_ID('tempdb..#AggTemp') IS NULL
	CREATE TABLE #AggTemp
	(
		SomeIdentifier int,
		TotalCounter int
	);

	BEGIN TRAN

	INSERT INTO #AggTemp
	SELECT SomeIdentifier, SUM(SomeCounter)
	FROM MyHotTable WITH(REPEATABLEREAD)
	GROUP BY SomeIdentifier

	WAITFOR DELAY '00:00:03'

	DELETE FROM #AggTemp

	COMMIT
GO



/*************************************************/
/************* Asynchronous Demo *****************/
/*************************************************/
IF OBJECT_ID('MyHotTable_Buffer') IS NOT NULL DROP TABLE MyHotTable_Buffer;
GO
CREATE TABLE MyHotTable_Buffer (
  SomeIdentifier int NOT NULL,
  SomeOtherIdentifier int NOT NULL,
  SomeText nchar(100) NULL,
  SomeOtherText nchar(100) NULL,
  SomeCounter int NOT NULL
);
GO
IF OBJECT_ID('MyHotTable_Staging') IS NOT NULL DROP TABLE MyHotTable_Staging;
GO
CREATE TABLE MyHotTable_Staging (
  SomeIdentifier int NOT NULL,
  SomeOtherIdentifier int NOT NULL,
  SomeText nchar(100) NULL,
  SomeOtherText nchar(100) NULL,
  SomeCounter int NOT NULL
);
GO
CREATE OR ALTER PROCEDURE UpsertHotTable
  @SomeIdentifier int,
  @SomeOtherIdentifier int
AS
SET NOCOUNT ON;

INSERT INTO MyHotTable_Buffer (SomeIdentifier, SomeOtherIdentifier, SomeText, SomeOtherText, SomeCounter)
VALUES (@SomeIdentifier, @SomeOtherIdentifier, NEWID(), NEWID(), 1)
GO
CREATE OR ALTER PROCEDURE [dbo].[sp_MyHotTable_AsyncInsert]
	@DelayBeforeRepeat VARCHAR(17) = '00:00:15'
AS
BEGIN
	SET NOCOUNT, XACT_ABORT, ARITHABORT, QUOTED_IDENTIFIER ON;
	DECLARE @PreexistingData BIT = 0
	
RestartPoint:
	
	IF EXISTS (SELECT * FROM dbo.MyHotTable_Staging)
	BEGIN
		SET @PreexistingData = 1;
		GOTO AggregateMerge;
	END

TruncateAndSwitch:
	
	TRUNCATE TABLE dbo.MyHotTable_Staging;
	ALTER TABLE dbo.MyHotTable_Buffer SWITCH TO dbo.MyHotTable_Staging;

AggregateMerge:
	DECLARE @RCount int, @DateString nvarchar(25);
	
	; WITH Trgt
	AS
	(
		SELECT *
		FROM dbo.MyHotTable AS prod WITH(UPDLOCK, HOLDLOCK)
		WHERE EXISTS
		(
			SELECT 1
			FROM MyHotTable_Staging AS stg
			WHERE stg.SomeIdentifier = prod.SomeIdentifier
			AND stg.SomeOtherIdentifier = prod.SomeOtherIdentifier
		)
	)
	MERGE INTO Trgt
	USING (
		SELECT SomeIdentifier, SomeOtherIdentifier, SUM(SomeCounter) AS SomeCounter
		FROM MyHotTable_Staging 
		GROUP BY SomeIdentifier, SomeOtherIdentifier
	) AS Src
			ON  Src.SomeIdentifier = Trgt.SomeIdentifier
			AND Src.SomeOtherIdentifier = Trgt.SomeOtherIdentifier
	WHEN MATCHED THEN
		UPDATE SET SomeCounter = Trgt.SomeCounter + Src.SomeCounter
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (SomeIdentifier, SomeOtherIdentifier, SomeText, SomeOtherText, SomeCounter)
		VALUES (Src.SomeIdentifier, Src.SomeOtherIdentifier, NEWID(), NEWID(), Src.SomeCounter)
	;

	SET @RCount = @@ROWCOUNT;
	SET @DateString = CONVERT(nvarchar(25), GETDATE(), 121)

	IF @RCount > 0 RAISERROR(N'%s - Loaded %d row(s)',0,1,@DateString,@RCount) WITH NOWAIT;

	TRUNCATE TABLE dbo.MyHotTable_Staging;
	
	IF @PreexistingData = 1
	BEGIN
		SET @PreexistingData = 0;
		GOTO RestartPoint;
	END

	-- Check for new data
	IF EXISTS (SELECT TOP 1 1 FROM dbo.MyHotTable_Buffer WITH(READPAST))
	BEGIN
		GOTO RestartPoint;
	END
	ELSE IF @DelayBeforeRepeat IS NOT NULL
	BEGIN
		WAITFOR DELAY @DelayBeforeRepeat;

		IF EXISTS (SELECT TOP 1 1 FROM dbo.MyHotTable_Buffer WITH(READPAST))
		BEGIN
			GOTO RestartPoint;
		END
	END
END
GO

SET XACT_ABORT, ARITHABORT ON;

IF NOT EXISTS (SELECT * FROM msdb..sysjobs WHERE name = N'MyHotTable_AsyncInsert')
BEGIN

    BEGIN TRANSACTION
    DECLARE @ReturnCode INT
    SELECT @ReturnCode = 0
    IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
    BEGIN
    EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    END
    
    DECLARE @jobId BINARY(16)
    EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'MyHotTable_AsyncInsert', 
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
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'MyHotTable_AsyncInsert', 
    		@step_id=1, 
    		@cmdexec_success_code=0, 
    		@on_success_action=1, 
    		@on_success_step_id=0, 
    		@on_fail_action=2, 
    		@on_fail_step_id=0, 
    		@retry_attempts=0, 
    		@retry_interval=0, 
    		@os_run_priority=0, @subsystem=N'TSQL', 
    		@command=N'EXEC [dbo].[sp_MyHotTable_AsyncInsert]', 
    		@database_name=N'MyDB', 
    		@flags=4
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every 1 minute', 
    		@enabled=1, 
    		@freq_type=4, 
    		@freq_interval=1, 
    		@freq_subday_type=4, 
    		@freq_subday_interval=1, 
    		@freq_relative_interval=0, 
    		@freq_recurrence_factor=0, 
    		@active_start_date=20210615, 
    		@active_end_date=99991231, 
    		@active_start_time=0, 
    		@active_end_time=235959
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    COMMIT TRANSACTION
    GOTO EndSave
    QuitWithRollback:
        IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
    EndSave:
    
END
GO
