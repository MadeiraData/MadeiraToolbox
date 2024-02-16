/*
Change Tracking Manual Cleanup
==============================
Author: Eitan Blumin | Madeira Data Solutions | https://madeiradata.com
Date: 2024-02-10
Description:

Specific tables can experience a high rate of changes, and you might find that the autocleanup job can't clean up
the side tables and syscommittab within the 30-minute interval.
If this occurs, you can run a manual cleanup job with increased frequency to facilitate the process.

For SQL Server and Azure SQL Managed Instance, create a background job using sp_flush_CT_internal_table_on_demand
with a shorter internal than the default 30 minutes.
For Azure SQL Database, Azure Logic Apps can be used to schedule these jobs.

The following T-SQL code can be used as the command of a job to help cleanup the side tables for change tracking.

Based on:
https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/cleanup-and-troubleshoot-change-tracking-sql-server?view=sql-server-ver16#run-cleanup-more-frequently-than-30-minutes
*/
USE MyDB; -- replace with your DB
GO
DBCC TRACEON (8284, -1); -- this grants access to sp_flush_CT_internal_table_on_demand
GO
SET NOCOUNT ON;
SET LOCK_TIMEOUT 5000; -- adjust as needed

-- Loop to invoke manual cleanup procedure for cleaning up change tracking tables in a database
-- Fetch the tables enabled for change tracking
DECLARE CT_tables CURSOR
LOCAL FAST_FORWARD
FOR
SELECT QUOTENAME(SCHEMA_NAME(tbl.schema_id)) + '.' + QUOTENAME(OBJECT_NAME(ctt.object_id)) AS TableName
, FYI = CONCAT(CHAR(13) + CHAR(10), N'Last cleanup history record was : '
	, (
		SELECT TOP(1) start_time, end_time, rows_cleaned_up, cleanup_version, comments
		FROM dbo.MSChange_tracking_history AS hist WITH(NOLOCK)
		WHERE OBJECT_ID(hist.table_name) = ctt.object_id
		ORDER BY start_time DESC
		FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER
	  )
	)
FROM sys.change_tracking_tables ctt WITH(NOLOCK)
INNER JOIN sys.tables tbl WITH(NOLOCK)
    ON tbl.object_id = ctt.object_id;

-- Set up the variables
DECLARE @tablename VARCHAR(255), @TimeString varchar(25), @RCount bigint, @FYI varchar(1000);

OPEN CT_tables;

WHILE 1 = 1
BEGIN
    -- Fetch the table to be cleaned up
    FETCH NEXT FROM CT_tables INTO @tablename, @FYI;
	IF @@FETCH_STATUS <> 0 BREAK;

	BEGIN TRY
		SET @TimeString = CONVERT(varchar(19), GETUTCDATE(), 121)
		RAISERROR(N'[%s UTC] - Cleaning up change tracking for : %s %s',0,1,@TimeString,@tablename,@FYI) WITH NOWAIT;

		-- Execute the manual cleanup stored procedure
		EXEC sp_flush_CT_internal_table_on_demand @tablename, @RCount OUTPUT;

		SET @TimeString = CONVERT(varchar(19), GETUTCDATE(), 121)
		RAISERROR(N'[%s UTC] - Cleaned up %I64d row(s) of change tracking for : %s',0,1,@TimeString,@RCount,@tablename) WITH NOWAIT;
	END TRY
	BEGIN CATCH
		DECLARE @ErrNumber int, @ErrMessage nvarchar(max)
		SET @ErrNumber = ERROR_NUMBER();
		SET @ErrMessage = ERROR_MESSAGE();
		SET @TimeString = CONVERT(varchar(19), GETUTCDATE(), 121)
		RAISERROR(N'[%s UTC] - ERROR %d : %s',0,1,@TimeString,@ErrNumber,@ErrMessage);
	END CATCH

END

CLOSE CT_Tables;
DEALLOCATE CT_Tables;