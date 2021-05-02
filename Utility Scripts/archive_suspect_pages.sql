/**********************************
-- Move suspect pages to archive --
***********************************

Move data from the system table msdb.dbo.suspect_pages to msdb.dbo.suspect_pages_history

As documented in the following article, this table must be maintained manually:
https://docs.microsoft.com/en-us/sql/relational-databases/backup-restore/manage-the-suspect-pages-table-sql-server#Recommendations

Your monitoring should be querying this system table, and in some cases, the event_type column 
is not being updated even after the corruption is fixed, causing false alarms to be fired.
We have even obsered cases where corruption event types 1/2/3 remain in this table,
despite DBCC CHECKDB returning no indication of corruption.

To avoid that, this command will move data older than 7 days to an archive table, regardless of status.
This archive table would never be deleted/truncated.

If there's still more than 800 rows remaining which are newer than 7 days,
then archive from the system table until only 500 rows are left.
*/

IF OBJECT_ID('msdb.dbo.suspect_pages_history') IS NULL
BEGIN
	--- creating a copy of suspect_pages sys table ---
	CREATE TABLE [msdb].[dbo].[suspect_pages_history]
	(
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[page_id] [bigint] NOT NULL,
		[event_type] [int] NOT NULL,
		[error_count] [int] NOT NULL,
		[last_update_date] [datetime] NOT NULL
	);
	CREATE CLUSTERED INDEX [IX_cl_updatedate] ON [msdb].[dbo].[suspect_pages_history]
	([last_update_date] ASC)
	-- WITH( DATA_COMPRESSION = PAGE ) -- uncomment this for Enterprise editions or versions 2016 SP1 and newer.
	;
END

-- Deletes and moves records in a single command to ensure no data loss
DELETE	[msdb].dbo.suspect_pages 
OUTPUT	deleted.* 
INTO	[msdb].[dbo].[suspect_pages_history]
WHERE	last_update_date <= DATEADD(dd, -7, GETDATE())

DECLARE @CurrentCount INT, @ToRemove INT;
SELECT @CurrentCount = COUNT(*) FROM [msdb].[dbo].[suspect_pages_history] WITH(NOLOCK)

-- Check if there's still high rowcount in system table
IF @CurrentCount > 800
BEGIN
	-- make sure only 500 rows at most are left
	SET @ToRemove = @CurrentCount - 500

	-- Archive oldest rows
	DELETE T
	OUTPUT	deleted.* 
	INTO	[msdb].[dbo].[suspect_pages_history]
	FROM
	(
		SELECT TOP (@ToRemove) *
		FROM [msdb].dbo.suspect_pages
		ORDER BY last_update_date ASC
	) AS T
END