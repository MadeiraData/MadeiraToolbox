-- Move suspect pages to archive
/*
Move data from the system table msdb.dbo.suspect_pages to msdb.dbo.DBSmart_suspect_pages
Our monitoring is querying this system table, and in some cases, the event_type column 
is not being updated even after the corruption is fixed, and false alarms are fired.
We have even obsered cases where event types 1/2/3 remain in this table,
despite DBCC CHECKDB returning no indication of corruption.
To avoid that, this command will move data older than 7 days to an archive
table - that is never being deleted/truncated.
*/

IF OBJECT_ID('msdb.dbo.DBSmart_suspect_pages') IS NULL
BEGIN
	--- creating a copy of suspect_pages sys table ---
	CREATE TABLE [msdb].[dbo].[DBSmart_suspect_pages]
	(
		[database_id] [int] NOT NULL,
		[file_id] [int] NOT NULL,
		[page_id] [bigint] NOT NULL,
		[event_type] [int] NOT NULL,
		[error_count] [int] NOT NULL,
		[last_update_date] [datetime] NOT NULL
	);
	CREATE CLUSTERED INDEX [IX_cl_updatedate] ON [msdb].[dbo].[DBSmart_suspect_pages]
	([last_update_date] ASC);
END

-- Deletes and moves records in a single command to ensure no data loss
DELETE	[msdb].dbo.suspect_pages 
OUTPUT	deleted.* 
INTO	[msdb].[dbo].[DBSmart_suspect_pages]
WHERE	last_update_date <= DATEADD(dd, -7, GETDATE())

-- Check if there's still high rowcount in system table
IF (SELECT COUNT(*) FROM [msdb].[dbo].[DBSmart_suspect_pages] WITH(NOLOCK)) > 800
BEGIN
	-- Delete based on rowcount
	DELETE T
	OUTPUT	deleted.* 
	INTO	[msdb].[dbo].[DBSmart_suspect_pages]
	FROM
	(
		SELECT TOP (500) *
		FROM [msdb].dbo.suspect_pages
		ORDER BY last_update_date ASC
	) AS T
END