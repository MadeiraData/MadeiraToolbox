/*
Check Index Fragmentation in all databases
==========================================
Author: Eitan Blumin | Madeira Data Solutions
Date: 2020-09-14

https://www.madeiradata.com
https://www.eitanblumin.com
*/
DECLARE
	 @SampleMode		VARCHAR(25) = NULL -- Valid inputs are DEFAULT, NULL, LIMITED, SAMPLED, or DETAILED. The default (NULL) is LIMITED.
	,@MinFragmentation	INT = 20
	,@MinPageCount		INT = 1000
	,@IncludeHeaps		BIT = 0 -- Set to 1 to also check heap tables

SET ARITHABORT, XACT_ABORT, NOCOUNT ON;

IF OBJECT_ID('tempdb..#results') IS NOT NULL DROP TABLE #results;
CREATE TABLE #results
(
	databaseName SYSNAME NULL,
	schemaName SYSNAME NULL,
	tableName SYSNAME NULL,
	indexName SYSNAME NULL,
	indexType SYSNAME NULL,
	lastStatsUpdate DATETIME NULL,
	avg_fragmentation_in_percent FLOAT NULL,
	record_count INT NULL,
	page_count INT NULL,
	compressed_page_count INT NULL
);

DECLARE @cmd NVARCHAR(MAX)
SET @cmd = CONCAT(N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
IF DATABASEPROPERTYEX(''?'', ''Updateability'') = ''READ_WRITE'' AND DATABASEPROPERTYEX(''?'', ''Status'') = ''ONLINE''
BEGIN
	USE [?];
	DECLARE @TimeString VARCHAR(25), @RCount INT
	SET @TimeString = CONVERT(VARCHAR, GETDATE(), 121);

	RAISERROR(N''[%s] Checking fragmentation in "?"...'',0,1,@TimeString) WITH NOWAIT;

	INSERT INTO #results
	SELECT
	  DB_NAME() AS databaseName
	 ,s.[name] AS schemaName
	 ,t.[name] AS tableName
	 ,i.[name] AS indexName
	 ,index_type_desc
	 ,STATS_DATE(t.object_id, i.index_id) AS lastStatsUpdate
	 ,avg_fragmentation_in_percent
	 ,record_count
	 ,page_count
	 ,compressed_page_count
	FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, '
		+ ISNULL(QUOTENAME(NULLIF(NULLIF(@SampleMode, 'NULL'), 'DEFAULT'), ''''), N'NULL') 
		+ N') ips
	INNER JOIN sys.tables t on t.[object_id] = ips.[object_id]
	INNER JOIN sys.schemas s on t.[schema_id] = s.[schema_id]
	INNER JOIN sys.indexes i ON (ips.object_id = i.object_id) AND (ips.index_id = i.index_id)
	WHERE
		avg_fragmentation_in_percent > ', @MinFragmentation, N'
	AND page_count > ', @MinPageCount, N'
	AND t.is_ms_shipped = 0'
	+ CASE WHEN @IncludeHeaps = 1 THEN N'' ELSE N'
	AND i.index_id >= 1' END + N'

	SET @RCount = @@ROWCOUNT;
	SET @TimeString = CONVERT(VARCHAR, GETDATE(), 121);
	RAISERROR(N''[%s] Found %d fragmented items in "?"'',0,1, @TimeString, @RCount);
END')
EXEC sp_MSforeachdb @cmd;

SELECT 
	CONCAT(QUOTENAME(databaseName), '.', QUOTENAME(schemaName), '.', QUOTENAME(tableName)) AS full_table_name
	,*
FROM
	#results
ORDER BY
	avg_fragmentation_in_percent DESC