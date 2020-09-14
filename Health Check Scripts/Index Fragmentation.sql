/*
Check Index Fragmentation in all databases
==========================================
Author: Eitan Blumin | Madeira Data Solutions
Date: 2020-09-14

https://www.madeiradata.com
https://www.eitanblumin.com
*/

SET ARITHABORT, XACT_ABORT, NOCOUNT ON;

IF OBJECT_ID('tempdb..#results') IS NOT NULL DROP TABLE #results;
CREATE TABLE #results
(
	databaseName SYSNAME,
	schemaName SYSNAME,
	tableName SYSNAME,
	indexName SYSNAME,
	indexType SYSNAME,
	lastStatsUpdate DATETIME,
	avg_fragmentation_in_percent FLOAT,
	record_count INT,
	page_count INT,
	compressed_page_count INT
);


EXEC sp_MSforeachdb N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
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
	FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''SAMPLED'') ips
	INNER JOIN sys.tables t on t.[object_id] = ips.[object_id]
	INNER JOIN sys.schemas s on t.[schema_id] = s.[schema_id]
	INNER JOIN sys.indexes i ON (ips.object_id = i.object_id) AND (ips.index_id = i.index_id)
	WHERE
		avg_fragmentation_in_percent > 20
	AND page_count > 1000
	AND t.is_ms_shipped = 0

	SET @RCount = @@ROWCOUNT;
	SET @TimeString = CONVERT(VARCHAR, GETDATE(), 121);
	RAISERROR(N''[%s] Found %d fragmented items in "?"'',0,1, @TimeString, @RCount);
END'

SELECT 
	CONCAT(QUOTENAME(databaseName), '.', QUOTENAME(schemaName), '.', QUOTENAME(tableName)) AS full_table_name
	,*
FROM
	#results
ORDER BY
	avg_fragmentation_in_percent DESC