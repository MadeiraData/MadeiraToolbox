/*
Check for low PAGE compression success rates
============================================
Author: Eitan Blumin
Date: 2022-01-13
Based on blog post by Paul Randal:
https://www.sqlskills.com/blogs/paul/the-curious-case-of-tracking-page-compression-success-rates/
*/
DECLARE
	/* threshold parameters: */
	 @MinimumCompressionAttempts int = 200
	,@MaxAttemptSuccessRatePercentage int = 20

	/* change index rebuild options as needed: */
	,@RebuildOptions nvarchar(MAX) = N'ONLINE = ON, SORT_IN_TEMPDB = ON, MAXDOP = 4'

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Results AS TABLE
(
[database_name] sysname NOT NULL, [schema_name] sysname NULL, [table_name] sysname NULL, [index_name] sysname NULL,
partition_number int NULL, total_rows int NULL, is_partitioned bit NULL, attempts_count int NOT NULL, success_count int NOT NULL,
range_scans_percent int NULL, updates_percent int NULL,
success_rate AS (success_count * 1.0 / NULLIF(attempts_count,0))
);

DECLARE @CurrDB sysname, @SpExecuteSql nvarchar(1000);
DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE state = 0
AND source_database_id IS NULL
AND database_id > 2
AND HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name],'Updateability') = 'READ_WRITE'

OPEN DBs;
WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @SpExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @Results
	EXEC @SpExecuteSql N'
SELECT DISTINCT
	db_name(),
	object_schema_name (i.object_id),
	object_name (i.object_id),
	i.name,
	p.partition_number,
	p.[rows],
	is_partitioned = 
		CASE WHEN EXISTS 
			(SELECT NULL FROM sys.partitions AS p2
			WHERE p2.partition_number > 1
			AND p2.object_id = p.object_id
			AND p2.index_id = p.index_id)
		THEN 1 ELSE 0 END,
	page_compression_attempt_count,
	page_compression_success_count,
	range_scans_percent = ISNULL(
		FLOOR(ISNULL(ios.range_scan_count,0) * 1.0 /
		NULLIF(
			ISNULL(ios.range_scan_count,0) +
			ISNULL(ios.leaf_delete_count,0) + 
			ISNULL(ios.leaf_insert_count,0) + 
			ISNULL(ios.leaf_page_merge_count,0) + 
			ISNULL(ios.leaf_update_count,0) + 
			ISNULL(ios.singleton_lookup_count,0)
		, 0) * 100.0), 0),
	updates_percent = ISNULL(
		CEILING(ISNULL(ios.leaf_update_count, 0) * 1.0 /
		NULLIF(
			ISNULL(ios.range_scan_count,0) +
			ISNULL(ios.leaf_delete_count,0) + 
			ISNULL(ios.leaf_insert_count,0) + 
			ISNULL(ios.leaf_page_merge_count,0) + 
			ISNULL(ios.leaf_update_count,0) + 
			ISNULL(ios.singleton_lookup_count,0)
		, 0) * 100.0), 0)
FROM sys.indexes AS i
INNER JOIN sys.partitions AS p ON p.object_id = i.object_id AND p.index_id = i.index_id
CROSS APPLY sys.dm_db_index_operational_stats (db_id(), p.object_id, p.index_id, p.partition_number) AS ios
WHERE
p.data_compression = 2
AND page_compression_attempt_count >= @MinimumCompressionAttempts
AND page_compression_success_count * 1.0 / NULLIF(page_compression_attempt_count,0) <= @MaxAttemptSuccessRatePercentage / 100.0
', N'@MinimumCompressionAttempts int, @MaxAttemptSuccessRatePercentage int'
, @MinimumCompressionAttempts, @MaxAttemptSuccessRatePercentage

END

CLOSE DBs;
DEALLOCATE DBs;

SELECT *
, RemediationCmd = N'USE ' + QUOTENAME([database_name]) + N'; ALTER '
	+ CASE WHEN [index_name] IS NULL THEN N'TABLE ' 
	  ELSE N'INDEX ' + QUOTENAME([index_name]) + N' ON '
	  END
	+ QUOTENAME([schema_name]) + N'.' + QUOTENAME([table_name])
	+ CASE WHEN is_partitioned = 1 THEN N' PARTITION ' + CONVERT(nvarchar(MAX), partition_number)
	  ELSE N''
	  END
	+ N' REBUILD WITH(DATA_COMPRESSION = ROW'
	+ ISNULL(N', ' + NULLIF(@RebuildOptions, N''), N'')
	+ N');'
FROM @Results
ORDER BY success_rate ASC
