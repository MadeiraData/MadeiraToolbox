/*
Check for low PAGE compression success rates
============================================
Author: Eitan Blumin
Date: 2022-01-13
Based on blog post by Paul Randal:
https://www.sqlskills.com/blogs/paul/the-curious-case-of-tracking-page-compression-success-rates/
*/
DECLARE
	 @MinimumCompressionAttempts int = 200
	,@MaxAttemptSuccessRatePercentage int = 20

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
CREATE TABLE #Results
(
[DatabaseName] sysname NOT NULL, [SchemaName] sysname NULL, [TableName] sysname NULL, [IndexName] sysname NULL,
PartitionNumber int NULL, TotalRows int NULL, AttemptsCount int NOT NULL, SuccessCount int NOT NULL,
SuccessRate AS (SuccessCount * 1.0 / NULLIF(AttemptsCount,0))
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

	INSERT INTO #Results
	EXEC @SpExecuteSql N'SELECT DISTINCT
	db_name(),
	object_schema_name (i.object_id),
	object_name (i.object_id),
    i.name,
    p.partition_number,
	p.[rows],
    page_compression_attempt_count,
    page_compression_success_count
FROM
    sys.indexes AS i
INNER JOIN
    sys.partitions AS p
ON
    p.object_id = i.object_id AND p.index_id = i.index_id
CROSS APPLY
    sys.dm_db_index_operational_stats (db_id(), i.object_id, i.index_id, p.partition_number) AS ios
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
FROM #Results
ORDER BY SuccessRate ASC
