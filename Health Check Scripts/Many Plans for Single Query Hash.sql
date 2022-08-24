DECLARE
	@RCA bit = 1,
	@MinimumSizeInPlanCacheMB int = 256,
	@Top int = 10,
	@PlanCountThreshold int = 5,
	@CountByPlanHandleInsteadOfPlanHash bit = 0
;
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @PlanCacheTotalSize decimal(19,2);
SELECT @PlanCacheTotalSize = CAST(SUM(CONVERT(bigint, size_in_bytes)) / 1024.0 / 1024.0 AS DECIMAL(19,2))
FROM sys.dm_exec_cached_plans

DECLARE @Result AS TABLE
(
 QueryHash binary(8),
 DistinctPlans int,
 DatabaseId int,
 TotalSizeMB decimal(19,2)
)

DECLARE @DBsCount int;
SELECT @DBsCount = COUNT(*) FROM sys.databases;

IF @PlanCountThreshold < @DBsCount
	SET @PlanCountThreshold = @DBsCount * 2;

INSERT INTO @Result
SELECT TOP (@Top)
    qs.query_hash
  , COUNT(DISTINCT (CASE WHEN @CountByPlanHandleInsteadOfPlanHash = 1 THEN qs.plan_handle ELSE qs.query_plan_hash END))
  , CAST(pa.value AS int)
  , CAST(SUM(ts.totalSize) / 1024.0 / 1024.0 AS decimal(19,2)) AS totalSize
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) pa
CROSS APPLY
(
	SELECT totalSize = SUM(CONVERT(bigint, size_in_bytes))
	FROM sys.dm_exec_cached_plans AS cp
	WHERE cp.plan_handle = qs.plan_handle
) AS ts
WHERE pa.attribute = 'dbid'
GROUP BY qs.query_hash, pa.value
HAVING COUNT(DISTINCT (CASE WHEN @CountByPlanHandleInsteadOfPlanHash = 1 THEN qs.plan_handle ELSE qs.query_plan_hash END)) >= @PlanCountThreshold
ORDER BY totalSize DESC
OPTION (RECOMPILE);

IF @RCA = 1
BEGIN
	SELECT QueryHash, query_plan_hash, DistinctPlans, DB_NAME(DatabaseId) AS DatabaseName, TotalSizeMB, TotalSizeMB / @PlanCacheTotalSize * 100 AS PercentOfTotalCache
	, qplan.query_plan AS example_query_plan, qtext.text AS example_sql_batch
	, StatsPerQueryPlanHashCmd = N';
	WITH QueryPlanHashes
	AS
	(
	SELECT TOP (' + CONVERT(nvarchar(max), @Top) + N') qs.query_plan_hash, qs.query_hash
	, TotalDistinctExecPlans = COUNT(*)
	, TotalExecutionCount = SUM(qs.execution_count)
	, TotalWorkerTime = SUM(qs.total_worker_time)
	, TotalElapsedTime = SUM(qs.total_elapsed_time)
	, TotalPhysicalReads = SUM(qs.total_physical_reads)
	, TotalLogicalReads = SUM(qs.total_logical_reads)
	, TotalLogicalWrites = SUM(qs.total_logical_writes)
	, TotalGrantKB = SUM(qs.total_grant_kb)
	, TotalUsedGrantKB = SUM(qs.total_used_grant_kb)
	FROM sys.dm_exec_query_stats AS qs
	WHERE qs.query_hash = ' + CONVERT(nvarchar(max), res.QueryHash, 1) + N'
	GROUP BY qs.query_plan_hash, qs.query_hash
	ORDER BY TotalExecutionCount DESC, TotalElapsedTime DESC
	)
	SELECT qs.*
	, ExampleQueryPlan = ex.query_plan
	, ExampleQueryText = ex.text
	FROM QueryPlanHashes AS qs
	CROSS APPLY (
		SELECT TOP 1 qplan.query_plan, txt.text
		FROM sys.dm_exec_query_stats AS qs2
		CROSS APPLY sys.dm_exec_query_plan(qs2.plan_handle) AS qplan
		CROSS APPLY sys.dm_exec_sql_text(qs2.sql_handle) AS txt
		WHERE qs2.query_hash = qs.query_hash
		AND qs2.query_plan_hash = qs.query_plan_hash
		ORDER BY qs2.execution_count DESC
		) AS ex'
	, MoreDetailsCmd = N'
	SELECT TOP (' + CONVERT(nvarchar(max), @Top) + N')
	ClearPlanHandleFromCacheCmd = N''DBCC FREEPROCCACHE ('' + CONVERT(nvarchar(max), qs.plan_handle, 1) + N'');''
	, qplan.query_plan, qs.query_plan_hash, txt.text, qs.*
	FROM sys.dm_exec_query_stats AS qs
	CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qplan
	CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS txt
	WHERE qs.query_hash = ' + CONVERT(nvarchar(max), res.QueryHash, 1) + N'
	ORDER BY qs.execution_count DESC'
	, ClearSqlHandleFromCacheCmd = N'DBCC FREEPROCCACHE (' + CONVERT(nvarchar(max), qs_p_handle.sql_handle, 1) + N');'
	FROM @Result AS res
	CROSS APPLY
	(
		SELECT TOP 1 qs.plan_handle, qs.sql_handle, qs.query_plan_hash
		FROM sys.dm_exec_query_stats AS qs
		WHERE qs.query_hash = res.QueryHash
		ORDER BY qs.execution_count DESC
	) AS qs_p_handle
	CROSS APPLY sys.dm_exec_query_plan(qs_p_handle.plan_handle) AS qplan
	CROSS APPLY sys.dm_exec_sql_text(qs_p_handle.sql_handle) AS qtext
	WHERE TotalSizeMB >= @MinimumSizeInPlanCacheMB
	OPTION(RECOMPILE);
END
ELSE
BEGIN
	SELECT
	Msg = N'Possible parameterization issues for Query Hash '
	+ CONVERT(nvarchar(max), QueryHash, 1) + N' in database ' + QUOTENAME(DB_NAME(DatabaseId))
	+ N': ' + CONVERT(nvarchar(max), DistinctPlans) + N' plans (' + CONVERT(nvarchar(max), TotalSizeMB) + N' MB which is '
	+ CONVERT(nvarchar(max), CONVERT(decimal(5,2), TotalSizeMB / @PlanCacheTotalSize * 100)) + N' % of plan cache)'
	, PlanCachePercent = CONVERT(decimal(5,2), TotalSizeMB / @PlanCacheTotalSize * 100)
	FROM @Result
	WHERE TotalSizeMB >= @MinimumSizeInPlanCacheMB
	OPTION(RECOMPILE);
END