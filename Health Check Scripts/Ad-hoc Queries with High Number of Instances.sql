/*========================================================================================================================

Description:	Display information about ad-hoc queries with high number of instances
Scope:			Instance
Author:			Guy Glantser
Created:		09/09/2020
Last Updated:	15/09/2020
Notes:			Ad-hoc queries with a high number of instances waste CPU and memory resources by generating the same plan many times.
				Such queries should be parameterized in most cases. But pay attention to parameter sniffing issues.
				If there are many ad-hoc queries with high number of instances,
				then also consider enabling the "Optimize for Ad-hoc Workloads" instance configuration.

=========================================================================================================================*/

WITH
	AdhocQueries
(
		DatabaseName ,
		QueryHash ,
		BatchText ,
		BatchPlan ,
		FirsCompilationTime ,
		LastCompilationTime ,
		TotalSizeInCache_MB ,
		QueryHashRowNumber ,
		QueryHashCount
)
AS
(
	SELECT
		DatabaseName		= DB_NAME (BatchPlans.[dbid]) ,
		QueryHash			= QueryStats.query_hash ,
		BatchText			= BatchTexts.[text] ,
		BatchPlan			= BatchPlans.query_plan ,
		FirsCompilationTime	= MIN (QueryStats.creation_time) OVER (PARTITION BY BatchPlans.[dbid] , QueryStats.query_hash) ,
		LastCompilationTime	= QueryStats.creation_time ,
		TotalSizeInCache_MB	= CAST (SUM (CONVERT(bigint, CachedPlans.size_in_bytes)) OVER (PARTITION BY BatchPlans.[dbid] , QueryStats.query_hash) / 1024.0 / 1024.0 AS DECIMAL(19,2)) ,
		QueryHashRowNumber	= ROW_NUMBER () OVER (PARTITION BY BatchPlans.[dbid] , QueryStats.query_hash ORDER BY QueryStats.creation_time DESC) ,
		QueryHashCount		= COUNT (*) OVER (PARTITION BY BatchPlans.[dbid] , QueryStats.query_hash)
	FROM
		sys.dm_exec_cached_plans AS CachedPlans
	INNER JOIN
		sys.dm_exec_query_stats AS QueryStats
	ON
		CachedPlans.plan_handle = QueryStats.plan_handle
	CROSS APPLY
		sys.dm_exec_sql_text (QueryStats.sql_handle) AS BatchTexts
	CROSS APPLY
		sys.dm_exec_query_plan (CachedPlans.plan_handle) AS BatchPlans
	WHERE
		CachedPlans.objtype = N'Adhoc'
)
SELECT TOP (10)
	DatabaseName			= DatabaseName ,
	QueryHash				= QueryHash ,
	BatchText				= BatchText ,
	BatchPlan				= BatchPlan ,
	CompilationsPerMinute	=
		CASE
			WHEN DATEDIFF (MINUTE , FirsCompilationTime , LastCompilationTime) = 0
				THEN 0
			ELSE
				CAST (CAST (QueryHashCount AS DECIMAL(19,2)) / CAST (DATEDIFF (MINUTE , FirsCompilationTime , LastCompilationTime) AS DECIMAL(19,2)) AS DECIMAL(19,2))
		END ,
	LastCompilationTime		= LastCompilationTime ,
	TotalSizeInCache_MB		= TotalSizeInCache_MB ,
	PotentialMemorySavings	= CAST (TotalSizeInCache_MB - TotalSizeInCache_MB / QueryHashCount AS DECIMAL(19,2)) ,
	QueryHashCount			= QueryHashCount
FROM
	AdhocQueries
WHERE
	QueryHashRowNumber = 1
ORDER BY
	QueryHashCount		DESC ,
	TotalSizeInCache_MB	DESC;
GO
