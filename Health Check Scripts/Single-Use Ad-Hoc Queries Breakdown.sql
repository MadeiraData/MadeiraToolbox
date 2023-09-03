SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT
ServerName = @@SERVERNAME
, SingleUsePlansSize_MB
, SingleUsePlanCachePercentOfPlanCache
, TotalPlanCacheSize_MB
, SingleUsePlanCachePercentOfServer
, TotalServerMemoryMB
, OptimizeForAdHocShouldBeEnabled = CASE WHEN TotalServerMemoryMB <= 65535 THEN
						CASE WHEN SingleUsePlanCachePercentOfServer >= 10 THEN 1 ELSE 0 END
						ELSE
						CASE WHEN SingleUsePlanCachePercentOfServer >= 5 THEN 1 ELSE 0 END
						END
FROM
(
	SELECT 
	SUM(CASE WHEN objtype IN ('Adhoc','Prepared') AND usecounts = 1 THEN CAST(size_in_bytes AS bigint) ELSE 0 END)/1024/1024 AS SingleUsePlansSize_MB,
	SUM(CAST(size_in_bytes AS bigint))/1024/1024 AS TotalPlanCacheSize_MB
	FROM sys.dm_exec_cached_plans
) AS q
CROSS APPLY
(SELECT
	CAST(total_physical_memory_kb as bigint) / 1024 AS TotalServerMemoryMB FROM sys.dm_os_sys_memory
) AS os
CROSS APPLY
(SELECT
	SingleUsePlanCachePercentOfPlanCache = CAST(SingleUsePlansSize_MB * 100.0 / TotalPlanCacheSize_MB AS decimal(5,2)),
	SingleUsePlanCachePercentOfServer = CAST(SingleUsePlansSize_MB * 100.0 / TotalServerMemoryMB AS decimal(5,2))
) AS pct



SELECT objtype, cacheobjtype, 
COUNT_BIG(*) AS NumOfSingleUseObjects, 
SUM(refcounts) AS NumOfRefObjects, 
SUM(CAST(size_in_bytes AS bigint))/1024/1024 AS SizeMB
FROM sys.dm_exec_cached_plans
WHERE objtype IN ('Adhoc','Prepared') AND usecounts = 1
GROUP BY objtype, cacheobjtype
ORDER BY NumOfRefObjects DESC;



SELECT *
, [Total MBs – USE Count 1] * 100 / [Total MBs] AS [Percentage - USE Count 1 Compared to Total Plan Cache]
, [Total MBs – USE Count 1] * 100 / (SELECT
			cast(total_physical_memory_kb as bigint) / 1024 AS TotalServerMemoryMB FROM sys.dm_os_sys_memory -- for SQL 2012 and newer
			--cast(physical_memory_in_bytes as bigint) / 1024 / 1024 AS TotalServerMemoryMB FROM sys.dm_os_sys_info -- for SQL 2008 / 2008R2
			) AS [Percentage - USE Count 1 Compared to Total Server Memory]
FROM
(
SELECT objtype AS [CacheType],
COUNT_BIG(*) AS [Total Plans],
SUM(CAST(size_in_bytes AS DECIMAL(18, 2))) / 1024 / 1024 AS [Total MBs],
AVG(usecounts) AS [Avg Use Count],
SUM(CAST((CASE WHEN usecounts = 1 THEN size_in_bytes
ELSE 0
END) AS DECIMAL(18, 2))) / 1024 / 1024 AS [Total MBs – USE Count 1],
SUM(CASE WHEN usecounts = 1 THEN 1
ELSE 0
END) AS [Total Plans – USE Count 1]
FROM sys.dm_exec_cached_plans
GROUP BY objtype
) AS q
ORDER BY [Total MBs – USE Count 1] DESC
