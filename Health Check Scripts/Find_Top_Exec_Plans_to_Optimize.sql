/*
=======================================================
Find Top Exec Plans to Optimize
=======================================================
Author: Eitan Blumin | eitanblumin.com , madeiradata.com
Date: 2020-08-12
Description:
Use this script to discover execution plans with a good
potential for performance optimization.
Finds execution plans with warnings and problematic operators.
Filter based on execution count, CPU time and duration.

You can use the parameters at the top to control behavior.
Change the sorting column at the end to control which queries
would show up first.
*/

DECLARE
	  @MaxQueriesToCheckFromQueryStats	INT = 200
	, @MaxExecPlansToOutput	INT = 20

	, @DaysBackToCheck	INT = 2

	, @MinimumExecCount	INT = 1000
	, @MinimumCPUTime	INT = 100
	, @MinimumDuration	INT = 300

IF OBJECT_ID('tempdb..#topqueries') IS NOT NULL DROP TABLE #topqueries;

SELECT TOP (@MaxQueriesToCheckFromQueryStats)
	 qs.sql_handle
	,qs.plan_handle
	,qs.creation_time
	,qs.last_execution_time
	,qs.execution_count
	,qs.total_elapsed_time
	,qs.min_elapsed_time
	,qs.max_elapsed_time
	,qs.total_worker_time
	,qs.min_worker_time
	,qs.max_worker_time
	,qs.total_physical_reads
	,qs.min_physical_reads
	,qs.max_physical_reads
	,qs.total_logical_reads
	,qs.min_logical_reads
	,qs.max_logical_reads
	,qs.total_used_grant_kb
	,qs.min_used_grant_kb
	,qs.max_used_grant_kb
INTO #topqueries
FROM sys.dm_exec_query_stats AS qs
WHERE qs.last_execution_time > DATEADD(day, -@DaysBackToCheck, GETDATE())
AND qs.execution_count > @MinimumExecCount
AND qs.max_worker_time > @MinimumCPUTime
AND qs.max_elapsed_time > @MinimumDuration
ORDER BY qs.total_elapsed_time DESC
OPTION (RECOMPILE);

;WITH XMLNAMESPACES (DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan', N'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)  
SELECT TOP (@MaxExecPlansToOutput) *
FROM
(
SELECT 
	 SQLBatchText = t.text
	,QueryPlan = QP.query_plan
	,HasParallelism = QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="Parallelism"][1])')
	,HasMissingIndexes = CASE WHEN QP.query_plan.query('.').exist('data(//MissingIndexes[1])') = 1 THEN 1 ELSE 0 END
	,WarningTypes = STUFF((
					SELECT ', ' + warning
					FROM 
					(
						SELECT DISTINCT cast(node_xml.query('local-name(.)') as varchar(1000)) AS warning
						FROM QP.query_plan.nodes('//Warnings/*') AS W(node_xml)
						UNION ALL
						SELECT 'ClusteredIndexScan' WHERE QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="Clustered Index Scan"][1])') = 1
						UNION ALL
						SELECT 'IndexScan' WHERE QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="Index Scan"][1])') = 1
						--SELECT 'IndexScan' WHERE QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="Index Scan"][@EstimateRows * @AvgRowSize > 5000.0][1])') = 1
						UNION ALL
						SELECT 'TableScan' WHERE QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="Table Scan"][1])') = 1
						UNION ALL
						SELECT 'KeyLookup' WHERE QP.query_plan.query('.').exist('data(//IndexScan[@Lookup="1"][1])') = 1
						UNION ALL
						SELECT 'RIDLookup' WHERE QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="RID Lookup"][1])') = 1
						UNION ALL
						SELECT 'TableSpool' WHERE QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="Table Spool"][1])') = 1
						UNION ALL
						SELECT 'IndexSpool' WHERE QP.query_plan.query('.').exist('data(//RelOp[@PhysicalOp="Index Spool"][1])') = 1
						UNION ALL
						SELECT 'MissingIndexes' WHERE QP.query_plan.query('.').exist('data(//MissingIndexes[1])') = 1
						UNION ALL
						SELECT 'SortOperator' WHERE QP.query_plan.query('.').exist('data(//RelOp[(@PhysicalOp[.="Sort"])])') = 1
						UNION ALL
						SELECT 'UserFunctionFilter' WHERE QP.query_plan.query('.').exist('data(//RelOp/Filter/Predicate/ScalarOperator/Compare/ScalarOperator/UserDefinedFunction[1])') = 1
						UNION ALL
						SELECT 'RemoteQuery' WHERE QP.query_plan.query('.').exist('data(//RelOp[(@PhysicalOp[contains(., "Remote")])])') = 1
						UNION ALL
						SELECT 'CompileMemoryLimitExceeded' WHERE QP.query_plan.query('.').exist('data(//StmtSimple/@StatementOptmEarlyAbortReason[.="MemoryLimitExceeded"])') = 1
						UNION ALL
						SELECT TOP 1 'NonSargeableScalarFunction'
						FROM         QP.query_plan.nodes('//RelOp/IndexScan/Predicate/ScalarOperator/Compare/ScalarOperator') AS ca(x)
						WHERE        (   ca.x.query('.').exist('//ScalarOperator/Intrinsic/@FunctionName') = 1
										OR     ca.x.query('.').exist('//ScalarOperator/IF') = 1 )
						UNION ALL
						SELECT TOP 1 'NonSargeableExpressionWithJoin'
						FROM         QP.query_plan.nodes('//RelOp//ScalarOperator') AS ca(x)
						WHERE        QP.query_plan.query('.').exist('data(//RelOp[contains(@LogicalOp, "Join")])') = 1
									AND ca.x.query('.').exist('//ScalarOperator[contains(@ScalarString, "Expr")]') = 1
						UNION ALL
						SELECT TOP 1 'NonSargeableLIKE'
						FROM         QP.query_plan.nodes('//RelOp/IndexScan/Predicate/ScalarOperator') AS ca(x)
						CROSS APPLY  ca.x.nodes('//Const') AS co(x)
						WHERE        ca.x.query('.').exist('//ScalarOperator/Intrinsic/@FunctionName[.="like"]') = 1
									AND (   (   co.x.value('substring(@ConstValue, 1, 1)', 'VARCHAR(100)') <> 'N'
												AND co.x.value('substring(@ConstValue, 2, 1)', 'VARCHAR(100)') = '%' )
											OR (   co.x.value('substring(@ConstValue, 1, 1)', 'VARCHAR(100)') = 'N'
													AND co.x.value('substring(@ConstValue, 3, 1)', 'VARCHAR(100)') = '%' ))
					) AS w
					FOR XML PATH('')
					), 1, 2, '')
	,qs.*
FROM #topqueries AS qs
CROSS APPLY sys.dm_exec_query_plan (qs.plan_handle) AS QP
CROSS APPLY sys.dm_exec_sql_text (qs.sql_handle) AS t
WHERE QP.query_plan IS NOT NULL
) AS q
WHERE WarningTypes IS NOT NULL
ORDER BY 
	total_elapsed_time		-- Duration
	--total_worker_time		-- CPU
	--total_physical_reads	-- Disk I/O
	--total_logical_reads	-- Memory/Disk Activity
	--total_used_grant_kb	-- Memory Utilization
	DESC