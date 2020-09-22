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
Change the sorting column to control which queries
would show up first.
=======================================================
Change Log:
	2020-09-22 Added @ColumnToSortBy, added backward-compatibility
	2020-08-13 Added statement-level details, and aggregation on query stats
=======================================================
*/
DECLARE

	  @MaxQueriesToCheckFromQueryStats	INT = 200
	, @MaxExecPlansToOutput			INT = 20

	, @ColumnToSortBy			SYSNAME = 
						----------------------------------------
						-- Choose one column to sort by.
						-- Comment / Uncomment below as needed:
						----------------------------------------
						--'execution_count'		-- Execution Count
						'total_elapsed_time'		-- Total Duration
						--'total_worker_time'		-- Total CPU
						--'total_physical_reads'	-- Total Disk I/O
						--'total_logical_reads'		-- Total Memory/Disk Activity
						--'total_used_grant_kb'		-- Total Memory Consumption (SQL 2016 and newer only)

	, @DaysBackToCheck			INT = 2

	, @MinimumExecCount			INT = 1000
	, @MinimumCPUTime			INT = 100
	, @MinimumDuration			INT = 300


/******************************************************************************/
/***            NO NEED TO CHANGE ANYTHING BELOW THIS LINE                  ***/
/******************************************************************************/


IF OBJECT_ID('tempdb..#topqueries') IS NOT NULL DROP TABLE #topqueries;
CREATE TABLE #topqueries
(
	ID int IDENTITY(1,1) PRIMARY KEY CLUSTERED
	,[sql_handle] varbinary(64)
	,statement_start_offset int    NULL
	,statement_end_offset int      NULL
	,plan_handle varbinary(64)     NULL
	,creation_time datetime	       NULL
	,last_execution_time datetime  NULL
	,execution_count bigint	       NULL
	,total_elapsed_time bigint     NULL
	,min_elapsed_time bigint       NULL
	,max_elapsed_time bigint       NULL
	,total_worker_time bigint      NULL
	,min_worker_time bigint	       NULL
	,max_worker_time bigint	       NULL
	,total_physical_reads bigint   NULL
	,min_physical_reads bigint     NULL
	,max_physical_reads bigint     NULL
	,total_logical_reads bigint    NULL
	,min_logical_reads bigint      NULL
	,max_logical_reads bigint      NULL
	,total_used_grant_kb bigint    NULL
	,min_used_grant_kb bigint      NULL
	,max_used_grant_kb bigint      NULL
);

DECLARE @CMD NVARCHAR(MAX);

SET @CMD = N'
INSERT INTO #topqueries
SELECT TOP (@MaxQueriesToCheckFromQueryStats)
	 qs.sql_handle
	,qs.statement_start_offset
	,qs.statement_end_offset
	,qs.plan_handle
	,creation_time		= MIN(qs.creation_time)
	,last_execution_time	= MAX(qs.last_execution_time)
	,execution_count	= SUM(qs.execution_count)
	,total_elapsed_time	= SUM(qs.total_elapsed_time)
	,min_elapsed_time	= MIN(qs.min_elapsed_time)
	,max_elapsed_time	= MAX(qs.max_elapsed_time)
	,total_worker_time	= SUM(qs.total_worker_time)
	,min_worker_time	= MIN(qs.min_worker_time)
	,max_worker_time	= MAX(qs.max_worker_time)
	,total_physical_reads	= SUM(qs.total_physical_reads)
	,min_physical_reads	= MIN(qs.min_physical_reads)
	,max_physical_reads	= MAX(qs.max_physical_reads)
	,total_logical_reads	= SUM(qs.total_logical_reads)
	,min_logical_reads	= MIN(qs.min_logical_reads)
	,max_logical_reads	= MAX(qs.max_logical_reads)
	'
	-- if SQL 2016 and newer, get memory grant columns
	+ CASE WHEN CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 13 THEN
	N',total_used_grant_kb	= SUM(qs.total_used_grant_kb)
	,min_used_grant_kb	= MIN(qs.min_used_grant_kb)
	,max_used_grant_kb	= MAX(qs.max_used_grant_kb)'
	ELSE 
	N',total_used_grant_kb	= NULL
	,min_used_grant_kb	= NULL
	,max_used_grant_kb	= NULL'
	END + N'
FROM sys.dm_exec_query_stats AS qs
WHERE qs.last_execution_time > DATEADD(day, -@DaysBackToCheck, GETDATE())
AND qs.execution_count > @MinimumExecCount
AND qs.max_worker_time > @MinimumCPUTime
AND qs.max_elapsed_time > @MinimumDuration
GROUP BY
	 qs.sql_handle
	,qs.statement_start_offset
	,qs.statement_end_offset
	,qs.plan_handle
ORDER BY
	' + @ColumnToSortBy + N'
	DESC
OPTION (RECOMPILE);'

EXEC sp_executesql @CMD
	, N'@MaxQueriesToCheckFromQueryStats INT, @DaysBackToCheck INT, @MinimumExecCount BIGINT, @MinimumCPUTime BIGINT, @MinimumDuration BIGINT'
	, @MaxQueriesToCheckFromQueryStats, @DaysBackToCheck, @MinimumExecCount, @MinimumCPUTime, @MinimumDuration

;WITH XMLNAMESPACES (DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan', N'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p)  
SELECT TOP (@MaxExecPlansToOutput) *
FROM
(
SELECT 
	 SQLBatchText = t.text
	,SQLStmtText = SUBSTRING(t.text,
					CASE WHEN NULLIF(qs.statement_start_offset,-1) IS NULL THEN 0
					ELSE qs.statement_start_offset / 2 + 1
					END
					,
					CASE WHEN NULLIF(qs.statement_end_offset,-1) IS NULL THEN LEN(t.text)
					ELSE (statement_end_offset - qs.statement_start_offset) / 2 + 1
					END)
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
CROSS APPLY (
		SELECT
				HighestStatementCost = MAX(S.node_xml.value('(@StatementSubTreeCost)[1]','float'))
			, TotalBatchCost = SUM(S.node_xml.value('(@StatementSubTreeCost)[1]','float'))
		FROM q.QueryPlan.nodes('//StmtSimple') AS S(node_xml)
		WHERE S.node_xml.query('.').exist('data(//StmtSimple[@StatementSubTreeCost>0][1])') = 1
		) AS StmtSummary
WHERE WarningTypes IS NOT NULL
ORDER BY ID ASC
