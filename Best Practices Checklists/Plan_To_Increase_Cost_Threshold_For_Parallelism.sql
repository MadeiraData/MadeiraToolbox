/*
Author: Eitan Blumin, (t: @EitanBlumin | b: eitanblumin.com)
Date: February, 2018
Description:

The data returned by the script would be a list of execution plans,
their respective SQL statements, the Sub-Tree cost of the statements, and their usecounts.

Using this script, you will be able to identify execution plans that use parallelism, 
which may stop using parallelism if you change “cost threshold for parallelism” to a value
higher than the sub-tree cost of their non-parallel counterpart (i.e. when using OPTION(MAXDOP 1)).

More info:
https://eitanblumin.com/2018/11/06/planning-to-increase-cost-threshold-for-parallelism-like-a-smart-person
*/
DECLARE
	  @MinUseCount			INT	= 50	-- Set minimum usecount to ignore rarely-used plans
	, @MaxSubTreeCost		FLOAT	= 30	-- Set the maximum sub-tree cost, plans with higher cost than this wouldn't normally interest us

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @CurrentCostThreshold INT
SELECT @CurrentCostThreshold = CONVERT(INT, value_in_use)
FROM sys.configurations
WHERE [name] = 'cost threshold for parallelism';

RAISERROR(N'Current Cost Threshold for Parallelism: %d', 0, 1, @CurrentCostThreshold) WITH NOWAIT;

WITH XMLNAMESPACES   
(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
SELECT *
FROM
(  
SELECT
	ecp.plan_handle,
	CompleteQueryPlan	= query_plan, 
	StatementText		= n.value('(@StatementText)[1]', 'VARCHAR(4000)'), 
	StatementSubTreeCost	= n.value('(@StatementSubTreeCost)[1]', 'VARCHAR(128)'), 
	ParallelSubTreeXML	= n.query('.'),  
	ecp.usecounts, 
	ecp.size_in_bytes,
	RankPerText		= ROW_NUMBER() OVER (PARTITION BY n.value('(@StatementText)[1]', 'VARCHAR(4000)') ORDER BY ecp.usecounts DESC)
FROM sys.dm_exec_cached_plans AS ecp 
CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS eqp 
CROSS APPLY query_plan.nodes('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS qn(n)
WHERE	n.query('.').exist('//RelOp[@PhysicalOp="Parallelism"]') = 1 
AND	ecp.usecounts > @MinUseCount
AND	n.value('(@StatementSubTreeCost)[1]', 'float') <= @MaxSubTreeCost
) AS Q
WHERE
	RankPerText = 1 -- This would filter out duplicate statements, returning only those with the highest usecount
ORDER BY
	usecounts DESC