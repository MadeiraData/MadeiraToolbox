/*
QueryExecutionFrequency
Written by: Eric Rouach - Madeira Data Solutions 
Date: 2022-12-22
Description: 
The following script is a slight variation of Pinal Dave's script from his blog post: 
https://blog.sqlauthority.com/2021/08/12/sql-server-find-high-frequency-queries/
It will return details about queries for which a plan is currently existing in the plan cache.
The result is ordered by the ExecutionFrequencyPerSec column (descending) so that
we can focus on the most frequently executed queries.

Optionally, you may uncomment the WHERE clause to filter out according to a specific query_hash.
*/


SELECT
	t.text									                                as QueryText,
	qs.query_hash                                                           as query_hash,
	t.dbid									                                as DatabaseId,
	DB_NAME(t.dbid)							                                as DatabaseName,
	qs.creation_time						                                as CreationTime,
	qs.execution_count						                                as ExecutionCount,
	ROUND(CAST(qs.total_elapsed_time as float)	                  
	/											                  
	1000										                  
	/											                  
	NULLIF(CAST(qs.execution_count AS FLOAT),0),3)                          as [AvgElapsedTime(ms)],
	ROUND(CAST(qs.execution_count AS FLOAT)
	/
	NULLIF(CAST(DATEDIFF(SECOND,qs.creation_time,GETDATE()) as float),0),3) as ExecutionFrequencyPerSec,
	qp.query_plan                                                           as QueryPlan
FROM
	sys.dm_exec_query_stats qs 
	CROSS APPLY
	sys.dm_exec_query_plan(qs.plan_handle) qp
	CROSS APPLY
	sys.dm_exec_sql_text(qs.plan_handle) t
--WHERE
--	qs.query_hash = 0xD72909107A423269  <========== replace with specific query_hash
ORDER BY
	ExecutionFrequencyPerSec DESC