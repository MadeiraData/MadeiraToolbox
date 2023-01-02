SELECT
	DB_NAME(d.database_id)										AS [DB],
	OBJECT_NAME(d.[object_id], d.database_id)					AS [ObjectName],
	CASE
		WHEN d.[type] = 'P'  THEN 'SQL SP'
		WHEN d.[type] = 'PC' THEN 'Assembly (CLR) SP'
		WHEN d.[type] = 'X'  THEN 'Extended SP'
		ELSE 'unknow'
	END															AS [ObjectType],
	qp.[encrypted]												AS [IsEncrypted],
	d.cached_time,
	d.execution_count											AS [TotalRuns],
	d.last_execution_time,

	-- Last run
	d.last_elapsed_time,
	d.last_worker_time											AS [last_CPU_time],
	d.last_logical_reads,
	d.last_physical_reads,
	d.last_logical_writes,
	d.last_spills,

	-- AVG of total
	d.total_elapsed_time/execution_count						AS [avg_elapsed_time],
	d.total_worker_time/execution_count							AS [avg_CPU_time],
	d.total_logical_reads/execution_count						AS [avg_logical_reads],
	d.total_physical_reads/execution_count						AS [avg_physical_reads],
	d.total_logical_writes/execution_count						AS [avg_logical_writes],
	d.total_spills/execution_count								AS [avg_spills],

	-- Totals runs (agregate)
	d.total_elapsed_time,
	d.total_worker_time											AS [total_CPU_time],
	d.total_logical_reads,
	d.total_physical_reads,
	d.total_logical_writes,
	d.total_spills,

	-- MIN run
	d.min_elapsed_time,
	d.min_worker_time											AS [min_CPU_time],
	d.min_logical_reads,
	d.min_physical_reads,
	d.min_logical_writes,
	d.min_spills,

	-- MAX run
	d.max_elapsed_time,
	d.max_worker_time											AS [max_CPU_time],
	d.max_logical_reads,
	d.max_physical_reads,
	d.max_logical_writes,
	d.max_spills,

	-- Cached Plan & SQL text
	qp.query_plan,
	st.[text],

	-- sql & plan_handls
	d.[sql_handle],
	d.plan_handle,
	'DBCC FREEPROCCACHE (0x' + CONVERT(NVARCHAR(MAX), d.plan_handle, 2) + ');'	AS [Script to remove plan]
FROM
	sys.dm_exec_procedure_stats AS d
	CROSS APPLY sys.dm_exec_query_plan(d.plan_handle) qp
	OUTER APPLY sys.dm_exec_sql_text(d.plan_handle) st
WHERE
	OBJECT_NAME(d.[object_id], d.database_id) IN (N'', N'')
ORDER BY
	 --DB_NAME(d.database_id)DESC,
	 d.execution_count DESC
--	,d.[total_worker_time] DESC;


