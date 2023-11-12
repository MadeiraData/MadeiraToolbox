/*========================================================================================================================

Description:	Display data about queries running in the database since @Since sorted by the total CPU time
Scope:			Database
Author:			Guy Glantser
Created:		12/11/2023
Last Updated:	12/11/2023
Notes:			The inspiration to this script was derived from this blog post by Jose Manuel Jurado Diaz:
				https://techcommunity.microsoft.com/t5/azure-database-support-blog/lesson-learned-442-determining-cpu-usage-in-azure-sql-database/ba-p/3953086
				Unlike Jose's suggestion, which is based on sys.dm_exec_query_stats, this script is based on the Query Store,
				so it will only work if you have the Query Store enabled, and it will only be reliable if the Query Store contains data since @Since.
				The main use case for this script is when you encounter a high CPU consumption,
				and you would like to know which are the top queries that constribute to the high CPU consumption.
				By examining the CPUTimePercentage column, you will see whether it's a single query that is responsible for most of the CPU consumption
				or it's many queries that together make an impact.

=========================================================================================================================*/

USE
	DatabaseName;
GO


DECLARE
	@Since AS DATETIMEOFFSET = DATEADD (HOUR , -1 , SYSUTCDATETIME ());	-- Since when to collect runtime statistics

WITH
	Queries
(
	QueryId ,
	QueryText ,
	ContainingObjectId ,
	QueryParameterizationType ,
	PlanId ,
	QueryPlan ,
	IsTrivialPlan ,
	IsParallelPlan ,
	IsForcedPlan ,
	LastExecutionTime ,
	CompilesCount ,
	AvgCompileDuration ,
	ExecutionCount ,
	TotalDuration ,
	AvgDuration ,
	TotalCPUTime
)
AS
(
	SELECT
		QueryId						= Queries.query_id ,
		QueryText					= QueryTexts.query_sql_text ,
		ContainingObjectId			= Queries.[object_id] ,
		QueryParameterizationType	= Queries.query_parameterization_type_desc ,
		PlanId						= Plans.plan_id ,
		QueryPlan					= CAST (Plans.query_plan AS XML) ,
		IsTrivialPlan				= Plans.is_trivial_plan ,
		IsParallelPlan				= Plans.is_parallel_plan ,
		IsForcedPlan				= Plans.is_forced_plan ,
		LastExecutionTime			= Plans.last_execution_time ,
		CompilesCount				= Plans.count_compiles ,
		AvgCompileDuration			= Plans.avg_compile_duration ,
		ExecutionCount				= SUM (RuntimeStats.count_executions) ,
		TotalDuration				= SUM (RuntimeStats.count_executions * RuntimeStats.avg_duration) ,
		AvgDuration					= SUM (RuntimeStats.count_executions * RuntimeStats.avg_duration) / SUM (RuntimeStats.count_executions) ,
		TotalCPUTime				= SUM (RuntimeStats.count_executions * RuntimeStats.avg_cpu_time)
	FROM
		sys.query_store_query_text AS QueryTexts
	INNER JOIN
		sys.query_store_query AS Queries
	ON
		QueryTexts.query_text_id = Queries.query_text_id 
	INNER JOIN
		sys.query_store_plan AS Plans
	ON
		Queries.query_id = Plans.query_id
	INNER JOIN
		sys.query_store_runtime_stats AS RuntimeStats
	ON
		Plans.plan_id = RuntimeStats.plan_id
	INNER JOIN
		sys.query_store_runtime_stats_interval AS RuntimeStatsIntervals
	ON
		RuntimeStats.runtime_stats_interval_id = RuntimeStatsIntervals.runtime_stats_interval_id
	WHERE
	--	RuntimeStats.execution_type = 0	-- Regular
	--AND
		RuntimeStatsIntervals.start_time >= @Since
	GROUP BY
		Queries.query_id ,
		QueryTexts.query_sql_text ,
		Queries.object_id ,
		Queries.query_parameterization_type_desc ,
		Plans.plan_id ,
		Plans.query_plan ,
		Plans.is_trivial_plan ,
		Plans.is_parallel_plan ,
		Plans.is_forced_plan ,
		Plans.last_execution_time ,
		Plans.count_compiles ,
		Plans.avg_compile_duration
)
SELECT TOP (100)
	QueryId						= QueryId ,
	QueryText					= QueryText ,
	ContainingObjectId			= ContainingObjectId ,
	QueryParameterizationType	= QueryParameterizationType ,
	PlanId						= PlanId ,
	QueryPlan					= QueryPlan ,
	IsTrivialPlan				= IsTrivialPlan ,
	IsParallelPlan				= IsParallelPlan ,
	IsForcedPlan				= IsForcedPlan ,
	LastExecutionTime			= LastExecutionTime ,
	CompilesCount				= CompilesCount ,
	AvgCompileDuration			= AvgCompileDuration ,
	ExecutionCount				= ExecutionCount ,
	TotalDuration				= TotalDuration ,
	AvgDuration					= AvgDuration ,
	TotalCPUTime				= TotalCPUTime ,
	QueryRank					= ROW_NUMBER () OVER (ORDER BY TotalCPUTime DESC) ,
	CPUTimePercentage			= FORMAT (CAST (TotalCPUTime AS DECIMAL(19,2)) / CAST (SUM (TotalCPUTime) OVER () AS DECIMAL(19,2)) , 'P')
FROM
	Queries
ORDER BY
	QueryRank ASC;
GO
