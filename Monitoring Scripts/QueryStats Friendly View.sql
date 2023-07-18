/*

QueryStats Friendly View
========================

Author:			Guy Glantser, https://www.madeiradata.com
Date:			18/07/2023
Description:
	This script creates a view that returns query statistics including statement-level query text and plan
	based on sys.dm_exec_query_stats.
	The view can then be used easily to identify the top queries based on various dimensions,
	such as the top 5 heavy queries in terms of CPU utilization (worker time).
*/

CREATE OR ALTER VIEW
	dbo.QueryStats
(
	QueryText ,
	QueryPlan ,
	CompilationCount ,
	LastCompilationDateTime ,
	LastExecutionDateTime ,
	ExecutionsSinceLastCompilation ,
	TotalWorkerTimeInMicroseconds ,
	LastWorkerTimeInMicroseconds ,
	MinWorkerTimeInMicroseconds ,
	MaxWorkerTimeInMicroseconds ,
	TotalPhysicalReads ,
	LastPhysicalReads ,
	MinPhysicalReads ,
	MaxPhysicalReads ,
	TotalLogicalWrites ,
	LastLogicalWrites ,
	MinLogicalWrites ,
	MaxLogicalWrites ,
	TotalLogicalReads ,
	LastLogicalReads ,
	MinLogicalReads ,
	MaxLogicalReads ,
	TotalElapsedTimeInMicroseconds ,
	LastElapsedTimeInMicroseconds ,
	MinElapsedTimeInMicroseconds ,
	MaxElapsedTimeInMicroseconds ,
	AverageElapsedTimeInMicroseconds ,
	TotalRowCount ,
	LastRowCount ,
	MinRowCount ,
	MaxRowCount
)
AS

WITH XMLNAMESPACES
	(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan') ,

	SingleQueryStats
(
	QueryPlan ,
	CompilationCount ,
	LastCompilationDateTime ,
	LastExecutionDateTime ,
	ExecutionsSinceLastCompilation ,
	TotalWorkerTimeInMicroseconds ,
	LastWorkerTimeInMicroseconds ,
	MinWorkerTimeInMicroseconds ,
	MaxWorkerTimeInMicroseconds ,
	TotalPhysicalReads ,
	LastPhysicalReads ,
	MinPhysicalReads ,
	MaxPhysicalReads ,
	TotalLogicalWrites ,
	LastLogicalWrites ,
	MinLogicalWrites ,
	MaxLogicalWrites ,
	TotalLogicalReads ,
	LastLogicalReads ,
	MinLogicalReads ,
	MaxLogicalReads ,
	TotalElapsedTimeInMicroseconds ,
	LastElapsedTimeInMicroseconds ,
	MinElapsedTimeInMicroseconds ,
	MaxElapsedTimeInMicroseconds ,
	AverageElapsedTimeInMicroseconds ,
	TotalRowCount ,
	LastRowCount ,
	MinRowCount ,
	MaxRowCount
)
AS
(
	SELECT
		QueryPlan							= TRY_CONVERT (XML , QueryPlans.query_plan) ,
		CompilationCount					= QueryStats.plan_generation_num ,
		LastCompilationDateTime				= QueryStats.creation_time ,
		LastExecutionDateTime				= QueryStats.last_execution_time ,
		ExecutionsSinceLastCompilation		= QueryStats.execution_count ,
		TotalWorkerTimeInMicroseconds		= QueryStats.total_worker_time ,
		LastWorkerTimeInMicroseconds		= QueryStats.last_worker_time ,
		MinWorkerTimeInMicroseconds			= QueryStats.min_worker_time ,
		MaxWorkerTimeInMicroseconds			= QueryStats.max_worker_time ,
		TotalPhysicalReads					= QueryStats.total_physical_reads ,
		LastPhysicalReads					= QueryStats.last_physical_reads ,
		MinPhysicalReads					= QueryStats.min_physical_reads ,
		MaxPhysicalReads					= QueryStats.max_physical_reads ,
		TotalLogicalWrites					= QueryStats.total_logical_writes ,
		LastLogicalWrites					= QueryStats.last_logical_writes ,
		MinLogicalWrites					= QueryStats.min_logical_writes ,
		MaxLogicalWrites					= QueryStats.max_logical_writes ,
		TotalLogicalReads					= QueryStats.total_logical_reads ,
		LastLogicalReads					= QueryStats.last_logical_reads ,
		MinLogicalReads						= QueryStats.min_logical_reads ,
		MaxLogicalReads						= QueryStats.max_logical_reads ,
		TotalElapsedTimeInMicroseconds		= QueryStats.total_elapsed_time ,
		LastElapsedTimeInMicroseconds		= QueryStats.last_elapsed_time ,
		MinElapsedTimeInMicroseconds		= QueryStats.min_elapsed_time ,
		MaxElapsedTimeInMicroseconds		= QueryStats.max_elapsed_time ,
		AverageElapsedTimeInMicroseconds	= CAST ((CAST (QueryStats.total_elapsed_time AS DECIMAL(19,2)) / CAST (QueryStats.execution_count AS DECIMAL(19,2))) AS DECIMAL(19,2)) ,
		TotalRowCount						= QueryStats.total_rows ,
		LastRowCount						= QueryStats.last_rows ,
		MinRowCount							= QueryStats.min_rows ,
		MaxRowCount							= QueryStats.max_rows
	FROM
		sys.dm_exec_query_stats AS QueryStats
	CROSS APPLY
		sys.dm_exec_text_query_plan (QueryStats.plan_handle , QueryStats.statement_start_offset , QueryStats.statement_end_offset) AS QueryPlans
)

SELECT
	QueryText							= Statements.StatementText.value (N'(@StatementText)[1]' , 'NVARCHAR(MAX)') ,
	QueryPlan							= SingleQueryStats.QueryPlan ,
	CompilationCount					= SingleQueryStats.CompilationCount ,
	LastCompilationDateTime				= SingleQueryStats.LastCompilationDateTime ,
	LastExecutionDateTime				= SingleQueryStats.LastExecutionDateTime ,
	ExecutionsSinceLastCompilation		= SingleQueryStats.ExecutionsSinceLastCompilation ,
	TotalWorkerTimeInMicroseconds		= SingleQueryStats.TotalWorkerTimeInMicroseconds ,
	LastWorkerTimeInMicroseconds		= SingleQueryStats.LastWorkerTimeInMicroseconds ,
	MinWorkerTimeInMicroseconds			= SingleQueryStats.MinWorkerTimeInMicroseconds ,
	MaxWorkerTimeInMicroseconds			= SingleQueryStats.MaxWorkerTimeInMicroseconds ,
	TotalPhysicalReads					= SingleQueryStats.TotalPhysicalReads ,
	LastPhysicalReads					= SingleQueryStats.LastPhysicalReads ,
	MinPhysicalReads					= SingleQueryStats.MinPhysicalReads ,
	MaxPhysicalReads					= SingleQueryStats.MaxPhysicalReads ,
	TotalLogicalWrites					= SingleQueryStats.TotalLogicalWrites ,
	LastLogicalWrites					= SingleQueryStats.LastLogicalWrites ,
	MinLogicalWrites					= SingleQueryStats.MinLogicalWrites ,
	MaxLogicalWrites					= SingleQueryStats.MaxLogicalWrites ,
	TotalLogicalReads					= SingleQueryStats.TotalLogicalReads ,
	LastLogicalReads					= SingleQueryStats.LastLogicalReads ,
	MinLogicalReads						= SingleQueryStats.MinLogicalReads ,
	MaxLogicalReads						= SingleQueryStats.MaxLogicalReads ,
	TotalElapsedTimeInMicroseconds		= SingleQueryStats.TotalElapsedTimeInMicroseconds ,
	LastElapsedTimeInMicroseconds		= SingleQueryStats.LastElapsedTimeInMicroseconds ,
	MinElapsedTimeInMicroseconds		= SingleQueryStats.MinElapsedTimeInMicroseconds ,
	MaxElapsedTimeInMicroseconds		= SingleQueryStats.MaxElapsedTimeInMicroseconds ,
	AverageElapsedTimeInMicroseconds	= SingleQueryStats.AverageElapsedTimeInMicroseconds ,
	TotalRowCount						= SingleQueryStats.TotalRowCount ,
	LastRowCount						= SingleQueryStats.LastRowCount ,
	MinRowCount							= SingleQueryStats.MinRowCount ,
	MaxRowCount							= SingleQueryStats.MaxRowCount
FROM
	SingleQueryStats
CROSS APPLY
	QueryPlan.nodes (N'/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS Statements (StatementText);
GO
