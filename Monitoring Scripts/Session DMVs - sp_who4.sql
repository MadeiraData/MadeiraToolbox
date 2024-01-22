SELECT
	er.session_id								AS [SPID],
	er.blocking_session_id							AS [Blk by],
	es.[status]								AS [Status],
	GETDATE() - last_request_end_time					AS [Duration],
	start_time								AS [Start Time],
	er.wait_time								AS [Wait (ms)],		-- If the request is currently blocked, this column returns the duration in milliseconds, of the current wait. Is not nullable.
	er.wait_type								AS [Wait Type],		-- If the request is currently blocked, this column returns the type of wait. Is nullable.
	er.last_wait_type							AS [Last Wait Type],	-- If this request has previously been blocked, this column returns the type of the last wait. Is not nullable.
	(SELECT COUNT(*) FROM master..sysprocesses WHERE spid = er.session_id)	AS [Threads],
	CAST(mg.query_cost AS DECIMAL(10, 2))					AS [Cost],		-- Estimated query cost.
	er.cpu_time								AS [CPU (ms)],		-- CPU time in milliseconds that is used by the request. Is not nullable.
	er.logical_reads							AS [L.Reads],		-- Number of logical reads that have been performed by the request. Is not nullable.
	er.reads								AS [P.Reads],		-- Number of reads performed by this request. Is not nullable.
	er.writes								AS [Writes],		-- Number of writes performed by this request. Is not nullable.
	mg.requested_memory_kb/1024						AS [R.Memory (MB)],	-- Total requested amount of memory in megabytes.
	mg.granted_memory_kb/1024						AS [G.Memory (MB)],	-- Total amount of memory actually granted in megabytes. Can be NULL if the memory is not granted yet. For a typical situation, this value should be the same as requested_memory_kb. For index creation, the server may allow additional on-demand memory beyond initially granted memory.
	(mg.granted_memory_kb - mg.ideal_memory_kb)/1024			AS [M.Memory (MB)],	-- Missing amount of memory in megabytes (Total amount of memory actually granted minus size of the memory grant to fit everything into physical memory, based on the cardinality estimate).
	OBJECT_NAME(st.objectid, er.database_id)				AS [Object],
	SUBSTRING(st.[text], (er.statement_start_offset/2)+1,((CASE er.statement_end_offset WHEN -1 THEN DATALENGTH(st.[text]) WHEN 0 THEN DATALENGTH(st.[text]) ELSE er.statement_end_offset END - er.statement_start_offset)/2)+1) AS [Statement],
	CAST(p.query_plan AS XML)						AS [Plan],
	es.login_name								AS [Login],
	es.[host_name]								AS [Host],
	DB_NAME(er.database_id)							AS [Database],
	ISNULL(OBJECT_SCHEMA_NAME(st.objectid, er.database_id),N'dbo')		AS [Schema],
	es.[program_name]							AS [Program],
	er.percent_complete							AS [% Complete],
	er.estimated_completion_time						AS [Estimated Completion Time (ms)],
	DATEADD(ms, er.estimated_completion_time, GETDATE())			AS [Estimated Completion Time (ETA)],
	es.cpu_time								AS [Session Total CPU (ms)],		-- CPU time in milliseconds that is used by the session. Is not nullable.
	es.logical_reads							AS [Session Total L.Reads],		-- Number of logical reads that have been performed by the session. Is not nullable.
	es.reads								AS [Session Total P.Reads],		-- Number of reads performed by this session. Is not nullable.
	es.writes								AS [Session Total Writes]		-- Number of writes performed by this session. Is not nullable.
FROM
	sys.dm_exec_requests er
	OUTER APPLY sys.dm_exec_sql_text(er.[sql_handle]) st
	OUTER APPLY sys.dm_exec_text_query_plan(er.plan_handle, er.statement_start_offset, er.statement_end_offset) p
	LEFT JOIN sys.dm_exec_sessions es ON es.session_id = er.session_id
	LEFT JOIN sys.dm_exec_query_memory_grants mg ON mg.session_id = er.session_id
WHERE
	er.session_id <> @@SPID
	--AND es.[status] = N'running'
ORDER BY
	[Threads] DESC,
	[Session Total CPU (ms)] DESC,
	--[Duration] DESC,
	[SPID] ASC
OPTION (RECOMPILE);