/*========================================================================================================================

Description:	Display information about head blockers in blocking chains
Scope:			Instance
Author:			Guy Glantser
Created:		02/09/2020
Last Updated:	24/03/2024
Notes:			Displays information about the head blocker at the task level.

=========================================================================================================================*/

WITH
	BlockingChains
(
	SessionId ,
	TaskAddress ,
	WaitTime_Milliseconds ,
	ChainLevel ,
	HeadBlockerSessionId ,
	HeadBlockerTaskAddress
)
AS
(
	SELECT
		SessionId				= [Sessions].session_id ,
		TaskAddress				= Tasks.task_address ,
		WaitTime_Milliseconds	= CAST (NULL AS BIGINT) ,
		ChainLevel				= CAST (0 AS INT) ,
		HeadBlockerSessionId	= [Sessions].session_id ,
		HeadBlockerTaskAddress	= Tasks.task_address
	FROM
		sys.dm_exec_sessions AS [Sessions]
	LEFT OUTER JOIN
		sys.dm_exec_requests AS Requests
	ON
		[Sessions].session_id = Requests.session_id
	LEFT OUTER JOIN
		sys.dm_os_tasks AS Tasks
	ON
		Requests.session_id = Tasks.session_id
	WHERE
		Requests.blocking_session_id IS NULL
	OR
		Requests.blocking_session_id = 0
	
	UNION ALL

	SELECT
		SessionId				= BlockedTasks.session_id ,
		TaskAddress				= BlockedTasks.waiting_task_address ,
		WaitTime_Milliseconds	= BlockedTasks.wait_duration_ms ,
		ChainLevel				= BlockingChains.ChainLevel + 1 ,
		HeadBlockerSessionId	= BlockingChains.HeadBlockerSessionId ,
		HeadBlockerTaskAddress	= BlockingChains.HeadBlockerTaskAddress
	FROM
		BlockingChains
	INNER JOIN
		sys.dm_os_waiting_tasks AS BlockedTasks
	ON
		BlockingChains.SessionId = BlockedTasks.blocking_session_id
	AND
		BlockedTasks.session_id <> BlockedTasks.blocking_session_id
	AND
	(
		BlockingChains.TaskAddress = BlockedTasks.blocking_task_address
	OR
		BlockingChains.TaskAddress IS NULL AND BlockedTasks.blocking_task_address IS NULL
	)
) ,

	HeadBlockers
(
	HeadBlockerSessionId ,
	HeadBlockerTaskAddress ,
	BlockingChainLength ,
	NumberOfBlockedTasks ,
	NumberOfBlockedSessions ,
	TotalBlockedTasksWaitTime_Milliseconds
)
AS
(
	SELECT
		HeadBlockerSessionId					= HeadBlockerSessionId ,
		HeadBlockerTaskAddress					= HeadBlockerTaskAddress ,
		BlockingChainLength						= MAX (ChainLevel) + 1 ,
		NumberOfBlockedTasks					= COUNT (*) - 1 ,
		NumberOfBlockedSessions					=
			COUNT
			(
				DISTINCT
					CASE
						WHEN SessionId = HeadBlockerSessionId
							THEN NULL
						ELSE
							SessionId
					END
			) ,
		TotalBlockedTasksWaitTime_Milliseconds	= SUM (WaitTime_Milliseconds)
	FROM
		BlockingChains
	GROUP BY
		HeadBlockerSessionId ,
		HeadBlockerTaskAddress
	HAVING
		MAX (ChainLevel) > 0
)

SELECT
	HeadBlockerSessionId					= HeadBlockers.HeadBlockerSessionId ,
	HeadBlockerTaskAddress					= HeadBlockers.HeadBlockerTaskAddress ,
	BlockingChainLength						= HeadBlockers.BlockingChainLength ,
	NumberOfBlockedTasks					= HeadBlockers.NumberOfBlockedTasks ,
	NumberOfBlockedSessions					= HeadBlockers.NumberOfBlockedSessions ,
	TotalBlockedTasksWaitTime_Milliseconds	= HeadBlockers.TotalBlockedTasksWaitTime_Milliseconds ,
	LoginDateTime							= [Sessions].login_time ,
	HostName								= [Sessions].[host_name] ,
	ProgramName								= [Sessions].[program_name] ,
	LoginName								= [Sessions].login_name ,
	SessionStatus							= [Sessions].[status] ,
	LastRequestStartDateTime				= [Sessions].last_request_start_time ,
	LastRequestEndDateTime					=
		CASE
			WHEN Requests.session_id IS NULL
				THEN [Sessions].last_request_end_time
			ELSE
				NULL
		END ,
	DatabaseName							=
		CASE
			WHEN [Sessions].database_id = 0
				THEN N'N/A'
			ELSE
				DB_NAME ([Sessions].database_id)
		END ,
	OpenTransactionCount					= [Sessions].open_transaction_count ,
	MostRecentBatchText						= MostRecentBatchTexts.[text] ,
	ActiveRequestId							= Requests.request_id ,
	ActiveRequestStatus						= Requests.[status] ,
	ActiveRequestCommand					= Requests.command ,
	ActiveRequestStatementText				=
		SUBSTRING
		(
			RequestBatchTexts.[text] ,
			Requests.statement_start_offset / 2 + 1 ,
			(
				(
					CASE
						WHEN Requests.statement_end_offset = -1
							THEN DATALENGTH (RequestBatchTexts.[text])
						ELSE
							Requests.statement_end_offset
					END
					- Requests.statement_start_offset
				) / 2
			) + 1
		) ,
	ActiveRequestStatementPlan				= CAST (RequestStatementPlans.query_plan AS XML) ,
	ActiveRequestWaitType					= Requests.wait_type ,
	ActiveRequestWaitTime_Milliseconds		= Requests.wait_time ,
	ActiveRequestLastWaitType				= Requests.last_wait_type ,
	ActiveRequestPercentComplete			= Requests.percent_complete ,
	ActiveRequestCPUTime_Milliseconds		= Requests.cpu_time ,
	ActiveRequestElapsedTime_Milliseconds	= Requests.total_elapsed_time ,
	ActiveRequestReads						= Requests.reads ,
	ActiveRequestWrites						= Requests.writes ,
	ActiveRequestLogicalReads				= Requests.logical_reads ,
	ActiveRequestDegreeOfParallelism		= Requests.dop , -- supported only in SQL2016 and newer. remove if using an older version.
	ActiveTransactionName					= ActiveTransactions.[name] ,
	ActiveTransactionBeginDateTime			= ActiveTransactions.transaction_begin_time
FROM
	HeadBlockers
INNER JOIN
	sys.dm_exec_sessions AS [Sessions]
ON
	HeadBlockers.HeadBlockerSessionId = [Sessions].session_id
LEFT OUTER JOIN
	sys.dm_exec_connections AS Connections
ON
	[Sessions].session_id = Connections.session_id
OUTER APPLY
	sys.dm_exec_sql_text (Connections.most_recent_sql_handle) AS MostRecentBatchTexts
LEFT OUTER JOIN
	sys.dm_exec_requests AS Requests
ON
	[Sessions].session_id = Requests.session_id
OUTER APPLY
	sys.dm_exec_sql_text (Requests.[sql_handle]) AS RequestBatchTexts
OUTER APPLY
	sys.dm_exec_text_query_plan (Requests.plan_handle , Requests.statement_start_offset , Requests.statement_end_offset) AS RequestStatementPlans
LEFT OUTER JOIN
	sys.dm_tran_session_transactions AS SessionTransactions
ON
	[Sessions].session_id = SessionTransactions.session_id
LEFT OUTER JOIN
	sys.dm_tran_active_transactions AS ActiveTransactions
ON
	SessionTransactions.transaction_id = ActiveTransactions.transaction_id
ORDER BY
	TotalBlockedTasksWaitTime_Milliseconds	DESC ,
	HeadBlockerSessionId					ASC ,
	HeadBlockerTaskAddress					ASC;
GO