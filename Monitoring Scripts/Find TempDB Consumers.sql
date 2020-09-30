/*
TempDB
http://www.madeiradata.com/troubleshooting-tempdb-space-usage/

Object Types That can use tempdb space-

1. User Objects- temp table, table variable
2. Internal Objects - query intermidiate results for hash operation
3. Version Stores - like snapshot isolation,row version


--find internal objects--
currently running large query that consumes a lot of space in tempdb due to internal objects, 
you can use the following query in order to return the batch text and execution plan currently performed by the offending 
*/
SELECT
	SessionId			= TasksSpaceUsage.SessionId ,
	RequestId			= TasksSpaceUsage.RequestId ,
	IsUserProcess			= Sessions.is_user_process ,
	InternalObjectsAllocPageCount	= TasksSpaceUsage.InternalObjectsAllocPageCount ,
	InternalObjectsDeallocPageCount	= TasksSpaceUsage.InternalObjectsDeallocPageCount ,
	UserObjectsAllocPageCount	= TasksSpaceUsage.UserObjectsAllocPageCount ,
	UserObjectsDeallocPageCount	= TasksSpaceUsage.UserObjectsDeallocPageCount ,
	RequestBatchText		= RequestsText.text ,
	RequestStatement		=
					ISNULL(
						NULLIF(
						SUBSTRING(
							RequestsText.text, 
							Requests.statement_start_offset / 2, 
							CASE WHEN Requests.statement_end_offset < Requests.statement_start_offset 
							THEN 0 
							ELSE( Requests.statement_end_offset - Requests.statement_start_offset ) / 2 END
						), ''
						), RequestsText.text
					) ,
	RequestPlan			= RequestsPlan.query_plan ,
	ClientHostName			= Sessions.host_name ,
	ClientProgram			= Sessions.program_name ,
	LoginName			= Sessions.login_name ,
	ClientProcessID			= Sessions.host_process_id
FROM
	(
		SELECT
			SessionId			= session_id ,
			RequestId			= request_id ,
			InternalObjectsAllocPageCount	= SUM (internal_objects_alloc_page_count) ,
			InternalObjectsDeallocPageCount	= SUM (internal_objects_dealloc_page_count) ,
			UserObjectsAllocPageCount	= SUM (user_objects_alloc_page_count) ,
			UserObjectsDeallocPageCount	= SUM (user_objects_dealloc_page_count)
		FROM
			sys.dm_db_task_space_usage AS ts
		GROUP BY
			session_id ,
			request_id
	)
	AS
		TasksSpaceUsage
INNER JOIN
	sys.dm_exec_sessions AS Sessions
ON
	TasksSpaceUsage.SessionId = Sessions.session_id
LEFT JOIN
	sys.dm_exec_requests AS Requests
ON
	TasksSpaceUsage.SessionId = Requests.session_id
AND
	TasksSpaceUsage.RequestId = Requests.request_id
OUTER APPLY
	sys.dm_exec_sql_text (Requests.sql_handle) AS RequestsText
OUTER APPLY
	sys.dm_exec_query_plan (Requests.plan_handle) AS RequestsPlan
WHERE
	TasksSpaceUsage.InternalObjectsAllocPageCount > 0 OR
	TasksSpaceUsage.InternalObjectsDeallocPageCount > 0 OR
	TasksSpaceUsage.UserObjectsAllocPageCount > 0 OR
	TasksSpaceUsage.UserObjectsDeallocPageCount > 0
ORDER BY
	SessionId	ASC ,
	RequestId	ASC;
