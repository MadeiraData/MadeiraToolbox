/*
-----------------
--- "SP_WHO3" ---
-----------------
Author:		Eitan Blumin (Madeira Data Solutions)
Date:		2015-05-27

Adapted from this source:
http://mssqlfun.com/2013/04/01/dmv-3-what-is-currently-going-on-sys-dm_exec_requests-2/
*/

SELECT
	s.session_id,
	s.status,
	s.host_name,
	s.host_process_id,
	c.client_net_address,
	login_name			= CASE
								WHEN s.login_name = s.original_login_name THEN s.login_name 
								ELSE s.login_name + ' (' + s.original_login_name + ')' 
						  END,
	s.program_name,
	database_name		= DB_NAME(ISNULL(r.database_id, s.database_id)),
	r.command,
	running_statement	= SUBSTRING(st.text, r.statement_start_offset/2+1, ((CASE WHEN r.statement_end_offset = -1 THEN DATALENGTH(st.text) ELSE r.statement_end_offset END - r.statement_start_offset)/2) + 1),
	query_text			= ISNULL(st.text, ib.event_info),
	xml_query_plan		= qp.query_plan,
	current_wait_type	= r.wait_type,
	r.last_wait_type,
	r.blocking_session_id,
	r.row_count,
	r.granted_query_memory,
	r.open_transaction_count,
	r.user_id,
	r.percent_complete,
	transaction_isolation_level_name	= CASE r.transaction_isolation_level
											WHEN 0 THEN 'unspecified'
											WHEN 1 THEN 'readuncomitted'
											WHEN 2 THEN 'readcommitted'
											WHEN 3 THEN 'repeatable'
											WHEN 4 THEN 'serializable'
											WHEN 5 THEN 'snapshot'
											ELSE CAST(r.transaction_isolation_level AS VARCHAR(32))
										END
FROM
	sys.dm_exec_sessions s
INNER JOIN
	sys.dm_exec_connections c 
	ON
		c.session_id = s.session_id
LEFT OUTER JOIN
	sys.dm_exec_requests r
	ON
		s.session_id = r.session_id 
OUTER APPLY
	sys.dm_exec_sql_text(r.sql_handle) st
OUTER APPLY
	sys.dm_exec_query_plan(r.plan_handle) qp
OUTER APPLY
	sys.dm_exec_input_buffer(s.session_id, r.request_id) ib
WHERE
	s.session_id <> @@SPID
