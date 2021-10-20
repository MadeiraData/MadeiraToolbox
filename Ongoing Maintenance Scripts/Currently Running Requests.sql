/*=======================================================================================
---------------------------- Currently Running Requests -------------------------
Written By: Sagi Amichai, Madeira Data Solutions
Date of Creation: Oct 2021
This query presents the currently active sessions (requests), with extanded information
regarding their status and activity.
With this information you will be abale to analyze what is happening in an instance right now.
=======================================================================================*/

SELECT 
	rq.session_id,
	rq.blocking_session_id,
	rq.status,
	SUBSTRING
	(
		t.text, 
		rq.statement_start_offset / 2 + 1, 
		CASE 
			WHEN rq.statement_end_offset > 0 
			THEN (rq.statement_end_offset - rq.statement_start_offset) / 2 + 1
			ELSE LEN(t.text) 
		END
	)			AS Executed_Text,	
	DB_NAME(rq.database_id) AS Database_Name,
	DATEDIFF(second, rq.start_time, getdate()) AS Duration_In_Sec,
	rq.start_time,
	es.last_request_end_time,
	es.cpu_time,
	rq.command,
	rq.logical_reads,
	rq.reads,
	rq.writes,
	rq.percent_complete,
	rq.wait_type,
	rq.last_wait_type,
	es.login_name,
	es.[program_name],	
	t.[text]	AS Full_Text	
FROM 
	sys.dm_exec_requests rq
INNER JOIN 
	sys.dm_exec_sessions es
ON 
	rq.session_id=es.session_id
CROSS APPLY 
	sys.dm_exec_sql_text(rq.sql_handle) t
WHERE 
	rq.session_id>50
ORDER BY session_id
