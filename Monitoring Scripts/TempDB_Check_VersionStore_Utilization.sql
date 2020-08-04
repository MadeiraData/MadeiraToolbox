SELECT ses.session_id, 
	db_name(spu.database_id) as spaceusage_database_name,
	db_name(ses.database_id) as context_database_name,
       at.transaction_begin_time as begin_time,
	   t.text as most_recent_sql_text,
       case 
         when at.transaction_state in (0,1) then 'init'
         when at.transaction_state = 2 then 'active'
         when at.transaction_state = 3 then 'ended'
         when at.transaction_state = 4 then 'committing'
         when at.transaction_state = 6 then 'comitted'
         when at.transaction_state = 7 then 'rolling back'
         when at.transaction_state = 6 then 'rolled back'
         else 'other'
       end as transaction_state,
       ast.elapsed_time_seconds as elapsed_seconds,
       ses.program_name, 
       ses.row_count,
       (spu.user_objects_alloc_page_count * 8) AS user_objects_kb,
       (spu.user_objects_dealloc_page_count * 8) AS user_objects_deallocated_kb,
       (spu.internal_objects_alloc_page_count * 8) AS internal_objects_kb,
       (spu.internal_objects_dealloc_page_count * 8) AS internal_objects_deallocated_kb
FROM sys.dm_tran_active_snapshot_database_transactions ast
  JOIN sys.dm_tran_active_transactions at on at.transaction_id = ast.transaction_id
  JOIN sys.dm_exec_sessions ses ON ses.session_id = ast.session_id
  JOIN sys.dm_db_session_space_usage spu ON spu.session_id = ses.session_id
  LEFT JOIN sys.dm_exec_connections con ON ses.session_id = con.session_id
  OUTER APPLY sys.dm_exec_sql_text(con.most_recent_sql_handle) as t
ORDER BY elapsed_time_seconds DESC
