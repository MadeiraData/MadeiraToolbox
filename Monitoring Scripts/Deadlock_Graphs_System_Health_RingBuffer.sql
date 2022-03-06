
select TOP(10)
 event_timestamp
,event_data.query('data/value/deadlock') as deadlock_graph
,event_data.value('(data/value/deadlock/process-list/process/executionStack/frame/@procname)[1]','SYSNAME') AS deadlock_procedure
,event_data.query('(data/value/deadlock/victim-list)[1]') AS deadlock_victim
,event_data.query('(data/value/deadlock/process-list)[1]') AS deadlock_processes_all
,event_data.query('(data/value/deadlock/resource-list)[1]') AS deadlock_resources_all
FROM
(select CAST(target_data as xml) as TargetData, CAST(target_data as xml).value('(event/@timestamp)[1]','DATETIME') as event_timestamp
from sys.dm_xe_session_targets st
join sys.dm_xe_sessions s on s.address = st.event_session_address
where name = 'system_health') AS Data
CROSS APPLY TargetData.nodes ('//RingBufferTarget/event') AS XEventData (event_data)
where event_data.value('@name', 'varchar(4000)') = 'xml_deadlock_report'
ORDER BY 1 DESC