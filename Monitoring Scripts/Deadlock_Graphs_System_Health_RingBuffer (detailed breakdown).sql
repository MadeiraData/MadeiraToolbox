-- =============================================
-- Author:		Rotem Meidan and Eitan Blumin
-- Create date: 2022
-- Description:	This parses the system_health extended events ring buffer and searches for deadlocks.
--		It then extracts all the data you need to understand the deadlock in a nice little table. 
--		Not all the columns are presented in the final select. Feel free to add more as you see fit. 		 
-- ===============

-- Creates the temp table if it doesn't exists and selects the number of deadlocks in the buffer.
IF OBJECT_ID('tempdb..#XMLDATA') IS NOT NULL DROP TABLE #XMLDATA

SELECT event_data.query('.') AS event_data
, event_data.value('@timestamp','datetime') AS deadlock_time
INTO #XMLDATA
FROM
(select CAST(target_data as xml) as TargetData
from sys.dm_xe_session_targets st
join sys.dm_xe_sessions s on s.address = st.event_session_address
where name = 'system_health') AS Data
CROSS APPLY TargetData.nodes ('//RingBufferTarget/event') AS XEventData (event_data)
where event_data.value('@name', 'varchar(4000)') = 'xml_deadlock_report'

SELECT @@ROWCOUNT AS DeadlockCountInRingBuffer

CREATE CLUSTERED INDEX IX ON #XMLDATA (deadlock_time)
--WITH (DATA_COMPRESSION = PAGE);


-- Selects important data for each deadlock

; WITH AllDeadlocks AS (

SELECT	
		ROW_NUMBER() OVER ( ORDER BY event_data.value('(event/@timestamp)[1]','DATETIME')) AS id,
		deadlock_time,
		--event_data.value('(event/@timestamp)[1]','DATETIME') AS deadlock_time,
		event_data.value('(event/data/value/deadlock/process-list/process/executionStack/frame/@procname)[1]','SYSNAME') AS deadlock_procedure,
		event_data.query('(event/data[@name="xml_report"]/value/deadlock)[1]') AS deadlock_graph,
		event_data.query('(event/data/value/deadlock/victim-list)[1]') AS deadlock_victim,
		event_data.query('(event/data/value/deadlock/process-list)[1]') AS deadlock_processes_all,
		event_data.query('(event/data/value/deadlock/resource-list)[1]') AS deadlock_resources_all

FROM #XMLDATA
-- ignore deadlocks without victims (yes, that's a thing. I was surprised too)
WHERE event_data.exist('event/data/value/deadlock/victim-list/victimProcess') = 1

), resources AS(
 
SELECT id, deadlock_time, AllDeadlocks.deadlock_graph,
	d.deadlock_resources.value('@objectname','NVARCHAR(200)') AS resource_objectname,
	d.deadlock_resources.value('@dbid','INT') AS resource_dbid,
	d.deadlock_resources.value('@mode','NVARCHAR(10)') AS resource_mode,
	d.deadlock_resources.value('(owner-list/owner/@id)[1]','NVARCHAR(20)') AS resource_owner_id,	
	d.deadlock_resources.value('(owner-list/owner/@mode)[1]','NVARCHAR(10)') AS resource_owner_mode,
	d.deadlock_resources.value('(owner-list/owner/@requestType)[1]','NVARCHAR(20)') AS resource_owner_requestType,	
	d.deadlock_resources.value('(waiter-list/waiter/@id)[1]','NVARCHAR(20)') AS resource_waiter_id,		
	d.deadlock_resources.value('(waiter-list/waiter/@mode)[1]','NVARCHAR(20)') AS resource_waiter_mode,		
	d.deadlock_resources.value('(waiter-list/waiter/@requestType)[1]','NVARCHAR(20)') AS resource_waiter_requestType,
	d.deadlock_resources.value('@fileid','INT') AS resource_fileid,
	d.deadlock_resources.value('@pageid','INT') AS resource_pageid,
	d.deadlock_resources.value('@hobtid','BIGINT') AS resource_keyid,
	d.deadlock_resources.value('@WaitType','NVARCHAR(50)') AS resource_WaitType,
	d.deadlock_resources.value ('@lockPartition','INT') AS resource_lockPartition,
	d.deadlock_resources.value ('@objid','INT') AS resource_objid,	
	d.deadlock_resources.value ('@subresource','NVARCHAR(10)') AS resource_subresource	
FROM AllDeadlocks
CROSS APPLY  deadlock_resources_all.nodes('/resource-list/ridlock,/resource-list/objectlock,/resource-list/pagelock,/resource-list/keylock,/resource-list/exchangeEvent')  AS d(deadlock_resources)

), Victims AS (

SELECT 
	deadlock_victim.value('(victim-list/victimProcess/@id)[1]','NVARCHAR(20)') AS deadlock_victim,
	deadlock_time
FROM
	AllDeadlocks

), Processes AS (

SELECT id, a.deadlock_time,
	CASE 
	WHEN v.deadlock_victim  IS NOT NULL THEN 'Victim'
	ELSE 'Killer' 
	END AS deadlock_role,
	d.deadlock_processes.value('@id','NVARCHAR(20)') AS process_id,	
	d.deadlock_processes.value('(executionStack/frame/@procname)[1]','NVARCHAR(200)') AS process_procname,
	d.deadlock_processes.value('(executionStack/frame)[1]','NVARCHAR(200)') AS process_sqltext,
	d.deadlock_processes.value('@waitresource','NVARCHAR(20)') AS process_waitresource,	
	d.deadlock_processes.value('@waittime','INT') AS process_waittime,
	d.deadlock_processes.value('@spid','INT') AS process_sessionid,
	d.deadlock_processes.value('@sbid','INT') AS process_requestid,
	d.deadlock_processes.value('@ecid','INT') AS process_workerthread,
	d.deadlock_processes.value('@lasttranstarted','DATETIME') AS process_lasttranstarted,	
	d.deadlock_processes.value('@lockMode','NVARCHAR(5)') AS process_lockMode,	
	d.deadlock_processes.value('@status','NVARCHAR(20)') AS process_status,	
	d.deadlock_processes.value('@lastbatchstarted','DATETIME') AS process_lastbatchstarted,	
	d.deadlock_processes.value('@lastbatchcompleted','DATETIME') AS process_lastbatchcompleted,	
	d.deadlock_processes.value('@clientapp','NVARCHAR(50)') AS process_clientapp,	
	d.deadlock_processes.value('@hostname','NVARCHAR(50)') AS process_hostname,	
	d.deadlock_processes.value('@loginname','NVARCHAR(50)') AS process_loginname,	
	d.deadlock_processes.value('@isolationlevel','NVARCHAR(50)') AS process_isolationlevel
FROM AllDeadlocks a
CROSS APPLY  deadlock_processes_all.nodes('/process-list/process') AS d(deadlock_processes)
LEFT JOIN Victims v
ON (d.deadlock_processes.value('@id','NVARCHAR(20)')) = v.deadlock_victim
AND a.deadlock_time = v.deadlock_time
)

SELECT r.deadlock_time,
r.deadlock_graph,
p.process_id,
CASE WHEN v.deadlock_victim IS NULL THEN 'No' ELSE 'Yes' END AS is_Victim,r.resource_objectname,r.resource_owner_id, r.resource_owner_mode, 
ISNULL(r.resource_owner_requestType,'accepted') AS resource_owner_requestType,
r.resource_waiter_id, r.resource_waiter_mode,r.resource_waiter_requestType,
r.resource_fileid, r.resource_pageid, r.resource_keyid, r.resource_WaitType,
r.resource_lockPartition, r.resource_subresource, r.resource_objid,
p.process_sessionid AS owner_process_sessionid, p.process_requestid AS owner_process_requestid, p.process_workerthread AS owner_process_workerthred,
p.process_procname,p.process_sqltext,
p.process_clientapp AS owner_process_clientapp, p.process_hostname AS owner_process_hostname,
p.process_loginname as owner_process_loginname,
p.process_isolationlevel AS owner_process_isolationlevel
FROM resources r
JOIN Processes p ON r.resource_owner_id = p.process_id AND r.deadlock_time = p.deadlock_time
LEFT JOIN Victims v ON p.process_id = v.deadlock_victim AND p.deadlock_time = v.deadlock_time
--WHERE r.deadlock_time BETWEEN DATEADD(dd,-1,GETUTCDATE()) AND GETUTCDATE()