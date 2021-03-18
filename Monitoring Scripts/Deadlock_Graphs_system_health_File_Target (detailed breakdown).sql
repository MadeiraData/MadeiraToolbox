


-- =============================================
-- Author:		Rotem Meidan
-- Create date: 2015
-- Description:	This parses the system_health extended events file and searches for deadlocks.
--		It then extracts all the data you need to understand the deadlock in a nice little table. 
--		Not all the columns are presented in the final select. Feel free to add more as you see fit. 		 
-- ===============

-- Parameters to change when running (explanations are up in the "Instructions" )
DECLARE @FileName NVARCHAR(MAX)

select @FileName = REPLACE(c.column_value, '.xel', '*.xel')
from sys.dm_xe_sessions s
JOIN sys.dm_xe_session_object_columns c
ON s.address =c.event_session_address
WHERE column_name = 'filename'
AND s.name = 'system_health'

-- Creates the temp table if it doesn't exists and selects the number of deadlocks in the file.
IF OBJECT_ID('tempdb..#XMLDATA') IS NOT NULL DROP TABLE #XMLDATA

SELECT CAST (event_data AS XML) AS event_data
INTO #XMLDATA
FROM    sys.fn_xe_file_target_read_file
	(@FileName,null,null, null)
WHERE object_name = 'xml_deadlock_report'

SELECT @@ROWCOUNT AS DeadlockCountInFile




-- Selects important data for each deadlock

; WITH AllDeadlocks AS (

SELECT	
		ROW_NUMBER() OVER ( ORDER BY event_data.value('(event/@timestamp)[1]','DATETIME')) AS ID,
		event_data.value('(event/@timestamp)[1]','DATETIME') AS deadlock_time,
		event_data.value('(event/data/value/deadlock/process-list/process/executionStack/frame/@procname)[1]','SYSNAME') AS deadlock_procedure,
		event_data.query('(event/data[@name="xml_report"]/value/deadlock)[1]') AS deadlock_graph,
		event_data.query('(event/data/value/deadlock/victim-list)[1]') AS deadlock_victim,
		event_data.query('(event/data/value/deadlock/process-list)[1]') AS deadlock_processes_all,
		event_data.query('(event/data/value/deadlock/resource-list)[1]') AS deadlock_resources_all

FROM #XMLDATA

),Resources AS(
 
SELECT id,deadlock_time,
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
	deadlock_victim.value('(victim-list/victimProcess/@id)[1]','NVARCHAR(20)') AS deadlock_victim
FROM
	AllDeadlocks

), Processes AS (

SELECT id,
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
)

SELECT p.process_id, CASE WHEN v.deadlock_victim IS NULL THEN 'No' ELSE 'Yes' END AS is_Victim,r.resource_objectname,r.resource_owner_id, r.resource_owner_mode, 
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
JOIN Processes p
on
r.resource_owner_id = p.process_id
LEFT JOIN Victims v
on
p.process_id = v.deadlock_victim
