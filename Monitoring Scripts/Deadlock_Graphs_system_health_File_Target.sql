DECLARE @FileName NVARCHAR(250)
  
select @FileName = REPLACE(c.column_value, '.xel', '*.xel')
from sys.dm_xe_sessions s
JOIN sys.dm_xe_session_object_columns c
ON s.address =c.event_session_address
WHERE column_name = 'filename'
AND s.name = 'system_health'

SELECT TOP (1000)
 event_timestamp
,event_data.query('.').query('for $proc in distinct-values(deadlock/process-list/process/executionStack/frame/@procname)
	return concat($proc, " ; ")').value('(text())[1]','nvarchar(max)') AS deadlock_frames
,event_data.query('.').value('(deadlock/process-list/process/executionStack/frame/@procname)[1]','SYSNAME') AS deadlock_procedure
,event_data.query('.') AS deadlock_graph
,victim_process_xml.query('.') AS victim_process_xml
,COALESCE(
  event_data.value('(/event/data[@name=''database_name'']/value/text())[1]','nvarchar(256)')
, victim_process_xml.value('(process/@currentdbname)[1]','nvarchar(256)')
, DB_NAME(victim_process_xml.value('(process/@currentdb)[1]','int')))
AS databaseName
--,victim_process_xml.value('(process/@currentdbname)[1]','nvarchar(256)') AS victimDatabase
,victim_process_xml.value('(process/@spid)[1]','int') AS victimSPID
,victim_process_xml.value('(process/@clientapp)[1]','nvarchar(256)') AS victimClientApp
,victim_process_xml.value('(process/@hostname)[1]','nvarchar(256)') AS victimClientHostname
,victim_process_xml.value('(process/@isolationlevel)[1]','nvarchar(256)') AS victimIsolationLevel
,victim_process_xml.value('(process/@loginname)[1]','nvarchar(256)') AS victimLoginName
,victim_process_xml.value('(process/@hostpid)[1]','int') AS victimHostPID
,victim_process_xml.value('(process/executionStack/frame/text())[1]','nvarchar(max)') AS victimSqlStatement
,victim_process_xml.value('(process/inputbuf/text())[1]','nvarchar(max)') AS victimInputBuf
FROM
(
SELECT
  CAST (event_data AS XML).value('(event/@timestamp)[1]','DATETIME') AS event_timestamp
, CAST (event_data AS XML).query('(event/data[@name="xml_report"]/value/deadlock)[1]') AS event_data
, CAST (event_data AS XML) AS event_data_raw
FROM sys.fn_xe_file_target_read_file (@FileName,null,null, null)
WHERE object_name = 'xml_deadlock_report'
) AS d
CROSS APPLY
(SELECT victim_process_xml = event_data_raw.query('
for $victimId in distinct-values(/event/data[@name=''xml_report'']/value/deadlock/victim-list/victimProcess/@id)
	return /event/data[@name=''xml_report'']/value/deadlock/process-list/process[@id = $victimId]
')
) AS victimProcess
WHERE victim_process_xml.exist('process/@spid') = 1 -- ignore deadlocks without victims (yes, that's a thing. I was surprised too)
ORDER BY event_timestamp DESC