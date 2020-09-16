/*
Analyze SQLDiag extended event files
=====================================
Author: Eitan Blumin , Madeira Data Solutions (https://www.madeiradata.com | https://www.eitanblumin.com)
Create Date: 2020-09-15
Description:
	This T-SQL script focuses on analyzing the "query_processing" components of SQLDiag files.
	This can be useful for investigating Deadlocked Scheduler incidents.
*/
DECLARE
	@FileTargetPath NVARCHAR(256) = '*_SQLDIAG_*.xel',
	@LocalTimeZone VARCHAR(50) = 'Israel Standard Time',
	@Top INT = 1000

SELECT TOP (@Top)
  event_data_xml
, timestamp_local	= timestamp_utc AT TIME ZONE 'UTC' AT TIME ZONE @LocalTimeZone
, [object_name]
, component		= event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)')
, maxWorkers		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@maxWorkers)[1]', 'int')
, workersCreated	= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@workersCreated)[1]', 'int')
, workersIdle		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@workersIdle)[1]', 'int')
, pendingTasks		= event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@pendingTasks)[1]', 'int')
, hasUnresolvableDeadlockOccurred = event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@hasUnresolvableDeadlockOccurred)[1]', 'int')
, hasDeadlockedSchedulersOccurred = event_data_xml.value('(event/data[@name="data"]/value/queryProcessing/@hasDeadlockedSchedulersOccurred)[1]', 'int')
, blockedProcesses = event_data_xml.query('<blocked>{*//blocked-process-report/blocked-process/process/inputbuf}</blocked>')
, blockingProcesses = event_data_xml.query('<blocking>{*//blocked-process-report/blocking-process/process/inputbuf}</blocking>')
, [object_name], [file_name], file_offset, timestamp_utc
FROM sys.fn_xe_file_target_read_file(@FileTargetPath, default, null, null) AS tr
CROSS APPLY (SELECT event_data_xml = TRY_CONVERT(xml, event_data)) AS e
WHERE [object_name] <> 'component_health_result' OR
event_data_xml.value('(event/data[@name="component"])[1]', 'varchar(256)') = 'query_processing'
ORDER BY timestamp_utc DESC
