-- Deadlock details for Azure SQL
-- Author: Eitan Blumin
-- Based on script by Rajasekhar Reddy Bolla
-- Use this script to investigate deadlocks on an Azure SQL DB or Azure SQL Managed Instance

SELECT TOP (50) timestamp_utc
,event_data.query('/event/data[@name=''xml_report'']/value/deadlock') AS deadlock_xml
,victim_process_xml.query('.') AS victim_process_xml
,event_data.value('(/event/data[@name=''database_name'']/value/text())[1]','nvarchar(256)') AS databaseName
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
	SELECT CAST(event_data AS xml)  AS event_data, timestamp_utc
	FROM sys.fn_xe_telemetry_blob_target_read_file('dl', NULL, NULL, NULL)
) AS DLevents
CROSS APPLY
(SELECT victim_process_xml = event_data.query('
for $victimId in distinct-values(/event/data[@name=''xml_report'']/value/deadlock/victim-list/victimProcess/@id)
	return /event/data[@name=''xml_report'']/value/deadlock/process-list/process[@id = $victimId]
')
) AS victimProcess
WHERE event_data.exist('/event/data[@name=''xml_report'']/value/deadlock') = 1
--AND convert(datetime2, timestamp_utc) > DATEADD(hour, -3, GETUTCDATE()) -- direct filtering on the column doesn't work for some reason
--AND victim_process_xml.value('(process/@clientapp)[1]','nvarchar(256)') not like N'SentryOne%'
--AND victim_process_xml.value('(process/inputbuf/text())[1]','nvarchar(max)') like N'%Employee%'
ORDER BY timestamp_utc DESC