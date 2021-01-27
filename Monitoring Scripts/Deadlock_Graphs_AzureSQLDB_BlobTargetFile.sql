-- Based on script by Rajasekhar Reddy Bolla
-- Use this script to investigate deadlocks on an Azure SQL DB or Azure SQL Managed Instance

SELECT target_data_XML.value('(/event/@timestamp)[1]', 'DateTime2') AS [timestamp],  
target_data_XML.query('/event/data[@name=''xml_report'']/value/deadlock') AS deadlock_xml,  
target_data_XML.query('/event/data[@name=''database_name'']/value').value('(/value)[1]', 'nvarchar(100)') AS db_name  
FROM (SELECT CAST(event_data AS XML)  AS [target_data_XML]   
	FROM sys.fn_xe_telemetry_blob_target_read_file('dl', null, null, null)  
	) as Data
where target_data_XML.query('/event/data[@name=''xml_report'']/value/deadlock') is not null
--and target_data_XML.value('(/event/@timestamp)[1]', 'DateTime2') BETWEEN '2021-01-26 13:00' AND '2021-01-26 18:15'
--and target_data_XML.query('/event/data[@name=''xml_report'']/value/deadlock') like '%Employee%'
order by [timestamp] desc