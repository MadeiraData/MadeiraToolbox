-- Based on script by Rajasekhar Reddy Bolla

SELECT target_data_XML.value('(/event/@timestamp)[1]', 'DateTime2') AS [timestamp],  
target_data_XML.query('/event/data[@name=''xml_report'']/value/deadlock') AS deadlock_xml,  
target_data_XML.query('/event/data[@name=''database_name'']/value').value('(/value)[1]', 'nvarchar(100)') AS db_name  
FROM (SELECT CAST(event_data AS XML)  AS [target_data_XML]   
	FROM sys.fn_xe_telemetry_blob_target_read_file('dl', null, null, null)  
	) as Data
where deadlock_xml is not null
--and target_data_XML.value('(/event/@timestamp)[1]', 'DateTime2')>='2020-03-01'
--and target_data_XML.query('/event/data[@name=''xml_report'']/value/deadlock') like '%Employee%'
order by [timestamp] desc