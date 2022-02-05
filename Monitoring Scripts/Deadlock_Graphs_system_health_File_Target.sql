DECLARE @FileName NVARCHAR(250)
  
select @FileName = REPLACE(c.column_value, '.xel', '*.xel')
from sys.dm_xe_sessions s
JOIN sys.dm_xe_session_object_columns c
ON s.address =c.event_session_address
WHERE column_name = 'filename'
AND s.name = 'system_health'

SELECT
  CAST (event_data AS XML).value('(event/@timestamp)[1]','DATETIME') AS event_timestamp
, CAST (event_data AS XML).query('(event/data[@name="xml_report"]/value/deadlock)[1]') AS deadlock_graph
FROM sys.fn_xe_file_target_read_file (@FileName,null,null, null)
WHERE object_name = 'xml_deadlock_report'
ORDER BY 1 DESC