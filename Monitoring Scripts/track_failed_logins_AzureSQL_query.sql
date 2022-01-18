-- Retrieve buffer contents
IF OBJECT_ID('tempdb..#events') IS NOT NULL DROP TABLE #events
CREATE TABLE #events (event_xml XML);
INSERT INTO #events
SELECT X.query('.')
FROM (SELECT xdata = CAST(xet.target_data AS xml)
FROM sys.dm_xe_database_session_targets AS xet  
JOIN sys.dm_xe_database_sessions AS xe ON xe.address = xet.event_session_address
WHERE xe.name = 'TrackFailedLogins'
AND target_name= 'ring_buffer'
) AS a
CROSS APPLY xdata.nodes (N'//event') AS session_events (X)

-- Unfurl raw data
SELECT
[server_name]		= @@SERVERNAME,
[database_name]		= session_events.event_xml.value (N'(event/action[@name="database_name"]/value)[1]' , N'SYSNAME') ,
event_name		= session_events.event_xml.value (N'(event/@name)[1]' , N'NVARCHAR(1000)') ,
event_timestamp_utc	= session_events.event_xml.value (N'(event/@timestamp)[1]' , N'DATETIME2(7)') ,
session_id		= session_events.event_xml.value (N'(event/action[@name="session_id"]/value)[1]' , N'INT') ,
error_number		= session_events.event_xml.value (N'(event/data[@name="error_number"]/value)[1]' , N'INT') ,
severity		= session_events.event_xml.value (N'(event/data[@name="severity"]/value)[1]' , N'INT') ,
state			= session_events.event_xml.value (N'(event/data[@name="state"]/value)[1]' , N'INT') ,
category		= session_events.event_xml.value (N'(event/data[@name="category"]/value)[1]' , N'INT') ,
category_desc		= session_events.event_xml.value (N'(event/data[@name="category"]/text)[1]' , N'NVARCHAR(MAX)') ,
message			= session_events.event_xml.value (N'(event/data[@name="message"]/value)[1]' , N'NVARCHAR(MAX)') ,
client_app_name		= session_events.event_xml.value (N'(event/action[@name="client_app_name"]/value)[1]' , N'NVARCHAR(1000)') ,
client_host_name	= session_events.event_xml.value (N'(event/action[@name="client_hostname"]/value)[1]' , N'NVARCHAR(1000)') ,
client_process_id	= session_events.event_xml.value (N'(event/action[@name="client_pid"]/value)[1]' , N'BIGINT') ,
username		= session_events.event_xml.value (N'(event/action[@name="username"]/value)[1]' , N'SYSNAME') ,
sql_text		= session_events.event_xml.value (N'(event/action[@name="sql_text"]/value)[1]' , N'NVARCHAR(MAX)')
,event_xml
FROM #events AS session_events;
