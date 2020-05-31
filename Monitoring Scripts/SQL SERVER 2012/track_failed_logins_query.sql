
;WITH event_data AS 
(
SELECT data = CONVERT(XML, event_data)
FROM sys.fn_xe_file_target_read_file
('TrackFailedLogins*.xel', default, NULL, NULL)
),
tabular AS
(
SELECT 
 [timestamp] = data.value('(event/@timestamp)[1]','varchar(30)'),
 [client_hostname] = data.value('(event/action[@name="client_hostname"]/value)[1]','nvarchar(4000)'),
 [client_app_name] = data.value('(event/action[@name="client_app_name"]/value)[1]','nvarchar(4000)'),
 [nt_username] = data.value('(event/action[@name="nt_username"]/value)[1]','nvarchar(4000)'),
 [database_id] = data.value('(event/action[@name="database_id"]/value)[1]','int'),
 [database_name] = DB_NAME(data.value('(event/action[@name="database_id"]/value)[1]','int')),
 [session_id] = data.value('(event/action[@name="session_id"]/value)[1]','int'),
 [error_number] = data.value('(event/data[@name="error_number"]/value)[1]','int'),
 [severity] = data.value('(event/data[@name="severity"]/value)[1]','int'),
 [state] = data.value('(event/data[@name="state"]/value)[1]','tinyint'),
 [message] = data.value('(event/data[@name="message"]/value)[1]','nvarchar(250)'),
 [data] = data.query('.')
FROM event_data
)
SELECT *
FROM tabular AS t
ORDER BY [timestamp] DESC;