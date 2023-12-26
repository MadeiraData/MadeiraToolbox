CREATE EVENT SESSION [AuditLogins] ON SERVER 
ADD EVENT sqlserver.login(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.server_principal_name))
ADD TARGET package0.histogram(SET filtering_event_name=N'sqlserver.login',source=N'sqlserver.server_principal_name')
ADD TARGET package0.event_file(SET filename=N'AuditLogins')
WITH (STARTUP_STATE=ON)
GO

ALTER EVENT SESSION [AuditLogins]  
ON SERVER STATE = START;
GO

-- Retrieve histogram

WITH xedata
AS (SELECT CAST(dxst.target_data AS XML) AS target_data
    FROM sys.dm_xe_session_targets AS dxst
        JOIN sys.dm_xe_sessions AS dxs
            ON dxs.address = dxst.event_session_address
    WHERE dxs.name = 'AuditLogins'
          AND dxst.target_name = 'histogram'),

     histdata
AS (SELECT xed.slot_data.value('(value)[1]', 'varchar(256)') AS objectid,
           xed.slot_data.value('(@count)[1]', 'varchar(256)') AS slotcount
    FROM xedata AS x
        CROSS APPLY x.target_data.nodes('//HistogramTarget/Slot') AS xed(slot_data) )

SELECT *
FROM histdata

GO

-- Retrieve file contents

IF OBJECT_ID('tempdb..#events') IS NOT NULL DROP TABLE #events
CREATE TABLE #events (event_xml XML);
INSERT INTO #events
SELECT xdata = CAST(event_data AS xml)
FROM (
select [TargetFileName] = REPLACE(c.column_value, '.xel', '') + '*.xel'
from sys.dm_xe_sessions AS s
join sys.dm_xe_session_object_columns AS c ON s.address = c.event_session_address
where column_name = 'filename' and s.name = 'AuditLogins'
) AS FileTarget CROSS APPLY sys.fn_xe_file_target_read_file (FileTarget.TargetFileName,null,null, null)


;WITH tabular AS
(
SELECT 
 [timestamp] = data.value('(event/@timestamp)[1]','varchar(30)'),
 [client_hostname] = data.value('(event/action[@name="client_hostname"]/value)[1]','nvarchar(4000)'),
 [client_app_name] = data.value('(event/action[@name="client_app_name"]/value)[1]','nvarchar(4000)'),
 [database_id] = data.value('(event/action[@name="database_id"]/value)[1]','int'),
 [database_name] = DB_NAME(data.value('(event/action[@name="database_id"]/value)[1]','int')),
 [server_principal_name] = data.value('(event/action[@name="server_principal_name"]/value)[1]','sysname'),
 [data] = data.query('.')
FROM #events AS event_data
)
--SELECT * FROM tabular AS t ORDER BY [timestamp] DESC;
SELECT [client_hostname], [client_app_name], [server_principal_name], COUNT(*) AS cnt FROM tabular GROUP BY [client_hostname], [client_app_name], [server_principal_name]
