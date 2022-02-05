CREATE EVENT SESSION [TrackLogins] ON SERVER 
ADD EVENT sqlserver.login(
    ACTION(sqlserver.session_id,sqlserver.session_nt_username,sqlserver.session_server_principal_name)
    WHERE ([sqlserver].[session_nt_user]=N'') -- filter for SQL logins only (comment this line to include Windows logins as well)
    ) 
ADD TARGET package0.histogram(SET filtering_event_name=N'sqlserver.login',source=N'sqlserver.session_server_principal_name')
WITH (MAX_MEMORY=4096 KB
,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)
;
ALTER EVENT SESSION TrackLogins
ON SERVER STATE = START
GO

SELECT
	SQLLoginName = X.value('(value/text())[1]','sysname'),
	LoginCount = X.value('(@count)[1]','int')
FROM
(
SELECT CAST(st.target_data AS xml) AS histogram_xml
FROM sys.dm_xe_session_targets AS st
JOIN sys.dm_xe_sessions AS s
ON (s.address = st.event_session_address)
WHERE s.name = 'TrackLogins'
) AS q
CROSS APPLY histogram_xml.nodes('/HistogramTarget/Slot') AS H(X)