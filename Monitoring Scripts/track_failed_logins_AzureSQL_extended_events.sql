CREATE EVENT SESSION [TrackFailedLogins] ON DATABASE 
ADD EVENT sqlserver.error_reported(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.username,sqlserver.database_id,sqlserver.session_id, sqlserver.client_pid)
    WHERE ([error_number]=(18056) 
    OR [error_number]=(17892) 
    OR [error_number]=(18061)
    OR [error_number]=(18452)
    OR [error_number]=(11248)
    OR [error_number]=(17806)
    OR [error_number]=(18456)
    OR [error_number]=(18470)
    OR [error_number]=(18487)
    OR [error_number]=(18488)
    OR [error_number]=(17817)
    OR [error_number]=(17828)
    OR [error_number]=(17830)
    OR [error_number]=(17832)
    OR [error_number]=(17897)
    OR [error_number]=(18401)
    OR [error_number]=(18451)
    OR [error_number]=(18458)
    OR [error_number]=(18459)
    OR [error_number]=(18460)
    OR [error_number]=(18461)
    OR [error_number]=(18486)
    OR [error_number]=(26078)
    OR [error_number]=(33147)
    OR [error_number]=(40623)
   ))
ADD TARGET package0.ring_buffer(SET max_events_limit=(5000),max_memory=(8192))
WITH (MAX_MEMORY=8192 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,
TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)
;
GO
ALTER EVENT SESSION [TrackFailedLogins]  
ON DATABASE STATE = START;
GO