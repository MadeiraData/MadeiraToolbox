DECLARE
	 @FromDate DATETIME2(3)	= DATEADD(hh,-24,GETDATE())
	,@ToDate DATETIME2(3)	= NULL

DECLARE @FileName NVARCHAR(4000)
SELECT @FileName = target_data.value('(EventFileTarget/File/@name)[1]','nvarchar(4000)')
FROM (SELECT CAST(target_data AS XML) target_data FROM sys.dm_xe_sessions s
JOIN sys.dm_xe_session_targets t ON s.address = t.event_session_address
WHERE s.name = N'AlwaysOn_health') ft

DROP TABLE IF EXISTS #event_xml;
CREATE TABLE #event_xml ( object_name nvarchar(255), event_timestamp datetime2(3), XEData XML );
INSERT INTO #event_xml
SELECT object_name, XEData.value('(event/@timestamp)[1]','datetime2(3)') as event_timestamp, XEData
FROM
(
SELECT object_name, CAST(event_data AS XML) XEData
FROM sys.fn_xe_file_target_read_file(@FileName, NULL, NULL, NULL)
WHERE @FileName IS NOT NULL
) event_data
WHERE 
	(@FromDate IS NULL OR XEData.value('(event/@timestamp)[1]','datetime2(3)') >= @FromDate)
AND
	(@ToDate IS NULL OR XEData.value('(event/@timestamp)[1]','datetime2(3)') <= @ToDate)

SELECT
	  event_timestamp
	, object_name AS event_name
	, ObjectName AS report
	, Report AS report_desc
	, availability_replica_name
	, availability_group_name
	, database_replica_name
	, errseverity AS event_severity
	, errmessage AS event_message
	, event_data
FROM
(
	SELECT
	  object_name
	, event_timestamp
	, XEData.value('(event/data[@name="previous_state"]/text)[1]', 'varchar(255)') AS previous_state
	, XEData.value('(event/data[@name="current_state"]/text)[1]', 'varchar(255)') AS current_state
	, XEData.value('(event/data[@name="availability_replica_name"]/value)[1]', 'varchar(255)') AS availability_replica_name
	, XEData.value('(event/data[@name="availability_group_name"]/value)[1]', 'varchar(255)') AS availability_group_name
	, XEData.value('(event/data[@name="database_replica_name"]/value)[1]', 'varchar(255)') AS database_replica_name
	, XEData.value('(event/data[@name="forced_quorum"]/value)[1]', 'varchar(255)') AS forced_quorum
	, XEData.value('(event/data[@name="joined_and_synchronized"]/value)[1]', 'varchar(255)') AS joined_and_synchronized
	, XEData.value('(event/data[@name="previous_primary_or_automatic_failover_target"]/value)[1]', 'varchar(255)') AS previous_primary_or_automatic_failover_target
	, XEData.value('(event/data[@name="error_number"]/value)[1]', 'int') AS errnumber
	, XEData.value('(event/data[@name="severity"]/value)[1]', 'int') AS errseverity
	, XEData.value('(event/data[@name="message"]/value)[1]', 'nvarchar(1000)') AS errmessage
	, XEData.value('(event/data[@name="suspend_status"]/value)[1]', 'varchar(255)') AS suspend_status
	, XEData.value('(event/data[@name="suspend_source"]/value)[1]', 'varchar(255)') AS suspend_source
	, XEData.value('(event/data[@name="suspend_reason"]/value)[1]', 'varchar(255)') AS suspend_reason
	, XEData.query('event') AS event_data
	FROM #event_xml
) AS a
OUTER APPLY
(
SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(ISNULL(database_replica_name, availability_replica_name))
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - Replica state changed from "' + previous_state + N'" to "' + current_state + N'"'
WHERE object_name = 'availability_replica_state_change'
AND current_state NOT IN ('RESOLVING_PENDING_FAILOVER', 'NOT_AVAILABLE')
 
UNION ALL
 
SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(ISNULL(database_replica_name, availability_replica_name))
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - Data Movement is ' + suspend_status + ISNULL(' (' + suspend_source + N')', N'') + N': ' + ISNULL(suspend_reason, N'Reason unknown')
WHERE object_name = 'data_movement_suspend_resume'
AND suspend_status <> 'RESUMED'

UNION ALL

SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(availability_replica_name)
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - Replica state changed from "' + previous_state + N'" to "' + current_state + N'"'
WHERE object_name = 'availability_replica_state_change'
AND current_state = 'RESOLVING_PENDING_FAILOVER'
 
UNION ALL
 
SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name)
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - AG lease expired (connectivity between the AG and the underlying WSFC cluster is broken)'
WHERE object_name = 'availability_group_lease_expired'

UNION ALL

SELECT
	  ObjectName = N'Availability Replica Manager'
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - Manager State is OFFLINE'
WHERE object_name = 'availability_replica_manager_state_change'
AND current_state = 'Offline'

UNION ALL

SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(availability_replica_name)
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - Replica state changed from "' + previous_state + N'" to "' + current_state + N'"'
WHERE object_name = 'availability_replica_state_change'
AND current_state ='NOT_AVAILABLE'
 
UNION ALL
 
SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(availability_replica_name)
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - Failover Validation Failed:'
		+ CASE WHEN forced_quorum = 'TRUE' THEN N' Forced Quorum;' ELSE N'' END
		+ CASE WHEN joined_and_synchronized = 'FALSE' THEN N' Not joined and synchronized;' ELSE N'' END
		+ CASE WHEN previous_primary_or_automatic_failover_target = 'FALSE' THEN N' Not previous Primary or Automatic Failover Target;' ELSE N'' END
WHERE
	object_name = 'availability_replica_automatic_failover_validation'
AND (
	forced_quorum = 'TRUE'
OR joined_and_synchronized = 'FALSE'
OR previous_primary_or_automatic_failover_target = 'FALSE'
)
 
UNION ALL
 
SELECT
	  ObjectName = N'Availability Group Error'
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' - Error ' + CONVERT(nvarchar, errnumber) + N': ' + errmessage
WHERE object_name = 'error_reported'
AND errseverity >= 10

UNION ALL
 
SELECT N'AlwaysOn_Health Session is not active!'
	, N'AlwaysOn is in use but extended event session is inactive!'
WHERE EXISTS (SELECT * FROM sys.dm_hadr_availability_group_states)
AND @FileName IS NULL
) AS R
ORDER BY
	event_timestamp DESC