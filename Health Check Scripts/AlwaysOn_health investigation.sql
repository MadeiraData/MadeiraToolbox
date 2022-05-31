/*
AlwaysOn Availability Group Error Events
========================================
Author: Eitan Blumin
Date: 2020-05-31
This alert check the contents of the AlwaysOn_Health extended events session for data suspension, role changes, and other errors.
 
For more info:
https://docs.microsoft.com/sql/database-engine/availability-groups/windows/always-on-extended-events
*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE
	 @FromDate			datetime2(3)	= DATEADD(hh,-24,GETDATE())
	,@ToDate			datetime2(3)	= NULL
	,@MaxSecondsForErrorRecovery	int		= 20	-- optionally ignore various errors if these were "recovered" within the specified number of seconds
	,@ShowRecoveryEvents		bit		= 0	-- optionally show "recovery" events as well (i.e. recovery from error states)

DECLARE @FileName NVARCHAR(4000)
SELECT @FileName = target_data.value('(EventFileTarget/File/@name)[1]','nvarchar(4000)')
FROM (SELECT CAST(target_data AS XML) target_data FROM sys.dm_xe_sessions s
JOIN sys.dm_xe_session_targets t ON s.address = t.event_session_address
WHERE s.name = N'AlwaysOn_health') ft

IF OBJECT_ID('tempdb..#event_xml') IS NOT NULL DROP TABLE #event_xml;
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
OPTION(RECOMPILE);

;WITH AGEvents
AS
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
)
SELECT
	  a.event_timestamp
	, a.object_name AS event_name
	, R.ObjectName AS report
	, R.Report AS report_desc
	, a.availability_replica_name
	, a.availability_group_name
	, a.database_replica_name
	, a.errnumber AS event_error
	, a.errseverity AS event_severity
	, a.errmessage AS event_message
	, a.event_data
FROM AGEvents AS a
CROSS APPLY
(
SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(ISNULL(database_replica_name, availability_replica_name))
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' ' + QUOTENAME(object_name) + N' - Replica state changed from "' + previous_state + N'" to "' + current_state + N'"'
WHERE object_name = 'availability_replica_state_change'
AND current_state NOT IN ('RESOLVING_PENDING_FAILOVER', 'NOT_AVAILABLE')
AND @ShowRecoveryEvents = 1

UNION ALL

SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(availability_replica_name)
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' ' + QUOTENAME(object_name) + N' - Replica state changed from "' + previous_state + N'" to "' + current_state + N'"'
WHERE object_name = 'availability_replica_state_change'
AND current_state IN ('RESOLVING_PENDING_FAILOVER', 'NOT_AVAILABLE')
AND NOT EXISTS (
	SELECT * FROM AGEvents AS n
	WHERE n.object_name = 'availability_replica_state_change'
	AND n.current_state NOT IN ('RESOLVING_PENDING_FAILOVER', 'NOT_AVAILABLE')
	AND n.event_timestamp BETWEEN a.event_timestamp AND DATEADD(second, @MaxSecondsForErrorRecovery, a.event_timestamp)
	AND a.availability_group_name = n.availability_group_name
	AND a.availability_replica_name = n.availability_replica_name
)

UNION ALL

SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(ISNULL(database_replica_name, availability_replica_name))
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' ' + QUOTENAME(object_name) + N' - Data Movement is ' + suspend_status + ISNULL(' (' + suspend_source + N')', N'') + N': ' + ISNULL(suspend_reason, N'Reason unknown')
WHERE object_name = 'data_movement_suspend_resume'
AND (suspend_status <> 'RESUMED' OR @ShowRecoveryEvents = 1)
AND NOT EXISTS (
	SELECT * FROM AGEvents AS n
	WHERE n.object_name = 'data_movement_suspend_resume'
	AND n.suspend_status = 'RESUMED'
	AND n.event_timestamp BETWEEN a.event_timestamp AND DATEADD(second, @MaxSecondsForErrorRecovery, a.event_timestamp)
	AND a.availability_group_name = n.availability_group_name
	AND ISNULL(a.database_replica_name, a.availability_replica_name) = ISNULL(n.database_replica_name, n.availability_replica_name)
)

UNION ALL

SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name)
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' ' + QUOTENAME(object_name) + N' - AG lease expired (connectivity between the AG and the underlying WSFC cluster is broken)'
WHERE object_name = 'availability_group_lease_expired'

UNION ALL

SELECT
	  ObjectName = N'Availability Replica Manager'
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' ' + QUOTENAME(object_name) + N' - Manager State is: ' + current_state
WHERE object_name = 'availability_replica_manager_state_change'
AND (current_state = 'Offline' OR @ShowRecoveryEvents = 1)
AND NOT EXISTS (
	SELECT * FROM AGEvents AS n
	WHERE n.object_name = 'availability_replica_manager_state_change'
	AND a.current_state = 'Offline'
	AND n.current_state = 'Online'
	AND n.event_timestamp BETWEEN a.event_timestamp AND DATEADD(second, @MaxSecondsForErrorRecovery, a.event_timestamp)
	--AND a.availability_group_name = n.availability_group_name
	--AND a.availability_replica_name = n.availability_replica_name
)

UNION ALL

SELECT
	  ObjectName = N'Availability Group ' + QUOTENAME(availability_group_name) + N' Replica ' + QUOTENAME(availability_replica_name)
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' ' + QUOTENAME(object_name) + N' - Failover Validation Failed:'
		+ CASE WHEN forced_quorum = 'TRUE' THEN N' Forced Quorum;' ELSE N'' END
		+ CASE WHEN joined_and_synchronized = 'FALSE' THEN N' Not joined and synchronized;' ELSE N'' END
		+ CASE WHEN previous_primary_or_automatic_failover_target = 'FALSE' THEN N' Not previous Primary or Automatic Failover Target;' ELSE N'' END
WHERE
	object_name = 'availability_replica_automatic_failover_validation'
AND (
	forced_quorum = 'TRUE'
OR joined_and_synchronized = 'FALSE'
OR previous_primary_or_automatic_failover_target = 'FALSE'
OR @ShowRecoveryEvents = 1
)
AND NOT EXISTS (
	SELECT * FROM AGEvents AS n
	WHERE n.object_name = 'availability_replica_automatic_failover_validation'
	AND (
		(a.forced_quorum = 'TRUE' AND n.forced_quorum = 'FALSE') OR
		(a.joined_and_synchronized = 'FALSE' AND n.joined_and_synchronized = 'TRUE') OR
		(a.previous_primary_or_automatic_failover_target = 'FALSE' AND n.previous_primary_or_automatic_failover_target = 'TRUE')
	)
	AND n.event_timestamp BETWEEN a.event_timestamp AND DATEADD(second, @MaxSecondsForErrorRecovery, a.event_timestamp)
	AND a.availability_group_name = n.availability_group_name
	AND a.availability_replica_name = n.availability_replica_name
)

UNION ALL

SELECT
	  ObjectName = N'Availability Group Error'
	, Report = CONVERT(nvarchar,event_timestamp,121) + N' ' + QUOTENAME(object_name) + N' - Error ' + CONVERT(nvarchar, errnumber) + N', Severity ' + CONVERT(nvarchar,errseverity) + N': ' + errmessage
WHERE object_name = 'error_reported'
AND errseverity >= 10
AND (errnumber NOT IN (26022,35202,41051,41053,41055) OR @ShowRecoveryEvents = 1)
AND NOT EXISTS (
	SELECT * FROM AGEvents AS n
	WHERE
	(
		(n.errnumber = 35202 AND a.errnumber = 35206)
	OR	(n.errnumber = 41051 AND a.errnumber = 41050)
	OR	(n.errnumber = 41053 AND a.errnumber = 41052)
	OR	(n.errnumber = 41055 AND a.errnumber = 41054)
	)
	AND n.event_timestamp BETWEEN a.event_timestamp AND DATEADD(second, @MaxSecondsForErrorRecovery, a.event_timestamp)
	--AND a.availability_group_name = n.availability_group_name
	--AND a.availability_replica_name = n.availability_replica_name
)

UNION ALL

SELECT N'AlwaysOn_Health Session is not active!'
	, N'AlwaysOn is in use but the AlwaysOn_health extended event session is inactive!'
WHERE EXISTS (SELECT * FROM sys.dm_hadr_availability_group_states)
AND @FileName IS NULL
) AS R
ORDER BY
	event_timestamp DESC
OPTION (RECOMPILE);
