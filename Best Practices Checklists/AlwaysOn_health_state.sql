SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT checks.Msg, checks.RemediationCmd
from sys.server_event_sessions AS es
CROSS APPLY
(
	SELECT N'AlwaysOn_health is not enabled for server startup!'
	, RemediationCmd = N'ALTER EVENT SESSION [AlwaysOn_health] ON SERVER WITH (STARTUP_STATE=ON);'
	WHERE es.startup_state = 0
	UNION ALL
	SELECT N'AlwaysOn_health session is not started!'
	, RemediationCmd = N'ALTER EVENT SESSION [AlwaysOn_health] ON SERVER STATE = START;'
	WHERE NOT EXISTS (select * from sys.dm_xe_sessions AS xes where xes.[name] = 'AlwaysOn_health')
) AS checks(Msg,RemediationCmd)
where es.[name] = 'AlwaysOn_health'
and CONVERT(tinyint, SERVERPROPERTY('IsHadrEnabled')) = 1
and exists (select * from sys.availability_replicas AS ar where ar.replica_server_name = @@SERVERNAME)
OPTION(RECOMPILE);