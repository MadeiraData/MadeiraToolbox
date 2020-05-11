select
	ObjectName = N'Replica Server "' + ar.replica_server_name + N'" (' + ars.role_desc + N')',
	M.Msg
from sys.dm_hadr_availability_replica_states ars
inner join sys.availability_replicas ar on ars.replica_id = ar.replica_id
and ars.group_id = ar.group_id
CROSS APPLY
(
	SELECT N'Synchronization is: ' + ars.synchronization_health_desc
	WHERE ars.synchronization_health_desc <> 'HEALTHY'
	UNION ALL
	SELECT N'Operational State is: ' + ISNULL(ars.operational_state_desc, 'UNKNOWN')
	WHERE ars.operational_state_desc <> 'ONLINE'
	UNION ALL
	SELECT N'Connection is: ' + ars.connected_state_desc
	WHERE ars.connected_state_desc <> 'CONNECTED'
) AS M(Msg)
 
UNION ALL
 
select distinct
N'Replica Database "' + rcs.database_name + N'" in server "' + ar.replica_server_name + N'"',
M.Msg
from sys.dm_hadr_database_replica_states drs
inner join sys.availability_replicas ar on drs.replica_id = ar.replica_id
and drs.group_id = ar.group_id
inner join sys.dm_hadr_database_replica_cluster_states rcs on drs.replica_id = rcs.replica_id
CROSS APPLY
(
	SELECT N'Synchronization is: ' + drs.synchronization_health_desc
	WHERE drs.synchronization_health_desc <> 'HEALTHY'
	UNION ALL
	SELECT N'Data Movement is: ' + drs.synchronization_state_desc
	WHERE drs.synchronization_state_desc IN ('NOT SYNCHRONIZING', 'REVERTING')
) AS M(Msg)
