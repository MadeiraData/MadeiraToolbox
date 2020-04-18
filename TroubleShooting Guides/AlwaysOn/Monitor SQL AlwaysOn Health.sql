-- List AG Replica Details 

SELECT DISTINCT
	n.group_name,
	n.replica_server_name,
	n.node_name,rs.role_desc 
FROM 
	sys.dm_hadr_availability_replica_cluster_nodes n 
JOIN 
	sys.dm_hadr_availability_replica_cluster_states cs 
ON 
	n.replica_server_name = cs.replica_server_name 
JOIN 
	sys.dm_hadr_availability_replica_states rs  
ON 
	rs.replica_id = cs.replica_id ;
 
-- Secondary AG Status (To exec from the Primary)

DECLARE @HADRName    varchar(25) 
SET @HADRName = @@SERVERNAME 
SELECT DISTINCT
	n.group_name,
	n.replica_server_name,
	n.node_name,
	rs.role_desc, 
	--db_name(drs.database_id) as 'DBName',
	drs.synchronization_state_desc,
	drs.synchronization_health_desc 
FROM 
	sys.dm_hadr_availability_replica_cluster_nodes n 
JOIN 
	sys.dm_hadr_availability_replica_cluster_states cs 
ON 
	n.replica_server_name = cs.replica_server_name 
JOIN 
	sys.dm_hadr_availability_replica_states rs  
ON 
	rs.replica_id = cs.replica_id 
JOIN 
	sys.dm_hadr_database_replica_states drs 
ON 
	rs.replica_id=drs.replica_id 
WHERE 
	n.replica_server_name <> @HADRName;


-- if fail over cluster manager has a cuorum (1 = Normal quorum)

SELECT * FROM sys.dm_hadr_cluster;

--returns a row for each of the members that constitute the quorum and the state of each of them

SELECT * FROM sys.dm_hadr_cluster_members; --(member_state =  1 = Online )

--validate the network virtual IP

SELECT * FROM sys.dm_hadr_cluster_networks;

--The purpose of this mapping is to handle the scenario in which the WSFC resource/group is renamed

SELECT * FROM sys.dm_hadr_name_id_map;

-- Availability Group State

SELECT * FROM sys.dm_hadr_availability_group_states;

-- If Page repair occur (if member asked from other member a fresh copy of currapted page)

SELECT * FROM sys.dm_hadr_auto_page_repair;

-- Listener state

SELECT * FROM sys.dm_tcp_listener_states;
SELECT * FROM sys.availability_group_listeners

-- Monitoring the Latency - exec on the primary
WITH AG_Stats 
AS 
(
            SELECT AGS.name                       AS AGGroupName, 
                   AR.replica_server_name         AS InstanceName, 
                   HARS.role_desc, 
                   Db_name(DRS.database_id)       AS DBName, 
                   DRS.database_id, 
                   AR.availability_mode_desc      AS SyncMode, 
                   DRS.synchronization_state_desc AS SyncState, 
                   DRS.last_hardened_lsn, 
                   DRS.end_of_log_lsn, 
                   DRS.last_redone_lsn, 
                   DRS.last_hardened_time, -- On a secondary database, time of the log-block identifier for the last hardened LSN (last_hardened_lsn).
                   DRS.last_redone_time, -- Time when the last log record was redone on the secondary database.
                   DRS.log_send_queue_size, 
                   DRS.redo_queue_size,
                    --Time corresponding to the last commit record.
                    --On the secondary database, this time is the same as on the primary database.
                    --On the primary replica, each secondary database row displays the time that the secondary replica that hosts that secondary database 
                    --   has reported back to the primary replica. The difference in time between the primary-database row and a given secondary-database 
                    --   row represents approximately the recovery time objective (RPO), assuming that the redo process is caught up and that the progress 
                    --   has been reported back to the primary replica by the secondary replica.
                   DRS.last_commit_time
            FROM   
				sys.dm_hadr_database_replica_states DRS 
            LEFT JOIN 
				sys.availability_replicas AR 
            ON 
				DRS.replica_id = AR.replica_id 
            LEFT JOIN 
				sys.availability_groups AGS 
            ON 
				AR.group_id = AGS.group_id 
            LEFT JOIN 
				sys.dm_hadr_availability_replica_states HARS ON AR.group_id = HARS.group_id 
            AND 
				AR.replica_id = HARS.replica_id 
            ),
    Pri_CommitTime AS 
            (
            SELECT  DBName
                    ,last_commit_time
            FROM    AG_Stats
            WHERE   role_desc = 'PRIMARY'
            ),
    Rpt_CommitTime AS 
            (
            SELECT  DBName, last_commit_time
            FROM    AG_Stats
            WHERE   role_desc = 'SECONDARY' --AND [InstanceName] = 'InstanceNameB-PrimaryDataCenter'
            ),
    FO_CommitTime AS 
            (
            SELECT  DBName, last_commit_time
            FROM    AG_Stats
            WHERE   role_desc = 'SECONDARY' --AND ([InstanceName] = 'InstanceNameC-SecondaryDataCenter' OR [InstanceName] = 'InstanceNameD-SecondaryDataCenter')
            )
SELECT 
		p.[DBName]										    AS [DatabaseName] 
	  ,	p.last_commit_time									AS [Primary_Last_Commit_Time]
      , r.last_commit_time									AS [Reporting_Last_Commit_Time]
      , DATEDIFF(ss,r.last_commit_time,p.last_commit_time)	AS [Reporting_Sync_Lag_(secs)]
      , f.last_commit_time									AS [FailOver_Last_Commit_Time]
      , DATEDIFF(ss,f.last_commit_time,p.last_commit_time)	AS [FailOver_Sync_Lag_(secs)]
FROM 
	Pri_CommitTime p
LEFT JOIN 
	Rpt_CommitTime r 
ON 
	[r].[DBName] = [p].[DBName]
LEFT JOIN 
	FO_CommitTime f 
ON [f].[DBName] = [p].[DBName]

-- If the SQL service is running
EXEC xp_servicecontrol N'querystate'
	,N'MSSQLSERVER'

-- Last_Time_Instance_Start with service current status

SELECT servicename AS ServiceName
	,startup_type_desc AS StartupType
	,status_desc AS ServiceStatus
	,process_id AS ProcessID
	,last_startup_time AS LastStartupTime
	,service_account AS ServiceAccount
FROM sys.dm_server_services
WHERE servicename = 'SQL Server (MSSQLSERVER)'

-- performance counters

SELECT * FROM sys.dm_os_performance_counters
WHERE object_name IN ( 'SQLServer:Database Replica','SQLServer:Availability Replica')

SQLServer:Availability Replica
/*
Counter Name	                  Description
Bytes Received from Replica/sec	  Number of bytes received from the availability replica per second. Pings and status updates will generate network traffic even on databases with no user updates.
Bytes Sent to Replica/sec	      Number of bytes sent to the remote availability replica per second. On the primary replica this is the number of bytes sent to the secondary replica. On the secondary replica this is the number of bytes sent to the primary replica.
Bytes Sent to Transport/sec	      Actual number of bytes sent per second over the network to the remote availability replica. On the primary replica this is the number of bytes sent to the secondary replica. On the secondary replica this is the number of bytes sent to the primary replica.
Flow Control Time (ms/sec)	      Time in milliseconds that log stream messages waited for send flow control, in the last second.
Flow Control/sec	              Number of times flow-control initiated in the last second. Flow Control Time (ms/sec) divided by Flow Control/sec is the average time per wait.
Receives from Replica/sec	      Number of AlwaysOn messages received from the replica per second.
Resent Messages/sec	              Number of AlwaysOn messages resent in the last second.
Sends to Replica/sec	          Number of AlwaysOn messages sent to this availability replica per second.
Sends to Transport/sec	          Actual number of AlwaysOn messages sent per second over the network to the remote availability replica. On the primary replica this is the number of messages sent to the secondary replica. On the secondary replica this is the number of messages sent to the primary replica.
*/
SQLServer:Database Replica
/*
Counter Name	                Description	View on…
File Bytes Received/sec	        Amount of FILESTREAM data received by the secondary replica for the secondary database in the last second.	Secondary replica
Log Bytes Received/sec	        Amount of log records received by the secondary replica for the database in the last second.	Secondary replica
Log remaining for undo	        The amount of log in kilobytes remaining to complete the undo phase.	Secondary replica
Log Send Queue	                Amount of log records in the log files of the primary database, in kilobytes, that has not yet been sent to the secondary replica. This value is sent to the secondary replica from the primary replica. Queue size does not include FILESTREAM files that are sent to a secondary.	Secondary replica
Mirrored Write Transaction/sec	Number of transactions that wrote to the mirrored database and waited for the log to be sent to the mirror in order to commit, in the last second.	Primary replica
Recovery Queue	                Amount of log records in the log files of the secondary replica that has not yet been redone.	Secondary replica
Redo Bytes Remaining	        The amount of log in kilobytes remaining to be redone to finish the reverting phase.	Secondary replica
Redone Bytes/sec	            Amount of log records redone on the secondary database in the last second.	Secondary replica
Total Log requiring undo	    Total kilobytes of log that must be undone.	Secondary replica
Transaction Delay	            Delay in waiting for unterminated commit acknowledgement, in milliseconds.	Primary replica
*/



-- read important error for AlwaysOn- information only

SELECT 
	message_id,
	TEXT,
	severity
FROM 
	sys.messages
WHERE 
	TEXT LIKE ('%availability%') 
AND 
	language_id = 1033  
AND message_id IN (
976	 ,   --The target database cannot be queried. Either data movement is suspended or the availability replica is not enabled for read access.
983	 ,   --The availability database is not accessible and reason needs to be investigated
1480,	--The AlwaysOn Availability Group has failed over. The reason for failing over should be examined. This is an informational message only.
19406,	--The state of the local availability replica has changed. Reason should be investigated
35254,	--An error occurred while accessing the availability group metadata. Check and investigate the root cause of this error
35262,	--This is informational error and it indicates that the default startup of database will be skipped as database is member of an availability group
35273,	--Bypassing recovery since availability group database is marked as inaccessible, because the session with the primary replica was interrupted or the WSFC node lacks quorum endpoint configuration
35274,	--An availability database recovery is pending while waiting for the secondary replica to receive transaction log from the primary
35275,	--The availability database is in a potentially damaged state, and as such it cannot be joined to availability group. Restoring and rejoining database is recommended
35276,	--This error indicates that manual intervention could be needed to restart synchronization of the database. If the problem is persistent, restart of the local SQL Server might be required.
35279,	--The primary replica rejected joining of the new database to availability group due to error.
35299,	--Info message that some nonqualified transactions are being rolled back in database for an AlwaysOn change of state.
41048,	--Local Windows Server Failover Clustering service is not available
41049,	--Local Windows Server Failover Clustering node is not online anymore
41050,	--Waiting for the start of a local Windows Server Failover Clustering service
41051,	--Local Windows Server Failover Clustering service started
41052,	--AlwaysOn Availability Groups: Waiting for local Windows Server Failover Clustering node to start. This is an informational message only. No user action is required.
41053,	--AlwaysOn Availability Groups: Local Windows Server Failover Clustering node started. This is an informational message only. No user action is required.
41054,	--AlwaysOn Availability Groups: Waiting for local Windows Server Failover Clustering node to come online. This is an informational message only. No user action is required.
41055,	--AlwaysOn Availability Groups: Local Windows Server Failover Clustering node is online. This is an informational message only. No user action is required.
41089,	--AlwaysOn Availability Groups startup has been cancelled because SQL Server is shutting down. This is an informational message only. No user action is required.
41091,	--The local AlwaysOn availability group replica is going offline because the lease expired or lease renewal failed
41131,	--Bring availability group online has been failed. Verify that the local Windows Server Failover Clustering (WSFC) node is online
41406,	--The availability group is prepared for automatic failover due to the secondary replica is not ready for an automatic failover. The secondary replica is unavailable, or its data synchronization state is currently not in the SYNCHRONIZED synchronization state.
41414,	--At least one secondary replica is not connected to the primary replica and the indicated connected state is DISCONNECTED.
41421	--Availability database is suspended.
)

-- Power Shell

--Managing AlwaysOn with Powershell
http://www.sqlservercentral.com/blogs/chadmiller/2011/09/05/managing-alwayson-with-powershell/

--Monitor an AlwaysOn Availability Group with PowerShell 
https://blogs.technet.microsoft.com/heyscriptingguy/2013/04/30/monitor-an-alwayson-availability-group-with-powershell/

-- Monitor Failover Cluster

Import-Module FailoverClusters

-- get cluster group
Get-ClusterGroup -Cluster <ClusterName> 

-- get all cluster resources
Get-ClusterResource -Cluster <ClusterName>  | Where-Object {$_.OwnerGroup -like "*"} |  Sort-Object -Property OwnerGroup