/*
====================================
NUMA Node Imbalance Detection
====================================
Author: Eitan Blumin
Date: 2021-03-24
Description:
	This script outputs data about the current load on each NUMA node.
	Output Resultset:
		memory_node_id - the identifier of the NUMA node
		memory_node_percent - the percentage of the NUMA node based on the total number of nodes
		cpu_nodes_count - number of CPU sockets belonging to the NUMA node
		load_type - Type of load on the NUMA node:
			- Active Worker Threads (sys.dm_os_nodes) - Number of workers that are active on all schedulers managed by this node.
			- Average Load Balance (sys.dm_os_nodes) - Average number of tasks per scheduler on this node.
			- Total Number of Connections (sys.dm_exec_connections) - Number of connections with an affinity to this node.
			- Load Factor (sys.dm_os_schedulers) - Reflects the total perceived load on this node. When a task is enqueued, the load factor is increased. When a task is completed, the load factor is decreased.
			- Online Schedulers (sys.dm_os_nodes) - Number of online schedulers that are managed by this node.
		load_value - The corresponding value (based on load_type)
		total_load_value - The total load at the server level across all NUMA nodes.
		load_percent - Relative load on this node (load_value * 100.0 / total_load_value)
		balanced_utilization_percentage - Load utilization of this node relative to its relative percent (load_percent * 100.0 / memory_node_percent)
		imbalance_factor - The difference between the NUMA node with the lowest and the highest balanced_utilization_percentage at the server level. The higher this value is, the more un-balanced the server is.

Additional Resources:
	https://www.sqlpassion.at/archive/2019/09/23/troubleshooting-numa-node-inbalance-problems/
	https://glennsqlperformance.com/2020/06/25/how-to-balance-sql-server-core-licenses-across-numa-nodes/
*/
;WITH nodes
AS
(
	SELECT 'connections' as load_type, node_affinity as node_id, COUNT(*) as load_value
	FROM sys.dm_exec_connections WITH (NOLOCK) 
	GROUP BY node_affinity

	UNION ALL

	SELECT 'load factor', parent_node_id, SUM(load_factor)
	FROM sys.dm_os_schedulers WITH (NOLOCK) 
	WHERE status = 'VISIBLE ONLINE' and is_online = 1
	GROUP BY parent_node_id

	UNION ALL

	SELECT load_type, node_id, load_value
	FROM sys.dm_os_nodes WITH (NOLOCK)
	CROSS APPLY
	(VALUES
		('online schedulers',online_scheduler_count),
		('active workers', active_worker_count),
		('avg load balance', avg_load_balance)
	) as t(load_type, load_value)
	WHERE node_state_desc <> N'ONLINE DAC'
), memory_nodes AS
(
	SELECT n.memory_node_id
		, CONVERT(float, ROUND(100.0 / COUNT(*) OVER(PARTITION BY load_type), 2)) AS memory_node_percent
		, COUNT(*) AS cpu_nodes_count
		, load_type, SUM(nodes.load_value) AS load_value, SUM(SUM(load_value)) OVER(PARTITION BY load_type) AS total_load_value
		, CONVERT(float, ROUND(SUM(load_value) * 100.0 / SUM(SUM(load_value)) OVER(PARTITION BY load_type), 2)) AS load_percent
	FROM nodes
	INNER JOIN sys.dm_os_nodes AS n WITH(NOLOCK) ON nodes.node_id = n.node_id
	WHERE n.node_state_desc <> N'ONLINE DAC'
	GROUP BY n.memory_node_id, load_type
)
SELECT *
  , CONVERT(float, load_percent * 100.0 / memory_node_percent) AS balanced_utilization_percentage
  , MAX(load_percent * 100.0 / memory_node_percent) OVER(PARTITION BY load_type) - MIN(load_percent * 100.0 / memory_node_percent) OVER (PARTITION BY load_type) AS imbalance_factor
FROM memory_nodes
ORDER BY load_type, memory_node_id
