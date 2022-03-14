/*
===================================
Balance CPU Cores Across NUMA Nodes
===================================
Author: Eitan Blumin
Date: 2022-03-14
Description:
This script automatically detects whether there are offline CPU cores,
and generates a command to change the CPU Affinity mask to evenly
distribute the number of online CPUs across all NUMA nodes.

This will resolve NUMA imbalance issues on systems with underutilized
CPU cores due to SQL Server licensing limits.

No restart is required for this change to take effect.

More info:
https://docs.microsoft.com/sql/database-engine/configure-windows/affinity-mask-server-configuration-option
https://docs.microsoft.com/sql/t-sql/statements/alter-server-configuration-transact-sql#Affinity
*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @numa_nodes int, @total_cpu_cores int, @online_cpu_cores int, @online_cores_per_numa int
DECLARE @cmd nvarchar(max)

SELECT
	@numa_nodes = COUNT(DISTINCT nod.memory_node_id),
	@total_cpu_cores = COUNT(*),
	@online_cpu_cores = SUM(CASE WHEN sch.is_online = 1 THEN 1 ELSE 0 END)
from sys.dm_os_schedulers AS sch
inner join sys.dm_os_nodes AS nod ON sch.parent_node_id = nod.node_id
where sch.[status] like 'VISIBLE%' -- only cores visible to SQL
AND nod.memory_node_id <> 64 -- ignore DAC node

SET @online_cores_per_numa = @online_cpu_cores / @numa_nodes

RAISERROR(N'NUMA nodes: %d, Total CPU cores: %d, Online CPU cores: %d, CPU cores per NUMA: %d',0,1,@numa_nodes,@total_cpu_cores,@online_cpu_cores,@online_cores_per_numa) WITH NOWAIT;

IF @total_cpu_cores = @online_cpu_cores
BEGIN
	RAISERROR(N'No offline CPU cores found. Affinity is not required.',0,1);

	IF EXISTS (select * from sys.configurations where name = 'affinity mask' AND value_in_use = 0)
	BEGIN
		RAISERROR(N'Affinity is already set to auto. No need to change anything.',0,1);
	END
	ELSE
	BEGIN
		SET @cmd = N'ALTER SERVER CONFIGURATION SET PROCESS AFFINITY CPU = AUTO;'
	END
END
ELSE
BEGIN
	SET @cmd = NULL;

	select @cmd = ISNULL(@cmd + N', ', N'')
	+ CONVERT(nvarchar(max), min(sch.cpu_id))
	+ N' TO '
	+ CONVERT(nvarchar(max), min(sch.cpu_id) + @online_cores_per_numa - 1)
	--select nod.memory_node_id, min(sch.cpu_id), max(sch.cpu_id)
	from sys.dm_os_schedulers AS sch
	inner join sys.dm_os_nodes AS nod ON sch.parent_node_id = nod.node_id
	where sch.[status] like 'VISIBLE%' -- only cores visible to SQL
	AND nod.memory_node_id <> 64 -- ignore DAC node
	group by nod.memory_node_id

	SET @cmd = N'ALTER SERVER CONFIGURATION SET PROCESS AFFINITY CPU = ' + @cmd + N';';
END

SELECT @cmd AS RemediationCommand;