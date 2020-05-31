-- Tiger Toolbox Max Memory Recommendation

DECLARE @sqlmajorver int, @systemmem int, @systemfreemem int, @maxservermem int, @numa_nodes_afinned int, @numa int
DECLARE @mwthreads_count int, @mwthreads int, @arch smallint, @sqlcmd nvarchar(4000)
DECLARE @MinMBMemoryForOS INT, @RecommendedMaxMemMB INT
SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);
SET @arch = CASE WHEN @@VERSION LIKE '%<X64>%' THEN 64 WHEN @@VERSION LIKE '%<IA64>%' THEN 128 ELSE 32 END;
 
SELECT @maxservermem = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE [Name] = 'max server memory (MB)';
SELECT @numa_nodes_afinned = COUNT (DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64 AND is_online = 1
SELECT @numa = COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64;
SELECT @mwthreads = CONVERT(int, [value]) FROM sys.configurations WHERE [Name] = 'max worker threads'
SELECT @mwthreads_count = max_workers_count FROM sys.dm_os_sys_info;
 
IF @sqlmajorver = 9
BEGIN
	SET @sqlcmd = N'SELECT @systemmemOUT = t1.record.value(''(./Record/MemoryRecord/TotalPhysicalMemory)[1]'', ''bigint'')/1024, 
	@systemfreememOUT = t1.record.value(''(./Record/MemoryRecord/AvailablePhysicalMemory)[1]'', ''bigint'')/1024
FROM (SELECT MAX([TIMESTAMP]) AS [TIMESTAMP], CONVERT(xml, record) AS record 
	FROM sys.dm_os_ring_buffers (NOLOCK)
	WHERE ring_buffer_type = N''RING_BUFFER_RESOURCE_MONITOR''
		AND record LIKE ''%RESOURCE_MEMPHYSICAL%''
	GROUP BY record) AS t1';
END
ELSE
BEGIN
	SET @sqlcmd = N'SELECT @systemmemOUT = total_physical_memory_kb/1024, @systemfreememOUT = available_physical_memory_kb/1024 FROM sys.dm_os_sys_memory';
END
EXECUTE sp_executesql @sqlcmd, N'@systemmemOUT bigint OUTPUT, @systemfreememOUT bigint OUTPUT', @systemmemOUT=@systemmem OUTPUT, @systemfreememOUT=@systemfreemem OUTPUT;
 
SET @MinMBMemoryForOS = CASE WHEN @systemmem <= 2048 THEN 512
		WHEN @systemmem BETWEEN 2049 AND 4096 THEN 819
		WHEN @systemmem BETWEEN 4097 AND 8192 THEN 1228
		WHEN @systemmem BETWEEN 8193 AND 12288 THEN 2048
		WHEN @systemmem BETWEEN 12289 AND 24576 THEN 2560
		WHEN @systemmem BETWEEN 24577 AND 32768 THEN 3072
		WHEN @systemmem > 32768 THEN 4096
	END
 
SET @RecommendedMaxMemMB = @systemmem-@MinMBMemoryForOS-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)-256)
 
SELECT
	  CurrentMaxMemorySettingMB = @maxservermem
	, ServerTotalMemoryMB		= @systemmem
	, MinMemoryForOSMB			= @MinMBMemoryForOS
	, RecommendedMaxMemForSingleNumaMB		= @RecommendedMaxMemMB
	, NumaNodes					= @numa
	, NumaNodesAfinned			= @numa_nodes_afinned

SELECT Recommendation = 'Maximum value for MaxMem setting on this configuration is ' + CONVERT(NVARCHAR,(@systemmem/@numa) * @numa_nodes_afinned) + ' MB for a single instance'
WHERE @numa > 1
UNION ALL
SELECT Recommendation = 'Maximum value for MaxMem setting on this configuration is ' + CONVERT(nvarchar(1000), @RecommendedMaxMemMB) + N' MB for a single instance'
WHERE @numa <= 1
