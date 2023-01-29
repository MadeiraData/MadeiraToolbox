SET NOCOUNT ON;
DECLARE @NumOfSamplesToCheck int = 1

declare @totalCPUs int, @activeCPUs int, @TimeStamp bigint, @TotalCPUUtilization float, @SQLCPUUtilization float;

select @activeCPUs = COUNT(*)
from sys.dm_os_schedulers
where is_online = 1
and status = 'VISIBLE ONLINE';

select @totalCPUs = cpu_count, @TimeStamp = cpu_ticks / (cpu_ticks/ms_ticks)
from sys.dm_os_sys_info;

SELECT @TotalCPUUtilization = AVG(100 - SystemIdle), @SQLCPUUtilization = AVG(SQLServerCPUUtilization)
FROM (
 SELECT TOP (@NumOfSamplesToCheck) [timestamp], convert(xml, record) as record
 FROM sys.dm_os_ring_buffers
 WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
 AND record LIKE '%<SystemHealth>%'
 ORDER BY [timestamp] DESC
) AS RingBufferInfo
CROSS APPLY
(SELECT
 SystemIdle = record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int'),
 SQLServerCPUUtilization = record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')
) AS countervalues

SELECT
	  @TotalCPUUtilization AS TotalCPUUtilization
	, @SQLCPUUtilization AS SQLServerCPUUtilization
-- Extrapolate relative CPU utilization based on total number of cores available to SQL Server:
	, @activeCPUs AS activeCPUs
	, @totalCPUs AS totalCPUs
	, @TotalCPUUtilization / @activeCPUs * @totalCPUs AS RelativeCPUUtilization