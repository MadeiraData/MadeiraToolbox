SET NOCOUNT ON;
SET ANSI_PADDING ON;
DECLARE @NumOfSamplesToCheck int = 10

DECLARE @TimeStamp bigint
SELECT @TimeStamp = ms_ticks FROM sys.dm_os_sys_info

SELECT
	DATEADD (ms, -1 * (@TimeStamp - [timestamp]), GETDATE()) AS [SystemTime],
	countervalues.SystemIdle,
	countervalues.SQLServerCPUUtilization,
	100 - SystemIdle - SQLServerCPUUtilization AS OtherCPUUtilization
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
