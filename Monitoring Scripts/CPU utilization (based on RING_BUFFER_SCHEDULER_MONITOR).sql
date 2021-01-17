DECLARE @TimeStamp bigint, @NumOfSamplesToCheck int
SET @NumOfSamplesToCheck = 10
SELECT @TimeStamp = cpu_ticks / (cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info

select top (@NumOfSamplesToCheck) CONVERT(varchar,DATEADD (ms, -1 * (@TimeStamp - [timestamp]), GETDATE()),8) + ' | SQL(' + Cast(SQLServerCPUUtilization as varchar(5)) + '%) | Other(' + Cast((100 - SystemIdle - SQLServerCPUUtilization) as varchar(30)) + '%)',  100 - SystemIdle as TotalCPU
FROM ( 
select record.value('(./Record/@id)[1]', 'int') as id,
record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as SystemIdle,
record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as SQLServerCPUUtilization,
timestamp
from (
select timestamp, convert(xml, record) as record
from sys.dm_os_ring_buffers
where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
and record like '%<SystemHealth>%') as RingBufferInfo
) AS TabularInfo
order by id desc