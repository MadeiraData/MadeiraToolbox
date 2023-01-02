-- Get system health history for the last X minutes (start from SQL2008)
DECLARE
	@MinutesBack	INT = 60;

-- [SQLProcessCPUUtilization %]		- Indicates the amount of CPU SQL Server was using at the time of the snapshot.
-- [OtherProcessCPUUtilization %]	- (100 minus ProcessUtilization minus System Idle) CPU being used by processes other than SQL Server.
-- [SystemCPUIdle %]				- Amount of Idle CPU that nothing is using. Available for any process that requires CPU.
-- [UserModeTime]					- Indicates the amount of CPU worker thread (Running in user mode) used during the period it did not yield. You need to divide this value by 10,000 to get time in milliseconds
-- [KernelModeTime]					- Indicates the amount of CPU worker thread (Running in Windows kernel) used during the period it did not yield. You need to divide this value by 10,000 to get time in milliseconds.
-- [PageFaults]						– Number of page faults at the time of the snapshot. A page fault occurs when a program requests an address on a page that is not in the current set of memory-resident pages.
-- [WorkingSetDelta]				- Difference in working set between last and current snapshot.
-- [SQLMAXMemoryUtilization %]		- Indicates the percentage of memory SQL Server is using based on max server memory (MB) setting. 100% is normal in this case as SQL OS is based on a greedy algorithm. It will consume all memory unless it is forced to give up memory due to external factors.

SELECT TOP(@MinutesBack)
	DATEADD(ms, -1 * (T.ts_now - A.[timestamp]), GETDATE())			AS [EventTime],
	A.[SQLProcessCPUUtilization %],
	(100 - A.[SystemCPUIdle %] - A.[SQLProcessCPUUtilization %])	AS [OtherProcessCPUUtilization %],
	A.[SystemCPUIdle %],
	A.[UserModeTime],
	A.[KernelModeTime],
	A.[PageFaults],
	A.[WorkingSetDelta],
	A.[SQLMAXMemoryUtilization %]
FROM ( 
	  SELECT
			[timestamp],
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'INT') 			AS [SQLProcessCPUUtilization %],
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'INT') 					AS [SystemCPUIdle %],
			record.value('(//Record/SchedulerMonitorEvent/SystemHealth/UserModeTime) [1]', 'BIGINT')			AS [UserModeTime],   
			record.value('(//Record/SchedulerMonitorEvent/SystemHealth/KernelModeTime) [1]', 'BIGINT')			AS [KernelModeTime],   
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/PageFaults)[1]', 'BIGINT')				AS [PageFaults],			
			record.value('(//Record/SchedulerMonitorEvent/SystemHealth/WorkingSetDelta) [1]', 'BIGINT')/1024	AS [WorkingSetDelta],
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/MemoryUtilization)[1]', 'INT') 			AS [SQLMAXMemoryUtilization %]
	  FROM ( 
			SELECT
				R.[timestamp],
				CONVERT(XML, R.record) AS [record] 
			FROM
				sys.dm_os_ring_buffers AS R
			WHERE
				R.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
				AND R.record LIKE '%SystemHealth%') AS x 
	  ) AS A
	  OUTER APPLY (SELECT cpu_ticks/(cpu_ticks/ms_ticks) AS ts_now FROM sys.dm_os_sys_info) AS T
ORDER BY
	[EventTime] DESC
OPTION (RECOMPILE, MAXDOP 1);