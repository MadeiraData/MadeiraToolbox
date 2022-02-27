SELECT 
dateadd (ms, rbf.[timestamp] - tme.ms_ticks, GETDATE()) as [Notification_Time],
cast(record as xml).value('(//SPID)[1]', 'bigint') as SPID,
cast(record as xml).value('(//ErrorCode)[1]', 'varchar(255)') as Error_Code,
cast(record as xml).value('(//CallingAPIName)[1]', 'varchar(255)') as [CallingAPIName],
cast(record as xml).value('(//APIName)[1]', 'varchar(255)') as [APIName],
cast(record as xml).value('(//Record/@id)[1]', 'bigint') AS [Record Id],
cast(record as xml).value('(//Record/@type)[1]', 'varchar(30)') AS [Type],
cast(record as xml).value('(//Record/@time)[1]', 'bigint') AS [Record Time],tme.ms_ticks as [Current Time]
from sys.dm_os_ring_buffers rbf
cross join sys.dm_os_sys_info tme
where rbf.ring_buffer_type = 'RING_BUFFER_SECURITY_ERROR'
ORDER BY rbf.timestamp ASC