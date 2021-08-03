IF OBJECT_ID('tempdb..#t') IS NOT NULL DROP TABLE #t;

SELECT GETDATE() AS start_timestamp, * INTO #t FROM sys.dm_os_wait_stats

--sleep for one minute
WAITFOR DELAY '00:01:00';

SELECT t.start_timestamp, GETDATE() AS end_timestamp,
	w.wait_type,
	w.waiting_tasks_count-t.waiting_tasks_count as waiting_tasks_count,
	w.wait_time_ms-t.wait_time_ms as wait_time_ms,
	w.max_wait_time_ms-t.max_wait_time_ms as max_wait_time_ms,
	w.signal_wait_time_ms-t.signal_wait_time_ms as signal_wait_time_ms
FROM sys.dm_os_wait_stats w inner join #t t on w.wait_Type=t.wait_type
WHERE w.wait_time_ms > t.wait_time_ms
ORDER BY wait_time_ms DESC;
