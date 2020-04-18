/*========================================================================================================================

Description:	This script measures the IO latency for reads and writes for every database file in the instance
				The script captures a snapshot of sys.dm_io_virtual_file_stats, sleeps for 1 minute and compares
				the current state against the snapshot
Author:			Matan Yungman, http://www.madeirasql.com/how-to-measure-io-latency-for-database-files

		
=========================================================================================================================*/


IF OBJECT_ID('tempdb..#io') is not null
	DROP TABLE #io
GO
SELECT * INTO #io FROM sys.dm_io_virtual_file_stats(null,null)
WAITFOR DELAY '00:01:00'
SELECT 
	DB_NAME(a.database_id),
	a.file_id,
	a.num_of_reads-b.num_of_reads AS num_of_reads,
	a.num_of_writes-b.num_of_writes as num_of_writes,
	CASE 
		WHEN a.num_of_reads-b.num_of_reads > 0 
		THEN
		(a.io_stall_read_ms-b.io_stall_read_ms)/(a.num_of_reads-b.num_of_reads) 
		ELSE 0 
	END AS read_latency,
	CASE 
		WHEN 
		a.num_of_writes-b.num_of_writes > 0 
		THEN 
		(a.io_stall_write_ms-b.io_stall_write_ms)/(a.num_of_writes-b.num_of_writes) 
		ELSE 0 
	END AS write_latency
FROM #io b inner join sys.dm_io_virtual_file_stats(null,null) a
ON a.database_id = b.database_id and a.file_id = b.file_id
ORDER BY DB_NAME(a.database_id)
GO
