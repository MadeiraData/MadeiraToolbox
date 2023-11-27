/*
TempDB Space Utilization Check
==============================
Author: Eitan Blumin | https://www.madeiradata.com
Date: 2022-05-03
Description:
	Based on scripts available at the following resources:
	https://www.sqlshack.com/monitor-sql-server-tempdb-database/
	https://www.mssqltips.com/sqlservertip/4356/track-sql-server-tempdb-space-usage/
*/
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

-- File Stats Overview
SELECT
	instance_name			    AS [DatabaseName]
      , [Data File(s) Size (KB)] / 1024	    AS [Data file (MB)]
      , [LOG File(s) Size (KB)] / 1024	    AS [Log file (MB)]
      , [Log File(s) Used Size (KB)] / 1024 AS [Log file space used (MB)]
      , (
		SELECT SUM(size) / 128 FROM tempdb.sys.database_files
	)				    AS [Total database size (MB)]
FROM
(
	SELECT	*
	FROM	sys.dm_os_performance_counters
	WHERE
		counter_name IN
		(
			'Data File(s) Size (KB)', 'Log File(s) Size (KB)', 'Log File(s) Used Size (KB)'
		)
		AND instance_name = 'tempdb'
) AS A
PIVOT
(
	MAX(cntr_value)
	FOR counter_name IN
	(
		[Data File(s) Size (KB)], [LOG File(s) Size (KB)], [Log File(s) Used Size (KB)]
	)
) AS B;
GO

-- Object Types Overview
SELECT	(SUM(unallocated_extent_page_count) / 128)   AS [Free space (MB)]
      , SUM(internal_object_reserved_page_count) * 8 AS [Internal objects (KB)]
      , SUM(user_object_reserved_page_count) * 8     AS [User objects (KB)]
      , SUM(version_store_reserved_page_count) * 8   AS [Version store (KB)]
FROM	tempdb.sys.dm_db_file_space_usage
--database_id '2' represents tempdb
WHERE	database_id = 2;
GO

-- Temp Table Space Utilization Stats
SELECT
	tb.name			    AS [Temporary table name]
      , SUM(stt.row_count)		    AS [Number of rows]
      , SUM(stt.used_page_count) * 8	    AS [Used space (KB)]
      , SUM(stt.reserved_page_count) * 8 AS [Reserved space (KB)]
	  , tb.create_date AS [Create Date]
	  --, DropCommand = CONCAT(N'DROP TABLE ', tb.name)
FROM
	tempdb.sys.partitions		      AS prt
INNER	JOIN tempdb.sys.dm_db_partition_stats AS stt ON prt.partition_id = stt.partition_id
							     AND prt.partition_number = stt.partition_number
INNER	JOIN tempdb.sys.tables		      AS tb ON stt.object_id = tb.object_id
GROUP BY tb.name, tb.create_date
ORDER BY [Create Date] ASC, [Reserved space (KB)] DESC;
GO

-- Session Usage of TempDB
SELECT
	COALESCE(T1.session_id, T2.session_id)							     [session_id]
      , T1.request_id
      , COALESCE(T1.database_id, T2.database_id)						     [database_id]
      , COALESCE(T1.[Total Allocation User Objects], 0) + T2.[Total Allocation User Objects]	     [Total Allocation User Objects]
      , COALESCE(T1.[Net Allocation User Objects], 0) + T2.[Net Allocation User Objects]	     [Net Allocation User Objects]
      , COALESCE(T1.[Total Allocation Internal Objects], 0) + T2.[Total Allocation Internal Objects] [Total Allocation Internal Objects]
      , COALESCE(T1.[Net Allocation Internal Objects], 0) + T2.[Net Allocation Internal Objects]     [Net Allocation Internal Objects]
      , COALESCE(T1.[Total Allocation], 0) + T2.[Total Allocation]				     [Total Allocation]
      , COALESCE(T1.[Net Allocation], 0) + T2.[Net Allocation]					     [Net Allocation]
      , (SELECT COALESCE(T1.[Query Text], T2.[Query Text]) FOR XML PATH(''))   [Query Text]
	  , DB_NAME(ses.database_id)											[Database Name]
	  , ses.*
FROM
(
	SELECT
		TS.session_id
	      , TS.request_id
	      , TS.database_id
	      , CAST(TS.user_objects_alloc_page_count / 128 AS decimal(15, 2))						      [Total Allocation User Objects]
	      , CAST((TS.user_objects_alloc_page_count - TS.user_objects_dealloc_page_count) / 128 AS decimal(15, 2))	      [Net Allocation User Objects]
	      , CAST(TS.internal_objects_alloc_page_count / 128 AS decimal(15, 2))					      [Total Allocation Internal Objects]
	      , CAST((TS.internal_objects_alloc_page_count - TS.internal_objects_dealloc_page_count) / 128 AS decimal(15, 2)) [Net Allocation Internal Objects]
	      , CAST((TS.user_objects_alloc_page_count + internal_objects_alloc_page_count) / 128 AS decimal(15, 2))	      [Total Allocation]
	      , CAST((TS.user_objects_alloc_page_count + TS.internal_objects_alloc_page_count
		      - TS.internal_objects_dealloc_page_count - TS.user_objects_dealloc_page_count
		     ) / 128 AS decimal(15, 2))										      [Net Allocation]
	      , ISNULL(T.text, inpbuf.event_info)									      [Query Text]
	FROM
		sys.dm_db_task_space_usage		  TS
	INNER	JOIN sys.dm_exec_requests		  ER ON ER.request_id = TS.request_id AND ER.session_id = TS.session_id
	OUTER	APPLY sys.dm_exec_sql_text(ER.sql_handle) T
	OUTER	APPLY sys.dm_exec_input_buffer(ER.session_id, NULL) inpbuf

) T1
RIGHT	JOIN
(
	SELECT
		SS.session_id
	      , SS.database_id
	      , CAST(SS.user_objects_alloc_page_count / 128 AS decimal(15, 2))						      [Total Allocation User Objects]
	      , CAST((SS.user_objects_alloc_page_count - SS.user_objects_dealloc_page_count) / 128 AS decimal(15, 2))	      [Net Allocation User Objects]
	      , CAST(SS.internal_objects_alloc_page_count / 128 AS decimal(15, 2))					      [Total Allocation Internal Objects]
	      , CAST((SS.internal_objects_alloc_page_count - SS.internal_objects_dealloc_page_count) / 128 AS decimal(15, 2)) [Net Allocation Internal Objects]
	      , CAST((SS.user_objects_alloc_page_count + internal_objects_alloc_page_count) / 128 AS decimal(15, 2))	      [Total Allocation]
	      , CAST((SS.user_objects_alloc_page_count + SS.internal_objects_alloc_page_count
		      - SS.internal_objects_dealloc_page_count - SS.user_objects_dealloc_page_count
		     ) / 128 AS decimal(15, 2))										      [Net Allocation]
	      , ISNULL(T.text, inpbuf.event_info)									      [Query Text]
	FROM
		sys.dm_db_session_space_usage			      SS
	LEFT	JOIN sys.dm_exec_connections			      CN ON CN.session_id = SS.session_id
	OUTER	APPLY sys.dm_exec_sql_text(CN.most_recent_sql_handle) T
	OUTER	APPLY sys.dm_exec_input_buffer(SS.session_id, NULL) inpbuf
) T2	ON T1.session_id = T2.session_id
LEFT JOIN sys.dm_exec_sessions AS ses
ON ses.session_id = COALESCE(T1.session_id, T2.session_id)
ORDER BY
	[Total Allocation] DESC
      , [Total Allocation User Objects] DESC;
GO
