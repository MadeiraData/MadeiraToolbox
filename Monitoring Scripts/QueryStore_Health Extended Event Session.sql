-- Get some metadata about query_store extended events

/*
select *
from sys.dm_xe_object_columns
where object_name like '%query_store%'
and name not in ('UUID','VERSION','CHANNEL','KEYWORD')
*/
/*
query_store_persist_on_shutdown_failed
query_store_background_task_creation_failed
query_store_background_task_initialization_failed
query_store_background_task_persist_failed
query_store_stmt_hash_map_over_memory_limit
query_store_buffered_items_over_memory_limit
query_store_buffered_items_memory_below_read_write_target
query_store_read_write_failed
query_store_database_initialization_failed
query_store_query_persistence_failure
query_store_plan_persistence_failure
query_store_flush_failed
query_store_severe_error_shutdown
query_store_shutdown_in_error_state_started
query_store_shutdown_in_error_state_finished
query_store_schema_consistency_check_failure
query_store_aprc_error
query_store_auto_enable_failure
query_store_disk_size_check_failed
query_store_catch_exception
*/
GO

-- Create extended event for query store events:

CREATE EVENT SESSION [QueryStore_Health] ON SERVER 
ADD EVENT qds.query_store_aprc_error,
ADD EVENT qds.query_store_auto_enable_failure,
ADD EVENT qds.query_store_background_cleanup_task_failed,
ADD EVENT qds.query_store_background_task_creation_failed,
ADD EVENT qds.query_store_background_task_initialization_failed,
ADD EVENT qds.query_store_background_task_persist_failed,
ADD EVENT qds.query_store_buffered_items_memory_below_read_write_target,
ADD EVENT qds.query_store_buffered_items_over_memory_limit,
ADD EVENT qds.query_store_check_consistency_init_failed,
ADD EVENT qds.query_store_database_initialization_failed,
ADD EVENT qds.query_store_flush_failed,
ADD EVENT qds.query_store_persist_task_init_failed,
ADD EVENT qds.query_store_plan_persistence_failure,
ADD EVENT qds.query_store_query_persistence_failure,
ADD EVENT qds.query_store_read_write_failed,
ADD EVENT qds.query_store_schema_consistency_check_failure,
ADD EVENT qds.query_store_severe_error_shutdown,
ADD EVENT qds.query_store_shutdown_in_error_state_finished,
ADD EVENT qds.query_store_shutdown_in_error_state_started,
ADD EVENT qds.query_store_stmt_hash_map_over_memory_limit,
ADD EVENT sqlserver.query_store_persist_on_shutdown_failed,
ADD EVENT qds.query_store_catch_exception,
ADD EVENT qds.query_store_disk_size_check_failed
ADD TARGET package0.event_file(SET filename=N'QueryStore_Health.xel',max_file_size=(100),max_rollover_files=(5))
WITH (MAX_MEMORY=8 MB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=ON)
GO

-- Querying Event Data:

DECLARE @FileName NVARCHAR(4000)
SELECT @FileName = target_data.value('(EventFileTarget/File/@name)[1]','nvarchar(4000)')
FROM (SELECT CAST(target_data AS XML) target_data FROM sys.dm_xe_sessions s
JOIN sys.dm_xe_session_targets t ON s.address = t.event_session_address
WHERE s.name LIKE N'QueryStore_Health%') ft
 
SELECT
	*
FROM
(
	SELECT
	object_name
	, XEData.value('(event/@timestamp)[1]','datetime2(3)') AS event_timestamp
	, XEData.query('event/data') AS event_data
	FROM (
	SELECT CAST(event_data AS XML) XEData, *
	FROM sys.fn_xe_file_target_read_file(@FileName, NULL, NULL, NULL)
	--WHERE @FileName IS NOT NULL
	) event_data
	--WHERE XEData.value('(event/@timestamp)[1]','datetime2(3)') > DATEADD(minute, -30, GETUTCDATE())
) AS a
GO
