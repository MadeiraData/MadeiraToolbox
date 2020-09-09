SELECT 
CONVERT(decimal(18,2), user_seeks * avg_total_user_cost * (avg_user_impact * 0.01)) AS [index_advantage],
FORMAT(migs.last_user_seek, 'yyyy-MM-dd HH:mm:ss') AS [last_user_seek],
DB_NAME(mid.database_id) AS [Database], 
mid.[statement] AS [Database.Schema.Table],
COUNT(1) OVER(PARTITION BY mid.[statement]) AS [missing_indexes_for_table],
COUNT(1) OVER(PARTITION BY mid.[statement], equality_columns) AS [similar_missing_indexes_for_table],
mid.equality_columns, mid.inequality_columns, mid.included_columns,
migs.unique_compiles, migs.user_seeks, 
CONVERT(decimal(18,2), migs.avg_total_user_cost) AS [avg_total_user_cost], migs.avg_user_impact 
, UserUpdates		= ISNULL(UsageStats.user_updates, 0) 
, UserScans		= ISNULL(UsageStats.user_scans, 0) 
, LastUpdate		= UsageStats.last_user_update 
, LastScan		= UsageStats.last_user_scan 
, TotalRows		= ISNULL(PartitionStats.TotalRows, 0)
, RemediationScript = CONCAT(
 N'CREATE NONCLUSTERED INDEX [IX_rename_me_'
 , mig.index_handle
 ,'] ON '
 ,mid.[statement]
 ,' ( '
 ,ISNULL(mid.equality_columns, mid.inequality_columns)
 ,' )'
 , CASE WHEN (mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL)
 THEN N' INCLUDE ( ' + mid.inequality_columns
 ELSE N''
 END
 , CASE WHEN mid.included_columns IS NOT NULL AND (mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL)
 THEN N', ' + mid.included_columns
 WHEN mid.included_columns IS NOT NULL
 THEN N' INCLUDE ( ' + mid.included_columns
 ELSE N''
 END
 , CASE WHEN mid.included_columns IS NOT NULL OR (mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL)
 THEN N' )'
 ELSE N''
 END
 )
FROM sys.dm_db_missing_index_group_stats AS migs WITH (NOLOCK)
INNER JOIN sys.dm_db_missing_index_groups AS mig WITH (NOLOCK)
ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid WITH (NOLOCK)
ON mig.index_handle = mid.index_handle
OUTER APPLY
(
	SELECT
		us.user_updates ,
		us.user_scans ,
		us.last_user_update ,
		us.last_user_scan
	FROM
		sys.dm_db_index_usage_stats AS us
	WHERE
		us.database_id = DB_ID()
	AND	us.object_id = mid.object_id
	AND	us.index_id <= 1
) AS UsageStats
OUTER APPLY
(
	SELECT
		TotalRows = SUM(p.rows)
	FROM
		sys.partitions AS p
	WHERE
		p.object_id = mid.object_id
	AND	p.index_id <= 1
) AS PartitionStats
ORDER BY index_advantage DESC, migs.avg_user_impact DESC
OPTION (RECOMPILE);
