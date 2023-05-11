DECLARE
  @MinAverageTotalUserCost int = 5
, @MinAverageUserImpact int = 65
, @MinSeeksOrScansPerDay int = 100
, @MinUniqueCompiles int = 20

DECLARE @daysUptime int;
SELECT @daysUptime = DATEDIFF(day,sqlserver_start_time,GETDATE()) FROM sys.dm_os_sys_info;

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
, RunThisForMoreDetails = N'USE ' + QUOTENAME(DB_NAME(database_id)) 
+ N'; EXEC sp_help ' + QUOTENAME(QUOTENAME(OBJECT_SCHEMA_NAME(mid.object_id, database_id)) + N'.' + QUOTENAME(OBJECT_NAME(mid.object_id, database_id)), '''')
+ N'; EXEC sp_spaceused ' + QUOTENAME(QUOTENAME(OBJECT_SCHEMA_NAME(mid.object_id, database_id)) + N'.' + QUOTENAME(OBJECT_NAME(mid.object_id, database_id)), '''')
+ N'; EXEC sp_indexes_rowset @table_schema = ' + QUOTENAME(OBJECT_SCHEMA_NAME(mid.object_id, database_id), '''') + N', @table_name = ' + QUOTENAME(OBJECT_NAME(mid.object_id, database_id), '''')
+ N';'
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
WHERE migs.avg_total_user_cost >= @MinAverageTotalUserCost
AND migs.avg_user_impact >= @MinAverageUserImpact
AND (migs.user_seeks+migs.user_scans) / @daysUptime >= @MinSeeksOrScansPerDay
AND migs.unique_compiles >= @MinUniqueCompiles
AND database_id > 4
AND DB_NAME(database_id) NOT IN ('SSISDB', 'ReportServer', 'ReportServerTempDB', 'distribution')
ORDER BY index_advantage DESC, migs.avg_user_impact DESC
OPTION (RECOMPILE);
