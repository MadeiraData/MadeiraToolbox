DECLARE @daysUptime int;
SELECT @daysUptime = DATEDIFF(day,sqlserver_start_time,GETDATE()) FROM sys.dm_os_sys_info;

SELECT MessageText = CONCAT('In Server: ', @@SERVERNAME, ', Database: ', QUOTENAME([database_name]), ', Table ' + QUOTENAME([schema_name])+ '.' + QUOTENAME(table_name)
, ': Found ', total_missing_indexes, ' missing index(es) with high impact ('
, total_impact, ' total impact, ', total_cost, ' total cost, ', total_unique_compiles, ' total unique compiles)'
), total_impact
FROM (
 SELECT
     database_name = DB_NAME(database_id)
   , schema_name = OBJECT_SCHEMA_NAME (dm_mid.object_id, database_id)
   , table_name  = OBJECT_NAME (dm_mid.object_id, database_id)
   , total_missing_indexes = count(*)
   , total_impact = sum(dm_migs.avg_user_impact)
   , total_cost  = sum(dm_migs.avg_total_user_cost)
   , total_unique_compiles = sum(dm_migs.unique_compiles)
   --, total_user_seeks = sum(dm_migs.user_seeks)
   --, total_user_scans = sum(dm_migs.user_scans)
 FROM sys.dm_db_missing_index_groups dm_mig 
 INNER JOIN sys.dm_db_missing_index_group_stats dm_migs 
 ON dm_migs.group_handle = dm_mig.index_group_handle 
 INNER JOIN sys.dm_db_missing_index_details dm_mid
 ON dm_mig.index_handle = dm_mid.index_handle  
 WHERE 
      dm_migs.avg_total_user_cost > 5
  AND dm_migs.avg_user_impact > 65
  AND (dm_migs.user_seeks+dm_migs.user_scans) / @daysUptime > 1000
  AND dm_migs.unique_compiles > 20
  AND database_id > 4
  AND DB_NAME(database_id) NOT IN ('SSISDB', 'ReportServer', 'ReportServerTempDB', 'distribution', 'HangFireScheduler')
 GROUP BY database_id, dm_mid.object_id
) AS q
WHERE total_cost > 20
AND @daysUptime > 7
ORDER BY total_cost DESC, total_impact DESC