SELECT
	DB_NAME(UsageStats.database_id) AS DBName,
	OBJECT_NAME(Indexes.object_id) AS ObjectName,
	Indexes.name AS IndexName,
	PartitionStats.row_count AS RCount,
	UsageStats.*
FROM
	sys.dm_db_index_usage_stats AS UsageStats
INNER JOIN
	sys.indexes Indexes
ON Indexes.object_id = UsageStats.object_id
AND Indexes.index_id = UsageStats.index_id
INNER JOIN
	sys.dm_db_partition_stats PartitionStats
ON PartitionStats.object_id = UsageStats.object_id
AND PartitionStats.index_id = UsageStats.index_id
WHERE
	UsageStats.user_scans = 0
AND UsageStats.user_seeks = 0
AND PartitionStats.row_count > 100000
AND Indexes.type_desc <> 'CLUSTERED'
AND Indexes.is_primary_key = 0
