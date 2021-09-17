DECLARE @TableName sysname = 'MyTableName'

SELECT
	  OBJECT_SCHEMA_NAME(t.object_id)
	, t.object_id
	, t.name
	, p.index_id
	, ix.name
	, partition_number = p.partition_number
	, size_MB = CEILING(SUM(ISNULL(sps.in_row_data_page_count,0) + ISNULL(sps.row_overflow_used_page_count,0) + ISNULL(sps.lob_reserved_page_count,0)) / 128.0)
	, in_row_percent = ISNULL(
				FLOOR(SUM(ISNULL(sps.in_row_data_page_count,0)) * 1.0 
				/ NULLIF(SUM(ISNULL(sps.in_row_data_page_count,0) + ISNULL(sps.row_overflow_used_page_count,0) + ISNULL(sps.lob_reserved_page_count,0)),0)
				* 100.0), 0)
FROM sys.tables AS t WITH(NOLOCK)
INNER JOIN sys.partitions AS p WITH(NOLOCK) ON t.object_id = p.object_id AND p.data_compression = 0
INNER JOIN sys.indexes AS ix WITH(NOLOCK) ON ix.object_id = t.object_id AND ix.index_id = p.index_id
LEFT JOIN sys.dm_db_partition_stats AS sps WITH(NOLOCK) ON sps.partition_id = p.partition_id
WHERE 
    t.object_id = OBJECT_ID(@TableName)
-- Ignore indexes or tables with unsupported LOB/FILESTREAM columns
AND NOT EXISTS
(
SELECT NULL
FROM sys.columns AS c
INNER JOIN sys.types AS t 
ON c.system_type_id = t.system_type_id
AND c.user_type_id = t.user_type_id
LEFT JOIN sys.index_columns AS ixc
ON ixc.object_id = c.object_id
AND ixc.column_id = c.column_id
AND ix.index_id = ixc.index_id
WHERE (t.[name] in ('text', 'ntext', 'image') OR c.is_filestream = 1)
AND ix.object_id = c.object_id
AND (ix.index_id IN (0,1) OR ixc.index_id IS NOT NULL)
)
GROUP BY
	  t.object_id
	, t.name
	, p.index_id
	, ix.name
	, p.partition_number
OPTION (RECOMPILE);

SELECT
	  DB_NAME()
	, OBJECT_SCHEMA_NAME(p.object_id) AS schema_name
	, OBJECT_NAME(p.object_id) AS table_name
	, p.object_id
	, p.index_id
	, ix.name as index_name
	, p.partition_number
	, range_scans_percent = ISNULL(
				FLOOR(SUM(ISNULL(ios.range_scan_count,0)) * 1.0 /
				NULLIF(SUM(
					ISNULL(ios.range_scan_count,0) +
					ISNULL(ios.leaf_delete_count,0) + 
					ISNULL(ios.leaf_insert_count,0) + 
					ISNULL(ios.leaf_page_merge_count,0) + 
					ISNULL(ios.leaf_update_count,0) + 
					ISNULL(ios.singleton_lookup_count,0)
				), 0) * 100.0), 0)
	, updates_percent = ISNULL(
				CEILING(SUM(ISNULL(ios.leaf_update_count, 0)) * 1.0 /
				NULLIF(SUM(
					ISNULL(ios.range_scan_count,0) +
					ISNULL(ios.leaf_delete_count,0) + 
					ISNULL(ios.leaf_insert_count,0) + 
					ISNULL(ios.leaf_page_merge_count,0) + 
					ISNULL(ios.leaf_update_count,0) + 
					ISNULL(ios.singleton_lookup_count,0)
				), 0) * 100.0), 0)
FROM sys.partitions AS p WITH(NOLOCK)
INNER JOIN sys.indexes AS ix WITH(NOLOCK) ON p.object_id = ix.object_id AND p.index_id = ix.index_id
OUTER APPLY sys.dm_db_index_operational_stats(db_id(),p.object_id,p.index_id,p.partition_number) AS ios
WHERE p.object_id = OBJECT_ID(@TableName)
GROUP BY
	  p.object_id
	, p.index_id
	, ix.name
	, p.partition_number
OPTION (RECOMPILE, MAXDOP 1);


