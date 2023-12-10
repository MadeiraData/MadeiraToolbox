DECLARE @TableName sysname = 'MyTableName'



DECLARE @SqlStartTime DATETIME, @UpTimeDays INT, @SqlStartTimeString VARCHAR(25);
SELECT @SqlStartTime = sqlserver_start_time FROM sys.dm_os_sys_info;
SET @UpTimeDays = DATEDIFF(dd, @SqlStartTime, GETDATE())
SET @SqlStartTimeString = CONVERT(varchar(25), @SqlStartTime, 121)

RAISERROR(N'--- SQL Server is operational since %s (~%d days)', 0, 1, @SqlStartTimeString, @UpTimeDays) WITH NOWAIT;


SELECT
	  database_name = DB_NAME()
	, schema_name = OBJECT_SCHEMA_NAME(p.object_id)
	, table_name = OBJECT_NAME(p.object_id)
	, p.object_id
	, p.index_id
	, index_name = ix.name
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
	, size_MB = CEILING(SUM(ISNULL(sps.in_row_data_page_count,0) + ISNULL(sps.row_overflow_used_page_count,0) + ISNULL(sps.lob_reserved_page_count,0)) / 128.0)
	, in_row_percent = ISNULL(
				FLOOR(SUM(ISNULL(sps.in_row_data_page_count,0)) * 1.0 
				/ NULLIF(SUM(ISNULL(sps.in_row_data_page_count,0) + ISNULL(sps.row_overflow_used_page_count,0) + ISNULL(sps.lob_reserved_page_count,0)),0)
				* 100.0), 0)
	, row_estimation_check = N'EXEC ' + QUOTENAME(DB_NAME()) + '.sys.sp_estimate_data_compression_savings ' + N'
						 @schema_name		= ''' + OBJECT_SCHEMA_NAME(p.object_id) + N''',  
						 @object_name		= ''' + OBJECT_NAME(p.object_id) + N''',
						 @index_id		= ' + CONVERT(nvarchar(max), p.index_id) + N',
						 @partition_number	= ' + CONVERT(nvarchar(max), p.partition_number) + N',   
						 @data_compression	= ''ROW'';'
	, row_rebuild_command		= N'USE ' + QUOTENAME(DB_NAME()) + N'; ALTER ' + ISNULL(N'INDEX ' + QUOTENAME(ix.name) + N' ON ', N'TABLE ') + QUOTENAME(OBJECT_SCHEMA_NAME(p.object_id)) + '.' + QUOTENAME(OBJECT_NAME(p.object_id)) 
				+ N' REBUILD PARTITION = ' + ISNULL(CONVERT(nvarchar(max),p.partition_number), N'ALL') 
				+ N' WITH (DATA_COMPRESSION = ROW);'
	, page_estimation_check = N'EXEC ' + QUOTENAME(DB_NAME()) + '.sys.sp_estimate_data_compression_savings ' + N'
						 @schema_name		= ''' + OBJECT_SCHEMA_NAME(p.object_id) + N''',  
						 @object_name		= ''' + OBJECT_NAME(p.object_id) + N''',
						 @index_id		= ' + CONVERT(nvarchar(max), p.index_id) + N',
						 @partition_number	= ' + CONVERT(nvarchar(max), p.partition_number) + N',   
						 @data_compression	= ''PAGE'';'
	, page_rebuild_command		= N'USE ' + QUOTENAME(DB_NAME()) + N'; ALTER ' + ISNULL(N'INDEX ' + QUOTENAME(ix.name) + N' ON ', N'TABLE ') + QUOTENAME(OBJECT_SCHEMA_NAME(p.object_id)) + '.' + QUOTENAME(OBJECT_NAME(p.object_id)) 
				+ N' REBUILD PARTITION = ' + ISNULL(CONVERT(nvarchar(max),p.partition_number), N'ALL') 
				+ N' WITH (DATA_COMPRESSION = PAGE);'
FROM sys.partitions AS p WITH(NOLOCK)
INNER JOIN sys.indexes AS ix WITH(NOLOCK) ON p.object_id = ix.object_id AND p.index_id = ix.index_id
OUTER APPLY sys.dm_db_index_operational_stats(db_id(),p.object_id,p.index_id,p.partition_number) AS ios
LEFT JOIN sys.dm_db_partition_stats AS sps WITH(NOLOCK) ON sps.partition_id = p.partition_id
WHERE p.object_id = OBJECT_ID(@TableName)
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
	  p.object_id
	, p.index_id
	, ix.name
	, p.partition_number
ORDER BY
	size_MB DESC
OPTION (RECOMPILE, MAXDOP 1);
