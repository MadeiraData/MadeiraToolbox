DECLARE
	  @PartitionLevel				bit		= 1		-- optionally set to 1 to view details per partition number
	, @FilterByPartitionFunction	sysname	= NULL	-- optionally filter by a specific partition function name
	, @FilterByTableName			sysname	= NULL	-- optionally filter by a specific table name

SELECT DISTINCT
  database_name				= DB_NAME()
, partition_function		= pf.name
, partition_function_type	= pf.type_desc
, boundary_side				= CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END
, total_partitions_fanout	= pf.fanout
, partition_scheme			= ps.name
, column_type				= tp.name
, params.max_length, params.precision, params.scale, params.collation_name
, table_schema_name			= sc.name
, table_name				= so.name
, index_name				= ix.name
, column_name				= c.name
, boundary_id				= CASE WHEN @PartitionLevel = 1 THEN prv.boundary_id END
, partition_number			= CASE WHEN @PartitionLevel = 1 THEN p.partition_number END
, partition_range_value		= CASE WHEN @PartitionLevel = 1 THEN prv.value END
, [rows]					= CASE WHEN @PartitionLevel = 1 THEN p.rows END
, [in_row_MB]				= CASE WHEN @PartitionLevel = 1 THEN stat.in_row_reserved_page_count * 8./1024. END
, [LOB_MB]					= CASE WHEN @PartitionLevel = 1 THEN stat.lob_reserved_page_count * 8./1024. END
, partition_filegroup		= CASE WHEN @PartitionLevel = 1 THEN fg.name END
FROM sys.partition_functions AS pf
INNER JOIN sys.partition_parameters AS params ON params.function_id = pf.function_id
INNER JOIN sys.types AS tp ON params.system_type_id = tp.system_type_id AND params.user_type_id = tp.user_type_id
INNER JOIN sys.partition_schemes as ps on ps.function_id=pf.function_id
INNER JOIN sys.indexes as si on si.data_space_id=ps.data_space_id
INNER JOIN sys.objects as so on si.object_id = so.object_id
INNER JOIN sys.schemas as sc on so.schema_id = sc.schema_id
INNER JOIN sys.partitions as p on si.object_id=p.object_id and si.index_id=p.index_id
LEFT JOIN sys.indexes AS ix ON ix.data_space_id = ps.data_space_id
LEFT JOIN sys.index_columns AS ic ON ic.object_id = p.object_id AND ic.index_id = p.index_id AND ic.partition_ordinal > 0
LEFT JOIN sys.columns AS c ON c.object_id = p.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.partition_range_values as prv on prv.function_id=pf.function_id AND p.partition_number= 
		CASE pf.boundary_value_on_right WHEN 1
			THEN prv.boundary_id + 1
		ELSE prv.boundary_id
		END
		/* For left-based functions, partition_number = boundary_id, 
		   for right-based functions we need to add 1 */
INNER JOIN sys.dm_db_partition_stats as stat on stat.object_id=p.object_id AND stat.index_id=p.index_id AND stat.index_id=p.index_id and stat.partition_id=p.partition_id AND stat.partition_number=p.partition_number
INNER JOIN sys.allocation_units as au on au.container_id = p.hobt_id AND au.type_desc ='IN_ROW_DATA' 
		/* Avoiding double rows for columnstore indexes. */
		/* We can pick up LOB page count from partition_stats */
INNER JOIN sys.filegroups as fg on fg.data_space_id = au.data_space_id
WHERE
	(@FilterByPartitionFunction IS NULL OR pf.name = @FilterByPartitionFunction)
	AND (@FilterByTableName IS NULL OR OBJECT_ID(@FilterByTableName) = so.object_id)
ORDER BY
	partition_function, partition_scheme, table_schema_name, table_name, index_name, partition_number
OPTION(RECOMPILE)
