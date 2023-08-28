DECLARE
	  @PartitionLevel				bit		= 1		-- optionally set to 1 to view details per partition number
	, @FilterByPartitionFunction	sysname	= NULL	-- optionally filter by a specific partition function name

SELECT DISTINCT
  database_name				= DB_NAME()
, partition_function		= pf.name
, partition_function_type	= pf.type_desc
, boundary_side				= CASE WHEN pf.boundary_value_on_right = 1 THEN 'RIGHT' ELSE 'LEFT' END
, total_partitions_fanout	= pf.fanout
, partition_scheme			= ps.name
, column_type				= tp.name
, params.max_length, params.precision, params.scale, params.collation_name
, table_schema_name			= OBJECT_SCHEMA_NAME(c.object_id)
, table_name				= OBJECT_NAME(c.object_id)
, index_name				= ix.name
, column_name				= c.name
, partition_number			= CASE WHEN @PartitionLevel = 1 THEN ISNULL(rv.boundary_id, p.partition_number) END
, partition_range_value		= CASE WHEN @PartitionLevel = 1 THEN rv.value END
, [rows]					= CASE WHEN @PartitionLevel = 1 THEN p.rows END
, partition_filegroup		= CASE WHEN @PartitionLevel = 1 THEN fg.name END
FROM sys.partition_schemes AS ps
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.partition_parameters AS params ON params.function_id = pf.function_id
INNER JOIN sys.types AS tp ON params.system_type_id = tp.system_type_id AND params.user_type_id = tp.user_type_id
LEFT JOIN sys.indexes AS ix ON ix.data_space_id = ps.data_space_id
LEFT JOIN sys.partitions AS p ON  p.object_id = ix.object_id AND p.index_id = ix.index_id
LEFT JOIN sys.index_columns AS ic ON ic.object_id = p.object_id AND ic.index_id = p.index_id AND ic.partition_ordinal > 0
LEFT JOIN sys.columns AS c ON c.object_id = p.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.partition_range_values AS rv ON rv.function_id = pf.function_id AND rv.boundary_id = p.partition_number
LEFT JOIN sys.destination_data_spaces dds ON rv.boundary_id = dds.destination_id AND ps.data_space_id = dds.partition_scheme_id
LEFT JOIN sys.filegroups AS fg ON dds.data_space_id = fg.data_space_id
WHERE
	(@FilterByPartitionFunction IS NULL OR pf.name = @FilterByPartitionFunction)
ORDER BY
	partition_function, partition_scheme, table_schema_name, table_name, index_name, partition_number
OPTION(RECOMPILE)
