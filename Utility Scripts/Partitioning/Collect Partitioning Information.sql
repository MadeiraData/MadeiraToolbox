SELECT
 DB_NAME() AS database_name
, pf.name AS partition_function
, pf.type_desc AS partition_function_type
, pf.boundary_value_on_right
, pf.fanout
, ps.name AS partition_scheme
, fg.name AS partition_filegroup
, tp.name AS column_type
, params.max_length, params.precision, params.scale, params.collation_name
, rv.boundary_id AS partition_number, rv.value AS partition_range_value
, OBJECT_SCHEMA_NAME(c.object_id) AS table_schema_name
, OBJECT_NAME(c.object_id) AS table_name
, ix.name AS index_name
, c.name AS column_name
, p.rows
FROM sys.partition_schemes AS ps
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.partition_range_values AS rv ON rv.function_id = pf.function_id
INNER JOIN sys.partition_parameters AS params ON params.function_id = pf.function_id
INNER JOIN sys.types AS tp ON params.system_type_id = tp.system_type_id AND params.user_type_id = tp.user_type_id
LEFT JOIN sys.indexes AS ix ON ix.data_space_id = ps.data_space_id
LEFT JOIN sys.partitions AS p ON rv.boundary_id = p.partition_number AND p.object_id = ix.object_id AND p.index_id = ix.index_id
LEFT JOIN sys.index_columns AS ic ON ic.object_id = p.object_id AND ic.index_id = p.index_id AND ic.partition_ordinal > 0
LEFT JOIN sys.columns AS c ON c.object_id = p.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.destination_data_spaces dds ON rv.boundary_id = dds.destination_id AND ps.data_space_id = dds.partition_scheme_id
LEFT JOIN sys.filegroups AS fg ON dds.data_space_id = fg.data_space_id
--WHERE pf.name = 'MyPartitionFunctionName'
ORDER BY partition_function, partition_scheme, table_schema_name, table_name, index_name, boundary_id
