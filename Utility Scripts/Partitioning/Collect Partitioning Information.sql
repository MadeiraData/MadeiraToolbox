SELECT
 DB_NAME() AS database_name
, pf.name AS partition_function
, pf.type_desc AS partition_function_type
, pf.boundary_value_on_right
, pf.fanout
, ps.name AS partition_scheme
, fg.name AS partition_filegroup
, OBJECT_SCHEMA_NAME(c.object_id) AS table_schema_name
, OBJECT_NAME(c.object_id) AS table_name
, ix.name AS index_name
, c.name AS column_name
, tp.name AS column_type
, c.max_length, c.precision, c.scale, c.collation_name
, rv.boundary_id, rv.value, p.rows
FROM sys.partitions AS p
INNER JOIN sys.indexes AS ix ON p.object_id = ix.object_id AND p.index_id = ix.index_id
INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id AND ps.data_space_id = dds.partition_scheme_id
INNER JOIN sys.filegroups AS fg ON dds.data_space_id = fg.data_space_id
INNER JOIN sys.partition_range_values AS rv ON rv.function_id = pf.function_id AND rv.boundary_id = p.partition_number
INNER JOIN sys.index_columns AS ic ON ic.object_id = p.object_id AND ic.index_id = ix.index_id AND ic.partition_ordinal > 0
INNER JOIN sys.columns AS c ON c.object_id = p.object_id AND c.column_id = ic.column_id
INNER JOIN sys.types AS tp ON c.system_type_id = tp.system_type_id AND c.user_type_id = tp.user_type_id
--WHERE pf.name = 'MyPartitionFunctionName'
ORDER BY partition_function, partition_scheme, table_schema_name, table_name, index_name, boundary_id
