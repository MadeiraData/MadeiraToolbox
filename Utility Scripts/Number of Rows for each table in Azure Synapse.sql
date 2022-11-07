
/*********************************************************************************************************
Author:			Ben Hazan @Madeira
Created Date:	06-11-2022
Description:	This script retrieve how many rows has in each table in Azure Synapse Analytics
**********************************************************************************************************/

SELECT
    SchemaName        = Schemas.[name] ,
    TableName        = Tables.[name] ,
    NUmberOfRows    = SUM (NodePartitionStats.row_count)
FROM
    sys.schemas AS Schemas
INNER JOIN
    sys.tables AS Tables
ON
    Schemas.[schema_id] = Tables.[schema_id]
INNER JOIN
    sys.indexes AS Indexes
ON
    Tables.[object_id] = Indexes.[object_id]
AND
    Indexes.[index_id] <= 1
INNER JOIN
    sys.pdw_permanent_table_mappings AS TableMappings
ON
    Tables.[object_id] = TableMappings.[object_id]
INNER JOIN
    sys.pdw_nodes_tables AS NodesTables
ON
    TableMappings.[physical_name] = NodesTables.[name]
INNER JOIN
    sys.dm_pdw_nodes_db_partition_stats AS NodePartitionStats
ON
    NodesTables.[object_id] = NodePartitionStats.[object_id]
AND
    NodesTables.[pdw_node_id] = NodePartitionStats.[pdw_node_id]
AND
    NodesTables.[distribution_id] = NodePartitionStats.[distribution_id]
GROUP BY
    Schemas.[name] ,
    Tables.[name];
GO
