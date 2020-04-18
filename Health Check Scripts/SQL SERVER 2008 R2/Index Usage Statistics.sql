/*========================================================================================================================

Description:	Display index usage statistics
Scope:			Database
Author:			Guy Glantser
Created:		29/10/2013
Last Updated:	29/10/2013
Notes:			If an index is updated much more than it's read, then it's inefficient,
				and it should be considered to be altered or removed.
				Notice that this query display information for only non-clustered and non-unique indexes
				with more than 10,000 rows.

=========================================================================================================================*/

SELECT
	SchemaName		= Schemas.name ,
	TableName		= Tables.name ,
	IndexName		= Indexes.name ,
	IndexId			= Indexes.index_id ,
	NumberOfRows	= SUM (Partitions.rows) ,
	NumberOfReads	= ISNULL (IndexUsageStats.user_seeks , 0) + ISNULL (IndexUsageStats.user_scans , 0) ,
	NumberOfWrites	= ISNULL (IndexUsageStats.user_updates , 0) ,
	ReadWriteRatio	=
		CASE
			WHEN IndexUsageStats.user_updates > 0
				THEN CAST ((CAST ((ISNULL (IndexUsageStats.user_seeks , 0) + ISNULL (IndexUsageStats.user_scans , 0)) AS DECIMAL(19,2)) / CAST (ISNULL (IndexUsageStats.user_updates , 0) AS DECIMAL(19,2))) AS DECIMAL(19,2))
			ELSE
				CAST (0 AS DECIMAL(19,2))
		END
FROM
	sys.schemas AS Schemas
INNER JOIN
	sys.tables AS Tables
ON
	Schemas.schema_id = Tables.schema_id
INNER JOIN
	sys.indexes AS Indexes
ON
	Tables.object_id = Indexes.object_id
LEFT OUTER JOIN
	sys.dm_db_index_usage_stats AS IndexUsageStats
ON
	Indexes.object_id = IndexUsageStats.object_id
AND
	Indexes.index_id = IndexUsageStats.index_id
AND
	IndexUsageStats.database_id = DB_ID ()
INNER JOIN
	sys.partitions AS Partitions
ON
	Indexes.object_id = Partitions.object_id
AND
	Indexes.index_id = Partitions.index_id
WHERE 
	Indexes.type = 2	-- Non-Clustered
AND
	Indexes.is_unique = 0
AND
	Indexes.is_hypothetical = 0
GROUP BY
	Indexes.object_id ,
	Indexes.index_id ,
	Schemas.name ,
	Tables.name ,
	Indexes.name ,
	IndexUsageStats.user_seeks ,
	IndexUsageStats.user_scans ,
	IndexUsageStats.user_updates
HAVING
	SUM (Partitions.rows) > 10000
ORDER BY
	NumberOfReads ASC;
GO
