/*========================================================================================================================

Description:	Display enhanced index information
Scope:			Database
Author:			Guy Glantser
Created:		30/05/2018
Last Updated:	02/09/2020
Notes:			Displays information for all the indexes in the current databases,
				including indexes on tables and views.
				The information is displayed at the partition level.

=========================================================================================================================*/

SELECT
	SchemaName			= SCHEMA_NAME (Objects.schema_id) ,
	ObjectName			= Objects.[name] ,
	ObjectId			= Objects.object_id ,
	ObjectType			=
		CASE Objects.[type]
			WHEN 'U'
				THEN N'Table'
			WHEN 'V'
				THEN N'View'
		END ,
	IndexId				= Indexes.index_id ,
	IndexName			= Indexes.[name] ,
	IndexType			= Indexes.[type_desc] ,
	ConstraintType		=
		CASE
			WHEN Indexes.is_primary_key = 1
				THEN N'Primary Key'
			WHEN Indexes.is_unique_constraint = 1
				THEN N'Unique Constraint'
			WHEN Indexes.is_unique = 1
				THEN N'Unique Index'
			ELSE
				N''
		END ,
	FilterDefinition	= Indexes.filter_definition ,
	IndexKeys			=
		STRING_AGG
		(
			CASE
				WHEN IndexColumns.is_included_column = 0
					THEN Columns.[name]
				ELSE
					NULL
			END ,
			N' , '
		)
		WITHIN GROUP (ORDER BY IndexColumns.key_ordinal ASC) ,
	IncludedColumns		=
		STRING_AGG
		(
			CASE
				WHEN IndexColumns.is_included_column = 1
					THEN Columns.[name]
				ELSE
					NULL
			END ,
			N' , '
		)
		WITHIN GROUP (ORDER BY IndexColumns.key_ordinal ASC) ,
	PartitionNumber		= IndexPhysicalStats.partition_number ,
	NumberOfRows		= Partitions.[rows] ,
	NumberOfPages		= IndexPhysicalStats.page_count ,
	Fragmentation		= FORMAT (IndexPhysicalStats.avg_fragmentation_in_percent / 100.0 , 'P') ,
	IndexFillFactor		= Indexes.fill_factor ,
	UsageLevel			=
		CASE
			WHEN ISNULL (IndexUsageStats.user_seeks , 0) + ISNULL (IndexUsageStats.user_scans , 0) + ISNULL (IndexUsageStats.user_lookups , 0) + ISNULL (IndexUsageStats.user_updates , 0) = 0
				THEN N'None'
			WHEN CAST ((ISNULL (IndexUsageStats.user_seeks , 0) + ISNULL (IndexUsageStats.user_scans , 0) + ISNULL (IndexUsageStats.user_lookups , 0)) AS DECIMAL(19,2)) / CAST ((ISNULL (IndexUsageStats.user_seeks , 0) + ISNULL (IndexUsageStats.user_scans , 0) + ISNULL (IndexUsageStats.user_lookups , 0) + ISNULL (IndexUsageStats.user_updates , 0)) AS DECIMAL(19,2)) <= 0.1
				THEN N'Low'
			ELSE
				N'High'
		END
FROM
	sys.objects AS Objects
INNER JOIN
	sys.indexes AS Indexes
ON
	Objects.object_id = Indexes.object_id
INNER JOIN
	sys.index_columns AS IndexColumns
ON
	Indexes.object_id = IndexColumns.object_id
AND
	Indexes.index_id = IndexColumns.index_id
INNER JOIN
	sys.columns AS Columns
ON
	IndexColumns.object_id = Columns.object_id
AND
	IndexColumns.column_id = Columns.column_id
INNER JOIN
	sys.partitions AS Partitions
ON
	Indexes.object_id = Partitions.object_id
AND
	Indexes.index_id = Partitions.index_id
INNER JOIN
	sys.dm_db_index_physical_stats (DB_ID () , NULL , NULL , NULL , N'LIMITED') AS IndexPhysicalStats
ON
	Partitions.object_id = IndexPhysicalStats.object_id
AND
	Partitions.index_id = IndexPhysicalStats.index_id
AND
	Partitions.partition_number = IndexPhysicalStats.partition_number
LEFT OUTER JOIN
	sys.dm_db_index_usage_stats AS IndexUsageStats
ON
	Indexes.object_id = IndexUsageStats.object_id
AND
	Indexes.index_id = IndexUsageStats.index_id
WHERE
	Objects.[type] IN ('U' , 'V')	-- Table or view
AND
	Indexes.index_id > 0
AND
	Indexes.is_disabled = 0
AND
	Indexes.is_hypothetical = 0
GROUP BY
	Objects.schema_id ,
	Objects.name ,
	Objects.object_id ,
	Objects.type ,
	Indexes.index_id ,
	Indexes.name ,
	Indexes.type_desc ,
	Indexes.is_primary_key ,
	Indexes.is_unique_constraint ,
	Indexes.is_unique ,
	Indexes.filter_definition ,
	IndexPhysicalStats.partition_number ,
	Partitions.[rows] ,
	IndexPhysicalStats.page_count ,
	IndexPhysicalStats.avg_fragmentation_in_percent ,
	Indexes.fill_factor ,
	IndexUsageStats.user_seeks ,
	IndexUsageStats.user_scans ,
	IndexUsageStats.user_lookups ,
	IndexUsageStats.user_updates
ORDER BY
	SchemaName	ASC ,
	ObjectName	ASC ,
	IndexId		ASC;
GO
