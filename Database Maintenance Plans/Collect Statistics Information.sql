/*========================================================================================================================

Description:	Display information about all the statistics on all user tables and views in the current database
Scope:			Database
Author:			Guy Glantser
Created:		11/09/2020
Last Updated:	11/09/2020
Notes:			Use this information to plan a maintenance plan to update statistics for the user databases in the instance.
				Notice that this script only works in SQL Server 2017 (14.x) and later as well as in Azure SQL Database
				due to the use of the STRING_AGG function.

=========================================================================================================================*/

SELECT
	SchemaName				= SCHEMA_NAME (Objects.schema_id) ,
	ObjectName				= Objects.[name] ,
	ObjectId				= Objects.object_id ,
	ObjectType				=
		CASE Objects.[type]
			WHEN 'U'
				THEN N'Table'
			WHEN 'V'
				THEN N'View'
		END ,
	StatsId					= StatsTable.stats_id ,
	StatsName				= StatsTable.[name] ,
	StatsType				=
		CASE
			WHEN StatsTable.auto_created = 0 AND StatsTable.user_created = 0
				THEN N'Index Stats'
			ELSE
				N'Column Stats'
		END ,
	AutoCreated				= StatsTable.auto_created ,
	UserCreated				= StatsTable.user_created ,
	IsNoRecompute			= StatsTable.no_recompute ,
	FilterDefinition		= StatsTable.filter_definition ,
	IsTemporary				= StatsTable.is_temporary ,						-- Applies to: SQL Server 2012 (11.x) and later
	IsIncremental			= StatsTable.is_incremental ,					-- Applies to: SQL Server 2014 (12.x) and later
	LastUpdated				= StatsProperties.last_updated ,
	NumberOfRows			= StatsProperties.[rows] ,
	RowsSampled				= StatsProperties.rows_sampled ,
	SamplePercent			= FORMAT (CAST (StatsProperties.rows_sampled AS DECIMAL(19,2)) / CAST (StatsProperties.[rows] AS DECIMAL(19,2)) , 'P') ,
	PersistedSamplePercent	= StatsProperties.persisted_sample_percent ,	-- Applies to: SQL Server 2016 (13.x) SP1 CU4
	UnfilteredRows			= StatsProperties.unfiltered_rows ,
	NumberOfSteps			= StatsProperties.steps ,
	ModificationCounter		= StatsProperties.modification_counter ,
	ColumnNames				= STRING_AGG (ColumnsTable.[name] , N',') WITHIN GROUP (ORDER BY StatsColumnsTable.stats_column_id ASC)
FROM
	sys.objects AS Objects
INNER JOIN
	sys.stats AS StatsTable
ON
	Objects.object_id = StatsTable.object_id
INNER JOIN
	sys.stats_columns AS StatsColumnsTable
ON
	StatsTable.[object_id] = StatsColumnsTable.[object_id]
AND
	StatsTable.stats_id = StatsColumnsTable.stats_id
INNER JOIN
	sys.columns AS ColumnsTable
ON
	StatsColumnsTable.[object_id] = ColumnsTable.[object_id]
AND
	StatsColumnsTable.column_id = ColumnsTable.column_id
CROSS APPLY
	sys.dm_db_stats_properties (Objects.object_id , StatsTable.stats_id) AS StatsProperties
WHERE
	Objects.[type] IN ('U' , 'V')
GROUP BY
	Objects.schema_id ,
	Objects.[name] ,
	Objects.object_id ,
	Objects.[type] ,
	StatsTable.stats_id ,
	StatsTable.[name] ,
	StatsTable.auto_created ,
	StatsTable.user_created ,
	StatsTable.no_recompute ,
	StatsTable.filter_definition ,
	StatsTable.is_temporary ,					-- Applies to: SQL Server 2012 (11.x) and later
	StatsTable.is_incremental ,					-- Applies to: SQL Server 2014 (12.x) and later
	StatsProperties.last_updated ,
	StatsProperties.[rows] ,
	StatsProperties.rows_sampled ,
	StatsProperties.persisted_sample_percent ,
	StatsProperties.unfiltered_rows ,
	StatsProperties.steps ,
	StatsProperties.modification_counter
ORDER BY
	SchemaName	ASC ,
	ObjectName	ASC ,
	StatsId		ASC;
GO
