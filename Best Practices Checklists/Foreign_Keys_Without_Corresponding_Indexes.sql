/*========================================================================================================================
-- Description: This query retrieves all the foreign keys in spesific DB that dont have corresponding indexes.
-- Scope: Database
-- Author:	Guy Glantser | https://www.madeiradata.com
-- Create Date: 08/04/2012
-- Type: Query Plug&play
-- Last Updated On:	01/09/2020
-- Notes: 
=========================================================================================================================*/


SELECT
	SchemaName			= OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ObjectId) ,
	TableName			= OBJECT_NAME (ForeignKeysWithColumns.ObjectId) ,
	ForeignKeyName		= ForeignKeysWithColumns.ForeignKeyName ,
	ForeignKeyColumns	= ForeignKeysWithColumns.ForeignKeyColumnList,
	UserUpdates		= ISNULL(UsageStats.user_updates, 0) ,
	UserScans		= ISNULL(UsageStats.user_scans, 0) ,
	LastUpdate		= UsageStats.last_user_update ,
	LastScan		= UsageStats.last_user_scan ,
	TotalRows		= ISNULL(PartitionStats.TotalRows, 0),
	RemediationScript		= 'CREATE NONCLUSTERED INDEX ' + QUOTENAME('IX_'+ForeignKeysWithColumns.ForeignKeyName) + ' ON '+ QUOTENAME( OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ObjectId))+'.'+QUOTENAME( OBJECT_NAME (ForeignKeysWithColumns.ObjectId))+' ('+ForeignKeysWithColumns.ForeignKeyColumnList+')'
FROM
	(
		SELECT
			ObjectId				= ForeignKeys.parent_object_id ,
			ForeignKeyColumnList	= ForeignKeyColumns.ForeignKeyColumnList ,
			ForeignKeyName			= ForeignKeys.name
		FROM
			sys.foreign_keys AS ForeignKeys
		CROSS APPLY
			(
				SELECT
					REPLACE
					(
						REPLACE
						(
							REPLACE
							(
								(
									SELECT
										QUOTENAME(Columns.name) AS c
									FROM
										sys.foreign_key_columns AS ForeignKeyColumns
									INNER JOIN
										sys.columns AS Columns
									ON
										ForeignKeyColumns.parent_object_id = Columns.object_id
									AND
										ForeignKeyColumns.parent_column_id = Columns.column_id
									WHERE
										ForeignKeyColumns.constraint_object_id = ForeignKeys.object_id
									ORDER BY
										ForeignKeyColumns.constraint_column_id ASC
									FOR XML PATH (N'')
								) ,
								N'</c><c>' ,
								N' , '
							) ,
							N'<c>' ,
							N''
						) ,
						N'</c>' ,
						N''
					)
					AS ForeignKeyColumnList
			)
			AS ForeignKeyColumns
	)
	AS ForeignKeysWithColumns
LEFT OUTER JOIN
	(
		SELECT
			ObjectId		= Indexes.object_id ,
			IndexKeysList	= IndexKeys.IndexKeysList
		FROM
			sys.indexes AS Indexes
		CROSS APPLY
			(
				SELECT
					REPLACE
					(
						REPLACE
						(
							REPLACE
							(
								(
									SELECT
										QUOTENAME(Columns.name) AS c
									FROM
										sys.index_columns AS IndexColumns
									INNER JOIN
										sys.columns AS Columns
									ON
										IndexColumns.object_id = Columns.object_id
									AND
										IndexColumns.column_id = Columns.column_id
									WHERE
										IndexColumns.object_id = Indexes.object_id
									AND
										IndexColumns.index_id = Indexes.index_id
									ORDER BY
										IndexColumns.index_column_id ASC
									FOR XML PATH (N'')
								) ,
								N'</c><c>' ,
								N' , '
							) ,
							N'<c>' ,
							N''
						) ,
						N'</c>' ,
						N''
					)
					AS IndexKeysList
			)
			AS IndexKeys
	)
	AS IndexesWithColumns
ON
	ForeignKeysWithColumns.ObjectId = IndexesWithColumns.ObjectId
AND (
	IndexesWithColumns.IndexKeysList LIKE REPLACE(REPLACE(ForeignKeysWithColumns.ForeignKeyColumnList,'[','_'),']','_') + N'%'
	OR
	ForeignKeysWithColumns.ForeignKeyColumnList LIKE REPLACE(REPLACE(IndexesWithColumns.IndexKeysList,'[','_'),']','_') + N'%'
	)
OUTER APPLY
(
	SELECT
		us.user_updates ,
		us.user_scans ,
		us.last_user_update ,
		us.last_user_scan
	FROM
		sys.dm_db_index_usage_stats AS us
	WHERE
		us.database_id = DB_ID()
	AND	us.object_id = ForeignKeysWithColumns.ObjectId
	AND	us.index_id <= 1
) AS UsageStats
OUTER APPLY
(
	SELECT
		TotalRows = SUM(p.rows)
	FROM
		sys.partitions AS p
	WHERE
		p.object_id = ForeignKeysWithColumns.ObjectId
	AND	p.index_id <= 1
) AS PartitionStats
WHERE
	IndexesWithColumns.ObjectId IS NULL
ORDER BY
	SchemaName		ASC ,
	TableName		ASC ,
	ForeignKeyName	ASC;
GO