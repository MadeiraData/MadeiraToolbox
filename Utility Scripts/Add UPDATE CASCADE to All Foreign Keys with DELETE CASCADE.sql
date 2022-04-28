/*========================================================================================================================
-- Description: This query retrieves all the foreign keys with delete cascade and generates commands to also add update cascade.
-- Scope: Database
-- Author:	Eitan Blumin | https://www.madeiradata.com
-- Create Date: 28/04/2022
-- Type: Query Plug&play
-- Last Updated On:	28/04/2022
-- Notes: 
=========================================================================================================================*/


SELECT
	SchemaName		= OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ObjectId) ,
	TableName		= OBJECT_NAME (ForeignKeysWithColumns.ObjectId) ,
	ForeignKeyName		= ForeignKeysWithColumns.ForeignKeyName ,
	ForeignKeyColumns	= ForeignKeysWithColumns.ForeignKeyColumnList,
	ReferencedSchemaName	= OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ReferencedObjectId) ,
	ReferencedTableName	= OBJECT_NAME (ForeignKeysWithColumns.ReferencedObjectId) ,
	ReferencedColumnList	= ForeignKeysWithColumns.ReferencedColumnList,
	RemediationScript	= N'BEGIN TRAN; BEGIN TRY '
	+ N'ALTER TABLE ' + OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ObjectId) + N'.' + OBJECT_NAME (ForeignKeysWithColumns.ObjectId) + N' DROP CONSTRAINT ' + QUOTENAME(ForeignKeysWithColumns.ForeignKeyName)
	+ N'; ALTER TABLE ' + OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ObjectId) + N'.' + OBJECT_NAME (ForeignKeysWithColumns.ObjectId)
	+ N' WITH CHECK ADD  CONSTRAINT ' + QUOTENAME(ForeignKeysWithColumns.ForeignKeyName)
	+ N' FOREIGN KEY(' + ForeignKeysWithColumns.ForeignKeyColumnList
	+ N') REFERENCES ' + OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ReferencedObjectId) + N'.' + OBJECT_NAME (ForeignKeysWithColumns.ReferencedObjectId)
	+ N'(' + ForeignKeysWithColumns.ReferencedColumnList + N') ON UPDATE CASCADE ON DELETE CASCADE;'
	+ N' COMMIT TRAN; END TRY BEGIN CATCH PRINT N''' + ForeignKeysWithColumns.ForeignKeyName + N': '' + ERROR_MESSAGE(); IF @@TRANCOUNT > 0 ROLLBACK; END CATCH;'
FROM
	(
		SELECT
			ObjectId		= ForeignKeys.parent_object_id ,
			ReferencedObjectId	= ForeignKeys.referenced_object_id ,
			ReferencedColumnList	= ForeignKeyColumnsReferenced.ReferencedColumnList ,
			ForeignKeyColumnList	= ForeignKeyColumns.ForeignKeyColumnList ,
			ForeignKeyName		= ForeignKeys.name
		FROM
			sys.foreign_keys AS ForeignKeys
		CROSS APPLY
			(
				SELECT
					STUFF
					(
						
						(
							SELECT
								N',' + QUOTENAME(Columns.name)
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
						)
						, 1, 1, N''
					)
					AS ForeignKeyColumnList
			)
			AS ForeignKeyColumns
		CROSS APPLY
			(
				SELECT
					STUFF
					(
						
						(
							SELECT
								N',' + QUOTENAME(Columns.name)
							FROM
								sys.foreign_key_columns AS ForeignKeyColumns
							INNER JOIN
								sys.columns AS Columns
							ON
								ForeignKeyColumns.referenced_object_id = Columns.object_id
							AND
								ForeignKeyColumns.referenced_column_id = Columns.column_id
							WHERE
								ForeignKeyColumns.constraint_object_id = ForeignKeys.object_id
							ORDER BY
								ForeignKeyColumns.constraint_column_id ASC
							FOR XML PATH (N'')
						)
						, 1, 1, N''
					)
					AS ReferencedColumnList
			)
			AS ForeignKeyColumnsReferenced
		WHERE
			ForeignKeys.update_referential_action = 0
		AND
			ForeignKeys.delete_referential_action <> 0
	)
	AS ForeignKeysWithColumns
ORDER BY
	SchemaName		ASC ,
	TableName		ASC ,
	ForeignKeyName	ASC;
GO