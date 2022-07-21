/*========================================================================================================================

Description:	This script drops all foreign keys in the database, allows you to delete data from multiple tables based on your logic,
				and then it recreates all the foreign keys again.
				The script also generates an audit and a rowcount comparison (before and after) for all tables.
Scope:			Database
Author:			Guy Glantser
Created:		21/07/2022
Last Updated:	21/07/2022
Notes:			

=========================================================================================================================*/


-- Step 1:	Create the #TableRowCount table to store row counts in all tables

DROP TABLE IF EXISTS
	#TableRowCount;
GO


CREATE TABLE
	#TableRowCount
(
	SchemaName			SYSNAME	NOT NULL ,
	TableName			SYSNAME	NOT NULL ,
	NumberOfRows_Before	INT		NOT NULL ,
	NumberOfRows_After	INT		NULL ,

	CONSTRAINT
		pk_TableRowCount_c_SchemaName#TableName
	PRIMARY KEY CLUSTERED
		(
			SchemaName	ASC ,
			TableName	ASC
		)
);
GO


-- Step 2:	Populate the #TableRowCount table before we delete any rows

INSERT INTO
	#TableRowCount
(
	SchemaName ,
	TableName ,
	NumberOfRows_Before ,
	NumberOfRows_After
)
SELECT
	SchemaName			= SCHEMA_NAME ([Tables].[schema_id]) ,
	TableName			= [Tables].[name] ,
	NumberOfRows_Before	= SUM ([Partitions].[rows]) ,
	NumberOfRows_After	= NULL
FROM
	sys.tables AS [Tables]
INNER JOIN
	sys.indexes AS [Indexes]
ON
	[Tables].[object_id] = [Indexes].[object_id]
INNER JOIN
	sys.partitions AS [Partitions]
ON
	[Tables].[object_id] = [Partitions].[object_id]
AND
	[Indexes].index_id = [Partitions].index_id
WHERE
	[Tables].is_external = 0
AND
	[Indexes].index_id IN (0,1)
GROUP BY
	[Tables].[schema_id] ,
	[Tables].[name];
GO


SELECT
	*
FROM
	#TableRowCount;
GO


-- Step 3:	Store the foreign key information in a table
--			Since there are no composite foreign keys in our database, the script assumes there is a single column in each foreign key

DROP TABLE IF EXISTS
	#ForeignKeys;
GO


SELECT
	ReferencingTableSchema	= SCHEMA_NAME (ReferencingTables.[schema_id]) ,
	ReferencingTableName	= ReferencingTables.[name] ,
	ForeignKeyName			= ForeignKeys.[name] ,
	ForeignKeyColumnName	= ReferencingColumns.[name] ,
	ReferencedTableSchema	= SCHEMA_NAME (ReferencedTables.[schema_id]) ,
	ReferencedTableName		= ReferencedTables.[name] ,
	ReferencedColumnName	= ReferencedColumns.[name]
INTO
	#ForeignKeys
FROM
	sys.foreign_keys AS ForeignKeys
INNER JOIN
	sys.foreign_key_columns AS ForeignKeyColumns
ON
	ForeignKeys.[object_id] = ForeignKeyColumns.constraint_object_id
INNER JOIN
	sys.tables AS ReferencingTables
ON
	ForeignKeys.parent_object_id = ReferencingTables.[object_id]
INNER JOIN
	sys.columns AS ReferencingColumns
ON
	ForeignKeyColumns.parent_column_id = ReferencingColumns.column_id
AND
	ReferencingTables.[object_id] = ReferencingColumns.[object_id]
INNER JOIN
	sys.tables AS ReferencedTables
ON
	ForeignKeys.referenced_object_id = ReferencedTables.[object_id]
INNER JOIN
	sys.columns AS ReferencedColumns
ON
	ForeignKeyColumns.referenced_column_id = ReferencedColumns.column_id
AND
	ReferencedTables.[object_id] = ReferencedColumns.[object_id];
GO


-- Step 4:	Create an audit table

CREATE TABLE
	#Audit
(
	AuditId			INT				NOT NULL	IDENTITY (1,1) ,
	CommandDateTime	DATETIME2(0)	NOT NULL ,
	Command			NVARCHAR(1000)	NOT NULL ,
	RowsAffected	INT				NULL

	CONSTRAINT
		pk_Audit_c_AuditId
	PRIMARY KEY CLUSTERED
		(AuditId ASC)
);
GO


-- Step 5:	Drop all the foreign keys

DECLARE
	@ReferencingTableSchema	AS SYSNAME ,
	@ReferencingTableName	AS SYSNAME ,
	@ForeignKeyName			AS SYSNAME ,
	@Command				AS NVARCHAR(MAX);

DECLARE
	ForeignKeysCursor
CURSOR
	LOCAL FAST_FORWARD
FOR
	SELECT
		ReferencingTableSchema ,
		ReferencingTableName ,
		ForeignKeyName
	FROM
		#ForeignKeys;

OPEN ForeignKeysCursor;

FETCH NEXT FROM
	ForeignKeysCursor
INTO
	@ReferencingTableSchema ,
	@ReferencingTableName ,
	@ForeignKeyName;

WHILE
	@@FETCH_STATUS = 0
BEGIN

	SET @Command =
		N'
			ALTER TABLE
				' + QUOTENAME (@ReferencingTableSchema) + N'.' + QUOTENAME (@ReferencingTableName) + N'
			DROP CONSTRAINT
				' + QUOTENAME (@ForeignKeyName) + N';
		';

	EXECUTE (@Command);

	INSERT INTO
		#Audit
	(
		CommandDateTime ,
		Command ,
		RowsAffected
	)
	VALUES
	(
		SYSDATETIME () ,
		N'Dropping foreign key ' + QUOTENAME (@ForeignKeyName) + N' on table '+ QUOTENAME (@ReferencingTableSchema) + N'.' + QUOTENAME (@ReferencingTableName) ,
		NULL
	);

	FETCH NEXT FROM
		ForeignKeysCursor
	INTO
		@ReferencingTableSchema ,
		@ReferencingTableName ,
		@ForeignKeyName;

END;

CLOSE ForeignKeysCursor;

DEALLOCATE ForeignKeysCursor;
GO


-- Step 6:	Delete data based on your logic...


-- Step 7:	Delete rows that violate foreign keys

DECLARE
	@ReferencingTableSchema	AS SYSNAME ,
	@ReferencingTableName	AS SYSNAME ,
	@ForeignKeyName			AS SYSNAME ,
	@ForeignKeyColumnName	AS SYSNAME ,
	@ReferencedTableSchema	AS SYSNAME ,
	@ReferencedTableName	AS SYSNAME ,
	@ReferencedColumnName	AS SYSNAME ,
	@Command				AS NVARCHAR(MAX) ,
	@RowCount				AS INT ,
	@NotDoneYet				AS BIT;

DECLARE
	ForeignKeysCursor
CURSOR
	LOCAL FAST_FORWARD
FOR
	SELECT
		ReferencingTableSchema ,
		ReferencingTableName ,
		ForeignKeyName ,
		ForeignKeyColumnName ,
		ReferencedTableSchema ,
		ReferencedTableName ,
		ReferencedColumnName
	FROM
		#ForeignKeys;

SET @NotDoneYet = 1;

WHILE
	@NotDoneYet = 1
BEGIN

	OPEN ForeignKeysCursor;

	FETCH NEXT FROM
		ForeignKeysCursor
	INTO
		@ReferencingTableSchema ,
		@ReferencingTableName ,
		@ForeignKeyName ,
		@ForeignKeyColumnName ,
		@ReferencedTableSchema ,
		@ReferencedTableName ,
		@ReferencedColumnName;

	SET @NotDoneYet = 0;

	WHILE
		@@FETCH_STATUS = 0
	BEGIN

		SET @Command =
			N'
				DELETE FROM
					' + QUOTENAME (@ReferencingTableSchema) + N'.' + QUOTENAME (@ReferencingTableName) + N'
				WHERE
					' + QUOTENAME (@ForeignKeyColumnName) + N' NOT IN
						(
							SELECT
								' + QUOTENAME (@ReferencedColumnName) + N'
							FROM
								' + QUOTENAME (@ReferencedTableSchema) + N'.' + QUOTENAME (@ReferencedTableName) + N'
						)
				AND
					' + QUOTENAME (@ForeignKeyColumnName) + N' IS NOT NULL;
			';

		EXECUTE (@Command);

		SET @RowCount = @@ROWCOUNT;

		INSERT INTO
			#Audit
		(
			CommandDateTime ,
			Command ,
			RowsAffected
		)
		VALUES
		(
			SYSDATETIME () ,
			N'Deleting rows that violate the foreign key ' + QUOTENAME (@ForeignKeyName) + N' from table ' + QUOTENAME (@ReferencingTableSchema) + N'.' + QUOTENAME (@ReferencingTableName) ,
			@RowCount
		);

		IF
			@RowCount > 0
		BEGIN

			SET @NotDoneYet = 1;

		END;

		FETCH NEXT FROM
			ForeignKeysCursor
		INTO
			@ReferencingTableSchema ,
			@ReferencingTableName ,
			@ForeignKeyName ,
			@ForeignKeyColumnName ,
			@ReferencedTableSchema ,
			@ReferencedTableName ,
			@ReferencedColumnName;

	END;

	CLOSE ForeignKeysCursor;

END;

DEALLOCATE ForeignKeysCursor;
GO


-- Step 8:	Recreate the foreign keys

DECLARE
	@ReferencingTableSchema	AS SYSNAME ,
	@ReferencingTableName	AS SYSNAME ,
	@ForeignKeyName			AS SYSNAME ,
	@ForeignKeyColumnName	AS SYSNAME ,
	@ReferencedTableSchema	AS SYSNAME ,
	@ReferencedTableName	AS SYSNAME ,
	@ReferencedColumnName	AS SYSNAME ,
	@Command				AS NVARCHAR(MAX);

DECLARE
	ForeignKeysCursor
CURSOR
	LOCAL FAST_FORWARD
FOR
	SELECT
		ReferencingTableSchema ,
		ReferencingTableName ,
		ForeignKeyName ,
		ForeignKeyColumnName ,
		ReferencedTableSchema ,
		ReferencedTableName ,
		ReferencedColumnName
	FROM
		#ForeignKeys;

OPEN ForeignKeysCursor;

FETCH NEXT FROM
	ForeignKeysCursor
INTO
	@ReferencingTableSchema ,
	@ReferencingTableName ,
	@ForeignKeyName ,
	@ForeignKeyColumnName ,
	@ReferencedTableSchema ,
	@ReferencedTableName ,
	@ReferencedColumnName;

WHILE
	@@FETCH_STATUS = 0
BEGIN

	SET @Command =
		N'
			ALTER TABLE
				' + QUOTENAME (@ReferencingTableSchema) + N'.' + QUOTENAME (@ReferencingTableName) + N'
			WITH CHECK ADD CONSTRAINT
				' + QUOTENAME (@ForeignKeyName) + N'
			FOREIGN KEY
				(' + QUOTENAME (@ForeignKeyColumnName) + N')
			REFERENCES
				' + QUOTENAME (@ReferencedTableSchema) + N'.' + QUOTENAME (@ReferencedTableName) + N' (' + QUOTENAME (@ReferencedColumnName) + N');
		';

	EXECUTE (@Command);

	INSERT INTO
		#Audit
	(
		CommandDateTime ,
		Command ,
		RowsAffected
	)
	VALUES
	(
		SYSDATETIME () ,
		N'Recreating foreign key ' + QUOTENAME (@ForeignKeyName) + N' on table '+ QUOTENAME (@ReferencingTableSchema) + N'.' + QUOTENAME (@ReferencingTableName) ,
		NULL
	);

	FETCH NEXT FROM
		ForeignKeysCursor
	INTO
		@ReferencingTableSchema ,
		@ReferencingTableName ,
		@ForeignKeyName ,
		@ForeignKeyColumnName ,
		@ReferencedTableSchema ,
		@ReferencedTableName ,
		@ReferencedColumnName;

END;

CLOSE ForeignKeysCursor;

DEALLOCATE ForeignKeysCursor;
GO


-- Step 9:	Populate the #TableRowCount table after we deleted the rows

UPDATE
	#TableRowCount
SET
	NumberOfRows_After =
		(
			SELECT
				NumberOfRows_After	= SUM ([Partitions].[rows])
			FROM
				sys.tables AS [Tables]
			INNER JOIN
				sys.schemas AS [Schemas]
			ON
				[Tables].[schema_id] = [Schemas].[schema_id]
			INNER JOIN
				sys.indexes AS [Indexes]
			ON
				[Tables].[object_id] = [Indexes].[object_id]
			INNER JOIN
				sys.partitions AS [Partitions]
			ON
				[Tables].[object_id] = [Partitions].[object_id]
			AND
				[Indexes].index_id = [Partitions].index_id
			WHERE
				[Tables].is_external = 0
			AND
				[Indexes].index_id IN (0,1)
			AND
				[Schemas].[name] = #TableRowCount.SchemaName
			AND
				[Tables].[name] = #TableRowCount.TableName				
		);
GO


-- Step 10:	Compare the number of rows before and after the delete

SELECT
	SchemaName			= SchemaName ,
	TableName			= TableName ,
	NumberOfRows_Before	= NumberOfRows_Before ,
	NumberOfRows_After	= NumberOfRows_After ,
	NumberOfRowsDeleted	= NumberOfRows_Before - NumberOfRows_After
FROM
	#TableRowCount
WHERE
	NumberOfRows_Before <> NumberOfRows_After
ORDER BY
	SchemaName	ASC ,
	TableName	ASC;
GO


-- Step 11:	View the audit

SELECT
	AuditId ,
	CommandDateTime ,
	Command ,
	RowsAffected
FROM
	#Audit
ORDER BY
	AuditId ASC;
GO
