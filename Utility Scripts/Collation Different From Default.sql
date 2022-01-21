/*
Find databases and columns with collation different from the default
====================================================================
Author: Eitan Blumin
Date: 2022-01-19
Description:
This script outputs 2 resultsets:
1. List of all accessible databases, the database collation of each, and whether it's equal to the server default collation.
2. List of all table columns with a collation different than its database default.
*/
DECLARE
	@DBName sysname = NULL -- Optional parameter to filter by a specific database. Leave NULL to check all accessible databases.

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Results AS table
(
	[database_name] sysname NOT NULL,
	[database_collation] sysname NOT NULL,
	[schema_name] sysname NOT NULL,
	[table_name] sysname NOT NULL,
	[column_name] sysname NOT NULL,
	[column_type] sysname NOT NULL,
	[column_type_schema] sysname NOT NULL,
	[column_type_user_defined] bit NULL,
	[max_length] int NULL,
	[collation_name] sysname NOT NULL,
	[is_nullable] bit NULL,
	[dependent_indexes] xml NULL,
	[dependent_foreign_keys] xml NULL,
	[schemabound_dependencies] xml NULL
)

DECLARE @SpExecuteSql nvarchar(1000), @DBCollation sysname;

SELECT @@SERVERNAME AS [server_name], [name] AS [database_name], collation_name
, is_server_default = CASE WHEN collation_name = CONVERT(sysname, SERVERPROPERTY('Collation')) THEN 1 ELSE 0 END
FROM sys.databases
WHERE (@DBName IS NULL OR @DBName = [name])
AND (@DBName IS NOT NULL OR database_id > 4)
AND state = 0
AND HAS_DBACCESS([name]) = 1
ORDER BY database_id ASC

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name], collation_name
FROM sys.databases
WHERE (@DBName IS NULL OR @DBName = [name])
AND (@DBName IS NOT NULL OR database_id > 4)
AND state = 0
AND HAS_DBACCESS([name]) = 1
ORDER BY database_id ASC

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @DBName, @DBCollation;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @SpExecuteSql = QUOTENAME(@DBName) + N'..sp_executesql';

	INSERT INTO @Results
	EXEC @SpExecuteSql N'SELECT DB_NAME()
, CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), ''Collation''))
, SCHEMA_NAME(o.schema_id), o.name, c.[name] AS column_name, t.name AS column_type, SCHEMA_NAME(t.schema_id) AS column_type_schema, t.is_user_defined
, c.max_length, c.collation_name, c.is_nullable
, dependent_indexes = (
	SELECT ix.type_desc AS [@type]
	, ix.is_primary_key AS [@is_primary_key]
	, ix.is_unique_constraint AS [@is_unique_constraint]
	, ix.[name] AS [text()]
	FROM sys.index_columns AS ixc
	INNER JOIN sys.indexes AS ix ON ix.object_id = ixc.object_id AND ix.index_id = ixc.index_id
	WHERE ix.object_id = o.object_id
	AND ixc.column_id = c.column_id
	FOR XML PATH(''Index''), TYPE
)
, dependent_foreign_keys = (
	SELECT
	  QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + N''.'' + QUOTENAME(OBJECT_NAME(fk.parent_object_id)) AS [@ParentTable]
	, QUOTENAME(OBJECT_SCHEMA_NAME(fk.referenced_object_id)) + N''.'' + QUOTENAME(OBJECT_NAME(fk.referenced_object_id)) AS [@ReferencedTable]
	, OBJECT_NAME(fk.constraint_object_id) AS [text()]
	FROM sys.foreign_key_columns AS fk
	WHERE (fk.parent_object_id = o.object_id AND fk.parent_column_id = c.column_id)
	OR (fk.referenced_object_id = o.object_id AND fk.referenced_column_id = c.column_id)
	FOR XML PATH(''ForeignKey''), TYPE
)
, schemabound_dependencies = (
	SELECT 
	  d.class_desc AS [@class]
	, OBJECT_SCHEMA_NAME(d.object_id) + N''.'' + OBJECT_NAME(d.object_id) AS [text()]
	FROM sys.sql_dependencies AS D
	WHERE d.referenced_major_id = o.object_id
	AND d.referenced_minor_id = c.column_id
	AND d.class > 0
	FOR XML PATH(''Dependency''), TYPE
)
FROM sys.tables AS o
INNER JOIN sys.columns AS c ON c.object_id = o.object_id
INNER JOIN sys.types AS t ON c.user_type_id = t.user_type_id AND c.system_type_id = t.system_type_id
WHERE o.is_ms_shipped = 0
AND c.is_computed = 0
AND SCHEMA_NAME(o.schema_id) <> ''sys''
AND c.collation_name <> CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), ''Collation''))'
END

CLOSE DBs;
DEALLOCATE DBs;

SELECT *
, AlterCmd = N'USE ' + QUOTENAME([database_name]) + N'; ALTER TABLE ' + QUOTENAME([schema_name]) + N'.' + QUOTENAME([table_name])
+ N' ALTER COLUMN ' + QUOTENAME([column_name]) + N' '
+ CASE WHEN [column_type_user_defined] = 1 THEN QUOTENAME(column_type_schema) + N'.' ELSE N'' END
+ QUOTENAME(column_type) + N'(' 
+ ISNULL(CONVERT(nvarchar(MAX), 
	NULLIF(max_length,-1)
	/ CASE WHEN column_type IN ('nvarchar','nchar') AND max_length > 0 THEN 2 ELSE 1 END
	), N'MAX') + N')'
+ N' COLLATE ' + [database_collation] + N' ' + CASE is_nullable WHEN 1 THEN N'NULL' ELSE N'NOT NULL' END
FROM @Results;

