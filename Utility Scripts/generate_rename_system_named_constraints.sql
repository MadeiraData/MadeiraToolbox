/*
Generate Rename for System-Named Constraints
============================================
Author: Eitan Blumin | https://www.eitanblumin.com
Date: 2022-01-12
Description:
This is a query to generate rename commands for all system-named constraints within all accessible databases.
The constraints are renamed based on convention of "{DF|CHK|PK|UQ|FK|EC}_[Non-dbo-SchemaName_]{TableName}_{ColumnName|ReferencedTable}[_n]"
Simply run this query and then copy & paste the entire remediationCommand column to get the script(s).

Supported constraint types and their prefixes:
DF - Default Constraints
PK - Primary Keys
UQ - Unique Constraints
FK - Foreign Keys
CHK - Check Constraints
EC - Edge Constraints (SQL 2019 and newer only, will be ignored otherwise)

This script uses the CONCAT function which is supported in SQL Server 2012 and newer.
*/

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @FilterByDatabase sysname = NULL -- optionally filter by a specific database name. Leave as NULL to check all accessible databases.

DECLARE @CMD nvarchar(MAX);
DECLARE @Results AS TABLE(
  databaseName sysname, schemaName sysname, tableName sysname, columnName sysname NULL
, constraintType sysname NULL, constraintName sysname NULL, newConstraintName sysname NULL
);

SET @CMD = N'
SELECT DB_NAME()
, *
FROM
(
	-- default constraints
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = col.[name],
		constraintType = const.[type_desc] COLLATE DATABASE_DEFAULT,
		constraintName = const.[name],
		newConstraintName = CONCAT(
				  ''DF_''
				, ISNULL(NULLIF(sch.[name],''dbo'') + N''_'',N'''')
				, tab.[name]
				, ''_''
				, col.[name]
				, CASE WHEN COUNT(*) OVER (PARTITION BY const.parent_object_id, const.parent_column_id) = 1
				  THEN N''''
				  ELSE
					ISNULL(''_'' + CONVERT(nvarchar(MAX), ROW_NUMBER() OVER (PARTITION BY const.parent_object_id, const.parent_column_id ORDER BY (SELECT NULL))), N'''')
				  END
				)
	FROM sys.default_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	INNER JOIN sys.columns AS col ON const.parent_object_id = col.object_id AND const.parent_column_id = col.column_id
	WHERE const.is_system_named = 1
	--AND tab.[type] = ''U''
	-- Ignore system objects:
	AND const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	'
SET @CMD = @CMD + N'
	UNION ALL
	
	-- key constraints (unique constraints, primary keys)
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = NULL,
		constraintType = const.[type_desc] COLLATE DATABASE_DEFAULT,
		constraintName = const.[name],
		newConstraintName = CONCAT(
				  const.[type] COLLATE DATABASE_DEFAULT
				, N''_''
				, ISNULL(NULLIF(sch.[name],''dbo'') + N''_'',N'''')
				, tab.[name]
				, CASE WHEN COUNT(*) OVER (PARTITION BY const.parent_object_id, const.[type]) = 1
				  THEN N''''
				  ELSE
					ISNULL(''_'' + CONVERT(nvarchar(MAX), ROW_NUMBER() OVER (PARTITION BY const.parent_object_id, const.[type] ORDER BY (SELECT NULL))), N'''')
				  END
				)
	FROM sys.key_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	WHERE const.is_system_named = 1
	--AND tab.[type] = ''U''
	-- Ignore system objects:
	AND const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0

	UNION ALL

	-- check constraints
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = col.[name],
		constraintType = const.[type_desc] COLLATE DATABASE_DEFAULT,
		constraintName = const.[name],
		newConstraintName = 
				CONCAT(
				  ''CHK_''
				, ISNULL(NULLIF(sch.[name],''dbo'') + N''_'', N'''')
				, tab.[name]
				, ISNULL(''_'' + col.[name], N'''')
				, CASE WHEN COUNT(*) OVER (PARTITION BY const.parent_object_id, const.parent_column_id) = 1
				  THEN N''''
				  ELSE
					ISNULL(''_'' + CONVERT(nvarchar(MAX), ROW_NUMBER() OVER (PARTITION BY const.parent_object_id, const.parent_column_id ORDER BY (SELECT NULL))), N'''')
				  END
				)
	FROM sys.check_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	LEFT JOIN sys.columns AS col ON const.parent_object_id = col.object_id AND const.parent_column_id = col.column_id
	WHERE const.is_system_named = 1
	--AND tab.[type] = ''U''
	-- Ignore system objects:
	AND const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	'
SET @CMD = @CMD + N'
	UNION ALL

	-- foreign keys
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = NULL,
		constraintType = const.[type_desc] COLLATE DATABASE_DEFAULT,
		constraintName = const.[name],
		newConstraintName = 
				CONCAT(
				  ''FK_''
				, ISNULL(NULLIF(sch.[name],''dbo'') + N''_'', N'''')
				, tab.[name]
				, N''_''
				, ISNULL(NULLIF(OBJECT_SCHEMA_NAME(const.referenced_object_id),''dbo'') + N''_'', N'''')
				, OBJECT_NAME(const.referenced_object_id)
				, CASE WHEN COUNT(*) OVER (PARTITION BY const.parent_object_id, const.referenced_object_id) = 1
				  THEN N''''
				  ELSE
					ISNULL(''_'' + CONVERT(nvarchar(MAX), ROW_NUMBER() OVER (PARTITION BY const.parent_object_id, const.referenced_object_id ORDER BY (SELECT NULL))), N'''')
				  END
				)
	FROM sys.foreign_keys AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	WHERE const.is_system_named = 1
	--AND tab.[type] = ''U''
	-- Ignore system objects:
	AND const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	'
IF (CONVERT(FLOAT, (@@microsoftversion / 0x1000000) & 0xff)) >= 15
BEGIN
SET @CMD = @CMD + N'

	UNION ALL
	
	-- edge constraints (SQL 2019 and newer only)
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = NULL,
		constraintType = const.[type_desc] COLLATE DATABASE_DEFAULT,
		constraintName = const.[name],
		newConstraintName = CONCAT(
				  N''EC_''
				, ISNULL(NULLIF(sch.[name],''dbo'') + N''_'',N'''')
				, tab.[name]
				, CASE WHEN COUNT(*) OVER (PARTITION BY const.parent_object_id) = 1
				  THEN N''''
				  ELSE
					ISNULL(''_'' + CONVERT(nvarchar(MAX), ROW_NUMBER() OVER (PARTITION BY const.parent_object_id ORDER BY (SELECT NULL))), N'''')
				  END
				)
	FROM sys.edge_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	WHERE const.is_system_named = 1
	--AND tab.[type] = ''U''
	-- Ignore system objects:
	AND const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	'
END

SET @CMD = @CMD + N'
) AS q'

DECLARE @CurrDB sysname, @SpExecuteSQL nvarchar(250);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE database_id > 4
AND [state] = 0
AND HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
AND ([name] = @FilterByDatabase OR @FilterByDatabase IS NULL)

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @SpExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql'

	RAISERROR(N'%s',0,1,@CurrDB) WITH NOWAIT;

	INSERT INTO @Results
	EXEC @SpExecuteSQL @CMD;
END

CLOSE DBs;
DEALLOCATE DBs;

SELECT *
, remediationCommand = CONCAT('USE '
			, QUOTENAME(databaseName)
			, N'; EXEC sp_rename '''
			, QUOTENAME(schemaName)
			, '.'
			, QUOTENAME(constraintName)
			, ''', '''
			, REPLACE(newConstraintName, ' ', '_')
			, ''', ''OBJECT'';')
FROM @Results;
