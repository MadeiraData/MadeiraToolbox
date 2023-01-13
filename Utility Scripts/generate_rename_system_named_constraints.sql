/*
Generate Rename for System-Named Constraints
============================================
Author: Eitan Blumin | https://www.eitanblumin.com
Date: 2022-01-12
Description:
This is a query to generate rename commands for all system-named constraints within all accessible databases.
The constraints are renamed based on convention of "{DF|CK|PK|UQ|FK|EC}_[Non-dbo-SchemaName_]{TableName}[_ColumnName][_ReferencedTable][_n]"
Simply run this query and then copy & paste the entire remediationCommand column to get the script(s).

Supported constraint types with their prefixes and naming convention:
DF - Default Constraints	DF_[Non-dbo-SchemaName_]TableName_ColumnName
PK - Primary Keys		PK_[Non-dbo-SchemaName_]TableName
UQ - Unique Constraints		UQ_[Non-dbo-SchemaName_]TableName_ColumnName(s)
FK - Foreign Keys		FK_[Non-dbo-SchemaName_]TableName_ColumnName(s)_ReferencedTable[_n]
CK - Check Constraints		CK_[Non-dbo-SchemaName_]TableName[_ColumnName][_n]

SQL 2019 and newer only, automatically skipped in older versions:
EC - Edge Constraints		EC_TableName

This script uses the CONCAT function which is supported only in SQL Server 2012 and newer.
*/

DECLARE
	 @FilterByDatabase	sysname	= NULL	-- optionally filter by a specific database name. Leave as NULL to check all accessible databases.
	,@IncludeColumnsInFK	bit	= 0	-- optionally set to 1 to include referencing column names in foreign key constraint names.
	,@IncludeNonDBOSchema	bit	= 1	-- optionally set to 1 to include non-dbo schema names in the constraint names.
	,@SystemNamedOnly	bit	= 1	-- optionally set to 0 to include user-named constraints as well (for standardizing all names).

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @CMD nvarchar(MAX);
DECLARE @Results AS TABLE(
  databaseName sysname, schemaName sysname, tableName sysname, columnName nvarchar(max) NULL
, constraintType sysname NULL, constraintName sysname NULL, referencedName sysname NULL, definitionValue nvarchar(max) NULL
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
		constraintType = N''DF'',
		constraintName = const.[name],
		referencedName = NULL,
		definitionValue = const.[definition]
	FROM sys.default_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	INNER JOIN sys.columns AS col ON const.parent_object_id = col.object_id AND const.parent_column_id = col.column_id
	WHERE const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	--AND tab.[type] = ''U''
	' + CASE WHEN @SystemNamedOnly = 1 THEN N'AND const.is_system_named = 1' ELSE N'' END
SET @CMD = @CMD + N'
	UNION ALL
	
	-- key constraints (unique constraints, primary keys)
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = STUFF((
				SELECT N''_'' + [name]
				FROM sys.index_columns AS ic
				INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
				WHERE ic.object_id = const.parent_object_id
				AND ic.index_id = const.unique_index_id
				ORDER BY ic.key_ordinal ASC
				FOR XML PATH(''''), TYPE
			     ).value(''(text())[1]'',''nvarchar(max)''), 1, 1, N''''),
		constraintType = const.[type] COLLATE DATABASE_DEFAULT,
		constraintName = const.[name],
		referencedName = NULL,
		definitionValue = NULL
	FROM sys.key_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	WHERE const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	--AND tab.[type] = ''U''
	' + CASE WHEN @SystemNamedOnly = 1 THEN N'AND const.is_system_named = 1' ELSE N'' END + N'

	UNION ALL

	-- check constraints
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = col.[name],
		constraintType = N''CK'',
		constraintName = const.[name],
		referencedName = NULL,
		definitionValue = const.[definition]
	FROM sys.check_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	LEFT JOIN sys.columns AS col ON const.parent_object_id = col.object_id AND const.parent_column_id = col.column_id
	WHERE const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	--AND tab.[type] = ''U''
	' + CASE WHEN @SystemNamedOnly = 1 THEN N'AND const.is_system_named = 1' ELSE N'' END

SET @CMD = @CMD + N'
	UNION ALL

	-- foreign keys
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = ' + CASE WHEN @IncludeColumnsInFK = 1 THEN N'STUFF((
				SELECT N''_'' + [name]
				FROM sys.foreign_key_columns AS ic
				INNER JOIN sys.columns AS c ON ic.parent_object_id = c.object_id AND ic.parent_column_id = c.column_id
				WHERE ic.constraint_object_id = const.object_id
				FOR XML PATH(''''), TYPE
			     ).value(''(text())[1]'',''nvarchar(max)''), 1, 1, N'''')'
				ELSE N'NULL'
				END + N',
		constraintType = N''FK'',
		constraintName = const.[name],
		referencedName = ISNULL(NULLIF(OBJECT_SCHEMA_NAME(const.referenced_object_id),''dbo'') + N''_'', N'''') + OBJECT_NAME(const.referenced_object_id),
		definitionValue = STUFF((
				SELECT N'', '' + pc.[name] + N''->'' + c.[name]
				FROM sys.foreign_key_columns AS ic
				INNER JOIN sys.columns AS c ON ic.referenced_object_id = c.object_id AND ic.referenced_column_id = c.column_id
				INNER JOIN sys.columns AS pc ON ic.parent_object_id = pc.object_id AND ic.parent_column_id = pc.column_id
				WHERE ic.constraint_object_id = const.object_id
				FOR XML PATH(''''), TYPE
			     ).value(''(text())[1]'',''nvarchar(max)''), 1, 2, N'''')
	FROM sys.foreign_keys AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	WHERE const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	--AND tab.[type] = ''U''
	' + CASE WHEN @SystemNamedOnly = 1 THEN N'AND const.is_system_named = 1' ELSE N'' END

IF (CONVERT(FLOAT, (@@microsoftversion / 0x1000000) & 0xff)) >= 15
BEGIN
SET @CMD = @CMD + N'

	UNION ALL
	
	-- edge constraints (SQL 2019 and newer only)
	SELECT
		schemaName = sch.[name],
		tableName = tab.[name],
		columnName = NULL,
		constraintType = N''EC'',
		constraintName = const.[name],
		referencedName = NULL,
		definitionValue = STUFF((
				SELECT N'',''
				+ ISNULL(NULLIF(OBJECT_SCHEMA_NAME(ecc.from_object_id),''dbo'') + N''.'', N'''') + OBJECT_NAME(ecc.from_object_id)
				+ N''->''
				+ ISNULL(NULLIF(OBJECT_SCHEMA_NAME(ecc.to_object_id),''dbo'') + N''.'', N'''') + OBJECT_NAME(ecc.to_object_id)
				FROM sys.edge_constraint_clauses AS ecc
				WHERE ecc.object_id = const.object_id
				FOR XML PATH(''''), TYPE
			     ).value(''(text())[1]'',''nvarchar(max)''), 1, 1, N'''')
	FROM sys.edge_constraints AS const
	INNER JOIN sys.tables AS tab ON const.parent_object_id = tab.object_id
	INNER JOIN sys.schemas AS sch ON const.schema_id = sch.schema_id
	WHERE const.is_ms_shipped = 0
	AND tab.is_ms_shipped = 0
	--AND tab.[type] = ''U''
	' + CASE WHEN @SystemNamedOnly = 1 THEN N'AND const.is_system_named = 1' ELSE N'' END

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
			, newConstraintName
			, ''', ''OBJECT'';')
FROM
(
SELECT *
, newConstraintName = REPLACE(CONCAT(newConstraintNamePreliminary
				, CASE WHEN COUNT(*) OVER (PARTITION BY databaseName, schemaName, tableName, columnName, constraintType, referencedName) = 1
				  THEN N''
				  ELSE
					ISNULL('_' + CONVERT(nvarchar(MAX), ROW_NUMBER() OVER (PARTITION BY databaseName, schemaName, tableName, columnName, constraintType, referencedName ORDER BY (SELECT NULL))), N'')
				  END
				)
			, ' ', '_')
FROM
(
SELECT *
, newConstraintNamePreliminary = LEFT(CONCAT(
				  constraintType
				, N'_'
				, CASE WHEN @IncludeNonDBOSchema = 1 AND schemaName <> 'dbo' THEN schemaName + N'_' ELSE N'' END
				, tableName
				, CASE WHEN constraintType <> 'PK' AND LEN(columnName) < 50 THEN ISNULL(N'_' + columnName, N'') ELSE N'' END
				, ISNULL(N'_' + referencedName, N'')
				), 125)
FROM @Results
) AS a
) AS b
WHERE constraintName <> newConstraintName