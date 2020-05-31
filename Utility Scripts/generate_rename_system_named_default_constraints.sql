-- Author: Eitan Blumin | https://www.eitanblumin.com
-- Date: 2020-02-26
-- Description: This is a query to generate rename commands for all system-named default constraints within the current database.
-- The constraints are renamed based on convention of "DF_{TableName}_{ColumnName}"
-- Simply run this query and then copy & paste the entire remediationCommand column to get the script(s).

SELECT
	schemaName = sch.[name],
	tableName = tab.[name],
	columnName = col.[name],
	defaultName = def.[name],
	remediationCommand = 
			CONCAT('EXEC sp_rename '''
			, QUOTENAME(sch.[name])
			, '.'
			, QUOTENAME(def.[name])
			, ''', ''DF_'
			, tab.[name]
			, '_'
			, col.[name]
			, ''', ''OBJECT'';')
FROM sys.default_constraints AS def
INNER JOIN sys.tables AS tab ON def.parent_object_id = tab.object_id
INNER JOIN sys.schemas AS sch ON def.schema_id = sch.schema_id
INNER JOIN sys.columns AS col ON def.parent_object_id = col.object_id AND def.parent_column_id = col.column_id
WHERE def.is_system_named = 1
AND tab.[type] = 'U'
-- Ignore system objects:
AND def.is_ms_shipped = 0
AND tab.is_ms_shipped = 0
AND tab.[name] NOT IN ('sysdiagrams','dtproperties')
