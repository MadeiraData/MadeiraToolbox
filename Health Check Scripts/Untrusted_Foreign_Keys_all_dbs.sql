IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (DBName SYSNAME, SchemaName SYSNAME, TableName SYSNAME, FullTableName AS QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName), UntrustedObject SYSNAME);

INSERT INTO #tmp(DBName, SchemaName, TableName, UntrustedObject)
EXEC sp_MSforeachdb 'IF EXISTS (SELECT * FROM sys.databases WHERE state_desc = ''ONLINE'' AND name = ''?'' AND DATABASEPROPERTYEX(''?'', ''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
SELECT ''?'', OBJECT_SCHEMA_NAME(parent_object_id), OBJECT_NAME(parent_object_id), [name]
FROM [?].sys.foreign_keys
WHERE is_not_trusted = 1 AND is_not_for_replication = 0 AND is_disabled = 0;
END'
 
SELECT
	*
	, CommandToRemediate = N'USE ' + QUOTENAME(DBName) + N'; ALTER TABLE ' + FullTableName + N' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(UntrustedObject) + N';'
FROM #tmp

