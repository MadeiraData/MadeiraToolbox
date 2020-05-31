IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (DBName SYSNAME, SchemaName SYSNAME, TableName SYSNAME, FullTableName AS QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName), UntrustedObject SYSNAME);

DECLARE @CMD NVARCHAR(MAX)
SET @CMD = N'SELECT DB_NAME(), OBJECT_SCHEMA_NAME(parent_object_id), OBJECT_NAME(parent_object_id), [name]
FROM sys.check_constraints
WHERE is_not_trusted = 1 AND is_not_for_replication = 0 AND is_disabled = 0;'

IF CONVERT(varchar(300),SERVERPROPERTY('Edition')) = 'SQL Azure'
BEGIN
	SET @CMD = REPLACE(@CMD, '?', DB_NAME())
	INSERT INTO #tmp(DBName, SchemaName, TableName, UntrustedObject)
	EXEC(@CMD)
END
ELSE
BEGIN
	SET @CMD = N'IF EXISTS (SELECT * FROM sys.databases WHERE state_desc = ''ONLINE'' AND name = ''?'' AND DATABASEPROPERTYEX(''?'', ''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
' + @CMD + N'
END'
	INSERT INTO #tmp(DBName, SchemaName, TableName, UntrustedObject)
	EXEC sp_MSforeachdb @CMD
END
 
SELECT
	*
	, CommandToRemediate = N'USE ' + QUOTENAME(DBName) + N'; ALTER TABLE ' + FullTableName + N' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(UntrustedObject) + N';'
FROM #tmp
