IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (DBName SYSNAME, SchemaName SYSNAME, TableName SYSNAME, IsDisabled BIT, FullTableName AS QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName), UntrustedObject SYSNAME);

DECLARE @CMD NVARCHAR(MAX)
SET @CMD = N'SELECT DB_NAME(), OBJECT_SCHEMA_NAME(parent_object_id), OBJECT_NAME(parent_object_id), [name], is_disabled
FROM sys.foreign_keys
WHERE (is_not_trusted = 1 OR is_disabled = 1) AND is_not_for_replication = 0;'

IF CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
BEGIN
	INSERT INTO #tmp(DBName, SchemaName, TableName, UntrustedObject, IsDisabled)
	EXEC(@CMD)
END
ELSE
BEGIN
	SET @CMD = N'IF EXISTS (SELECT * FROM sys.databases WHERE state_desc = ''ONLINE'' AND name = ''?'' AND DATABASEPROPERTYEX(''?'', ''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
' + @CMD + N'
END'
	INSERT INTO #tmp(DBName, SchemaName, TableName, UntrustedObject, IsDisabled)
	EXEC sp_MSforeachdb @CMD
END
 
SELECT
	*
	, CommandToRemediate = N'USE ' + QUOTENAME(DBName) + N'; BEGIN TRY ALTER TABLE ' + FullTableName + N' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(UntrustedObject) + N'; END TRY
 BEGIN CATCH PRINT ERROR_MESSAGE(); END CATCH'
FROM #tmp

