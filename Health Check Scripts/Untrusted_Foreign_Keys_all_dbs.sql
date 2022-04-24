SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE
	 @IncludeDisabledFKs bit = 0 -- change to 1 to also check for disabled constraints
	,@ApplyCommands varchar(10) = '$(RunRemediation)' -- To apply commands, use in Powershell with Invoke-SqlCmd and adding: -Variable "RunRemediation=Yes"

IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp (DBName SYSNAME, SchemaName SYSNAME, TableName SYSNAME, IsDisabled BIT, FullTableName AS QUOTENAME(SchemaName) + N'.' + QUOTENAME(TableName), UntrustedObject SYSNAME);

DECLARE @CMD NVARCHAR(MAX)
SET @CMD = N'SELECT DB_NAME(), OBJECT_SCHEMA_NAME(parent_object_id), OBJECT_NAME(parent_object_id), [name], is_disabled
FROM sys.foreign_keys
WHERE (is_not_trusted = 1 '
+ CASE WHEN @IncludeDisabledFKs = 1 THEN N'OR is_disabled = 1' ELSE N'AND is_disabled = 0' END + N'
) AND is_not_for_replication = 0;'

IF CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5
BEGIN
	IF DATABASEPROPERTYEX(DB_NAME(), 'Updateability') = 'READ_WRITE'
	INSERT INTO #tmp(DBName, SchemaName, TableName, UntrustedObject, IsDisabled)
	EXEC(@CMD)
END
ELSE
BEGIN
	SET @CMD = N'IF DATABASEPROPERTYEX(''?'', ''Status'') = ''ONLINE'' AND HAS_DBACCESS(''?'') = 1 AND DATABASEPROPERTYEX(''?'', ''Updateability'') = ''READ_WRITE''
BEGIN
USE [?];
' + @CMD + N'
END'
	INSERT INTO #tmp(DBName, SchemaName, TableName, UntrustedObject, IsDisabled)
	EXEC sp_MSforeachdb @CMD
END
 
SELECT
	*
	, CommandToRemediate = CASE WHEN CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5 THEN N'' ELSE N'USE ' + QUOTENAME(DBName) + N'; ' END
	+ N'BEGIN TRY ALTER TABLE ' + FullTableName + N' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(UntrustedObject) + N'; END TRY BEGIN CATCH PRINT ERROR_MESSAGE(); END CATCH'
FROM #tmp

IF LEFT(@ApplyCommands,1) = 'Y'
BEGIN
	DECLARE Cmds CURSOR
	LOCAL FAST_FORWARD
	FOR
	SELECT CASE WHEN CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5 THEN N'' ELSE N'USE ' + QUOTENAME(DBName) + N'; ' END
	+ N'BEGIN TRY ALTER TABLE ' + FullTableName + N' WITH CHECK CHECK CONSTRAINT ' + QUOTENAME(UntrustedObject) + N'; END TRY BEGIN CATCH PRINT ERROR_MESSAGE(); END CATCH'
	FROM #tmp

	OPEN Cmds

	WHILE 1=1
	BEGIN
		FETCH NEXT FROM Cmds INTO @CMD;
		IF @@FETCH_STATUS <> 0 BREAK;

		RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
		EXEC(@CMD);
	END

	CLOSE Cmds;
	DEALLOCATE Cmds;
END