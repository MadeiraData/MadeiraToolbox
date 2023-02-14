/*
Online Index Rebuild and Resume by Default
===========================================
Author: Eitan Blumin
Date: 2023-02-14
Description:
This script returns all databases where the database scoped configurations
ELEVATE_ONLINE or ELEVATE_RESUMABLE are turned off.
This is relevant to SQL Server versions 2019 and newer only, Enterprise edition or equivalent.
*/
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

DECLARE @ShowRemediation bit = 0 -- change to 1 to output remediation commands for all relevant databases


DECLARE @CurrDB sysname, @SpExecuteSQL nvarchar(501)
DECLARE @Results AS TABLE (dbname sysname, config sysname)

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
AND database_id > 4
AND CONVERT(int, SERVERPROPERTY('EngineEdition')) NOT IN (1,2,4)

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @SpExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @Results
	EXEC @SpExecuteSQL N'select DB_NAME(), [name]
from sys.database_scoped_configurations
where name IN (''ELEVATE_ONLINE'', ''ELEVATE_RESUMABLE'')
and value = ''OFF''' WITH RECOMPILE;

END

CLOSE DBs;
DEALLOCATE DBs;

DECLARE @RCount int, @DBCount int;
SELECT @RCount = COUNT(*), @DBCount = COUNT(DISTINCT dbname) FROM @Results;

IF @RCount <= 10
BEGIN
	SELECT N'In server: ' + @@SERVERNAME + N', database: ' + QUOTENAME(dbname) + N', the database scoped configuration is disabled: ' + config, 1
	FROM @Results
END
ELSE IF @DBCount <= 10
BEGIN
	SELECT N'In server: ' + @@SERVERNAME + N', database: ' + QUOTENAME(dbname) + N', has ' + CONVERT(nvarchar(max), COUNT(*)) + N' disabled database scoped configuration(s)', COUNT(*)
	FROM @Results
	GROUP BY dbname
END
ELSE
BEGIN
	SELECT N'In server: ' + @@SERVERNAME + N', there are ' + CONVERT(nvarchar(max), @DBCount) + N' databases with disabled database scoped configurations', @RCount
END

IF @ShowRemediation = 1 AND @RCount > 0
BEGIN
	SELECT dbname, config,
	RemediationCmd = N'USE ' + QUOTENAME(dbname) + N'; ALTER DATABASE SCOPED CONFIGURATION SET ' + config + N' = WHEN_SUPPORTED;'
	FROM @Results
END