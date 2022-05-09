USE [master];
GO
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#AllDatabases') IS NOT NULL DROP TABLE #AllDatabases;
CREATE TABLE #AllDatabases
(
	DBName sysname,
	Result NVARCHAR(256)
);

DECLARE @CMD nvarchar(MAX), @CurrDB sysname, @spExecuteSql nvarchar(1000)

SET @CMD = N'
SELECT
	DB_NAME(),
	N''USE '' + QUOTENAME(DB_NAME()) + N''; REVOKE CONNECT FROM GUEST;''
FROM sys.sysusers
WHERE [name] = ''guest''
AND hasdbaccess = 1
'

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND state_desc = 'ONLINE'
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
AND database_id > 4

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;
	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO #AllDatabases
	EXEC @spExecuteSql @CMD;
END

CLOSE DBs;
DEALLOCATE DBs;

SELECT *
FROM #AllDatabases

-- DROP TABLE #AllDatabases;
