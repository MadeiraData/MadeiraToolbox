DECLARE @Search sysname = N'%Your String Here%'

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @CurrDB sysname, @ExecuteSql nvarchar(500)

IF OBJECT_ID('tempdb..#results') IS NOT NULL DROP TABLE #results;
CREATE TABLE #results
(
	DatabaseName sysname NULL,
	SchemaName sysname NULL,
	ObjectName sysname NULL,
	ObjectType sysname NULL,
	[Definition] nvarchar(max) NULL
);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT name
FROM sys.databases
WHERE HAS_DBACCESS(name) = 1
AND state_desc = 'ONLINE'

OPEN DBs

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @ExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO #results
	EXEC @ExecuteSql N'
SELECT
	DatabaseName	= DB_NAME() ,
	SchemaName	= SCHEMA_NAME ([Objects].schema_id) ,
	ObjectName	= [Objects].[name] ,
	ObjectType	= [Objects].[type_desc] ,
	[Definition]	= SQLModules.[definition]
FROM
	sys.sql_modules AS SQLModules
INNER JOIN
	sys.objects AS [Objects]
ON
	SQLModules.[object_id] = [Objects].[object_id]
WHERE
	SQLModules.[definition] LIKE @Search;', N'@Search nvarchar(max)', @Search
END

CLOSE DBs
DEALLOCATE DBs

SELECT *
FROM #results


SELECT j.job_id, j.name AS job_name, js.*
FROM msdb..sysjobsteps AS js
INNER JOIN msdb..sysjobs AS j ON js.job_id = j.job_id
WHERE js.command LIKE @Search