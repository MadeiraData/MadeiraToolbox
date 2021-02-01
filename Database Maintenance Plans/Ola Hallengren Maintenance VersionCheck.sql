-- Check version of Ola Hallengren's SQL Server Maintenance Solution 
-- source: https://ola.hallengren.com/scripts/misc/VersionCheck.sql
DECLARE @VersionKeyword nvarchar(max), @CMD NVARCHAR(MAX)

SET @VersionKeyword = '--// Version: '

SET @CMD = N'
SELECT DB_NAME() AS DatabaseName,
       sys.schemas.[name] AS SchemaName,
       sys.objects.[name] AS ObjectName,
       CASE WHEN CHARINDEX(@VersionKeyword,OBJECT_DEFINITION(sys.objects.[object_id])) > 0 THEN SUBSTRING(OBJECT_DEFINITION(sys.objects.[object_id]),CHARINDEX(@VersionKeyword,OBJECT_DEFINITION(sys.objects.[object_id])) + LEN(@VersionKeyword) + 1, 19) END AS [Version],
       CAST(CHECKSUM(CAST(OBJECT_DEFINITION(sys.objects.[object_id]) AS nvarchar(max)) COLLATE SQL_Latin1_General_CP1_CI_AS) AS bigint) AS [Checksum]
FROM sys.objects
INNER JOIN sys.schemas ON sys.objects.[schema_id] = sys.schemas.[schema_id]
WHERE sys.schemas.[name] = ''dbo''
AND sys.objects.[name] IN(''CommandExecute'',''DatabaseBackup'',''DatabaseIntegrityCheck'',''IndexOptimize'')
ORDER BY sys.schemas.[name] ASC, sys.objects.[name] ASC'

DECLARE @DB SYSNAME, @ExecuteSQL SYSNAME
DECLARE @Results AS TABLE (DatabaseName SYSNAME, SchemaName SYSNAME, ObjectName SYSNAME, VersionTimestamp VARCHAR(25), ObjectChecksum INT);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE state = 0

OPEN DBs

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @DB
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @ExecuteSQL = QUOTENAME(@DB) + N'..sp_executesql'

	INSERT INTO @Results
	EXEC @ExecuteSQL @CMD, N'@VersionKeyword nvarchar(max)', @VersionKeyword
END

CLOSE DBs
DEALLOCATE DBs


SELECT *
FROM @Results

-- Version History
-- https://ola.hallengren.com/versions.html

-- If you don't have the latest version, you can download and install the latest version
-- https://ola.hallengren.com/scripts/MaintenanceSolution.sql

-- Make sure to change this line in the script so it doesn't create new Agent jobs: SET @CreateJobs = 'N'
