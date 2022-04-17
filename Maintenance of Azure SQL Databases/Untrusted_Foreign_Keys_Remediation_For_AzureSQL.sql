--declare variable for cursor
DECLARE @CommandToRemediate NVARCHAR(MAX)

--drop and create temp table
IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;
CREATE TABLE #tmp
(
	DBName            SYSNAME		,
	TableName         SYSNAME		,
	UntrustedObject   NVARCHAR(1000)
)
;

--populate temp table
IF EXISTS
(
	SELECT
		*
	FROM
		sys.databases 
	WHERE
		state_desc = 'ONLINE'
		AND
		DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
)
INSERT INTO #tmp
SELECT
	DB_NAME(), 
	QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id, DB_ID())) + '.' + QUOTENAME(OBJECT_NAME(parent_object_id, DB_ID())), 
	QUOTENAME(name)
FROM
	sys.foreign_keys
WHERE
	is_not_trusted = 1
	AND
	is_not_for_replication = 0
	AND is_disabled = 0
 
--optional content check of the temp table (comment out if you don't want it)
SELECT
	DBName, 
	TableName, 
	UntrustedObject, 
	CommandToRemediate = N'ALTER TABLE ' + TableName + N' WITH CHECK CHECK CONSTRAINT ' + UntrustedObject
FROM
	#tmp

--declare cursor for executing all remediation commands for the present database
DECLARE CommandToRemediate CURSOR
FOR 
	SELECT
		CommandToRemediate = N'ALTER TABLE ' + TableName + N' WITH CHECK CHECK CONSTRAINT ' + UntrustedObject
	FROM
		#tmp

OPEN CommandToRemediate  

FETCH NEXT FROM CommandToRemediate INTO @CommandToRemediate

WHILE @@FETCH_STATUS = 0  
BEGIN  
	  EXEC sp_executesql @CommandToRemediate
      FETCH NEXT FROM CommandToRemediate INTO @CommandToRemediate
END 

CLOSE CommandToRemediate  
DEALLOCATE CommandToRemediate 
;

DROP TABLE #tmp
GO