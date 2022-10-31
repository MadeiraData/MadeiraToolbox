/*
===========================================================================================
Drop or Rebuild All Disabled Indexes
===========================================================================================
Author: Eitan Blumin
Date: 2022-10-30
Description:

This script detects all currently disabled indexes for all databases.
It outputs their details, as well as a command to rebuild each index (to re-enable it),
and a command to drop each disabled index. The generated commands are idempotent.

The script also automatically detects whether current version supports online rebuild or not
and applies WITH(ONLINE=ON) accordingly. You may optionally override this behavior by setting
the variable @RebuildOnline to either 1 (true) or 0 (false) to force a specific behavior.

You may filter the results by specific database by changing the value for @FilterByDatabaseName
===========================================================================================
*/
DECLARE
	 @FilterByDatabaseName	sysname		= NULL	/* NULL = All accessible databases */
	,@RebuildOnline		bit		= NULL	/* 0 = OFFLINE | 1 = ONLINE | NULL = Auto-detect */
	
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
IF @RebuildOnline IS NULL SET @RebuildOnline = CASE WHEN CONVERT(int, SERVERPROPERTY('EngineEdition')) IN (3,5,8,9,11) THEN 1 ELSE 0 END;

DECLARE @Results AS table 
(
[database_name] sysname NOT NULL,
[schema_name] sysname NOT NULL,
[table_name] sysname NOT NULL,
[index_name] sysname NOT NULL,
filter_definition nvarchar(MAX) NULL,
is_unique bit NULL,
is_unique_constraint bit NULL
)

DECLARE @CMD nvarchar(MAX), @CurrDB sysname, @SpExecuteSql nvarchar(523)

SET @CMD = N'SELECT
  [database_name] = DB_NAME()
, [schema_name] = OBJECT_SCHEMA_NAME(ix.object_id)
, [table_name] = OBJECT_NAME(ix.object_id)
, [index_name] = ix.[name]
, ix.filter_definition
, ix.is_unique
, ix.is_unique_constraint
FROM sys.indexes AS ix
WHERE is_disabled = 1'

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE [name] = @FilterByDatabaseName
OR (@FilterByDatabaseName IS NULL
	AND HAS_DBACCESS([name]) = 1
	AND [state] = 0
	AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
	)

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @SpExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @Results
	EXEC @SpExecuteSql @CMD

END
CLOSE DBs;
DEALLOCATE DBs;


SELECT
  [database_name]
, [schema_name]
, [table_name]
, [index_name]
, filter_definition
, is_unique
, is_unique_constraint
, RebuildCmd = N'USE ' + QUOTENAME([database_name]) + N'; IF INDEXPROPERTY(OBJECT_ID(''' + QUOTENAME([schema_name]) + N'.' + QUOTENAME([table_name]) + N'''), '''
	+ [index_name] + N''', ''IsDisabled'') = 1 ALTER INDEX ' + QUOTENAME([index_name]) + N' ON ' + QUOTENAME([schema_name]) + N'.' + QUOTENAME([table_name]) 
	+ N' REBUILD' + CASE WHEN @RebuildOnline = 1 THEN N' WITH(ONLINE=ON);' ELSE N';' END
, DropCmd = N'USE ' + QUOTENAME([database_name]) + N'; IF INDEXPROPERTY(OBJECT_ID(''' + QUOTENAME([schema_name]) + N'.' + QUOTENAME([table_name]) + N'''), '''
	+ [index_name] + N''', ''IsDisabled'') = 1 DROP INDEX ' + QUOTENAME([index_name]) + N' ON ' + QUOTENAME([schema_name]) + N'.' + QUOTENAME([table_name]) + N';'
FROM @Results
