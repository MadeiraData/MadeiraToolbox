/*
========================================================
Find explicit index hint references for a specific index
========================================================
Author: Eitan Blumin
Date: 2022-08-17
Description:
This script searches both in SQL Modules (Functions, Procedures, Triggers)
as well as in the current SQL Plan Cache.
This may NOT cover ad-hoc commands that are not currently in the cache.

This script is a variation of the script by Aaron Bertrand provided in this article:
https://www.mssqltips.com/sqlservertip/7026/sql-server-index-hint-stored-procedure-query/

You must specify an index name for the @IndexName parameter (without quotes)
*/

DECLARE
	 @IndexName sysname = N'IX_IndexName'

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#results') IS NOT NULL DROP TABLE #results;
CREATE TABLE #results
(
dbname        sysname NULL,
schemaname    sysname NULL, 
tablename     sysname NULL,
indexname     sysname NULL,
modulename    nvarchar(600) NULL,
moduleid      int NULL,
handle        varchar(64) NULL, 
statement     nvarchar(max) NULL,
fullstatement nvarchar(max) NULL,
findingtype   varchar(20) NOT NULL
);

DECLARE @sql nvarchar(max) = N'
SELECT 
[Database]     = QUOTENAME(DB_NAME()), 
[Schema]       = NULL, 
[Table]        = NULL, 
[Index]        = NULL, 
SourceModule   = s.name COLLATE DATABASE_DEFAULT
        + N''.'' + o.name COLLATE DATABASE_DEFAULT 
        + N'' ('' + RTRIM(o.type) + N'')'', 
SourceModuleID = m.object_id,
handle         = NULL, 
Statement      = NULL, 
FullStatement  = m.definition,
FindingType    = ''SqlModules''
FROM sys.sql_modules AS m
INNER JOIN sys.objects AS o ON m.object_id = o.object_id
INNER JOIN sys.schemas AS s ON o.[schema_id] = s.[schema_id]
WHERE m.definition LIKE N''%INDEX%[(=]%' + @IndexName + N'%'';';

DECLARE @CurrDB sysname, @SpExecuteSql nvarchar(1024);
DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND state = 0

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @SpExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT #results
	EXEC @SpExecuteSql @sql;
	
	RAISERROR(N'Found %d SqlModules in: %s', 0, 1, @@ROWCOUNT, @CurrDB) WITH NOWAIT;
END

CLOSE DBs;
DEALLOCATE DBs;

RAISERROR(N'Checking in PlanCache...', 0, 1) WITH NOWAIT;

SET @IndexName = QUOTENAME(@IndexName);
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH XMLNAMESPACES(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan') 
INSERT #results 
SELECT 
[Database]	= COALESCE(obj.value (N'(@Database)[1]', N'sysname'), QUOTENAME(DB_NAME(sqltext.dbid)), QUOTENAME(DB_NAME(qp.dbid))),
[Schema]	= obj.value (N'(@Schema)[1]',         N'sysname'),
[Table]		= obj.value (N'(@Table)[1]',          N'sysname'),
[Index]		= obj.value (N'(@Index)[1]',          N'sysname'),
SourceModule	= OBJECT_SCHEMA_NAME(qp.objectid, COALESCE(sqltext.dbid, qp.dbid, DB_ID(obj.value (N'(@Database)[1]', N'sysname')))) COLLATE DATABASE_DEFAULT 
			+ N'.'
			+ OBJECT_NAME(qp.objectid, COALESCE(sqltext.dbid, qp.dbid, DB_ID(obj.value (N'(@Database)[1]', N'sysname')))) COLLATE DATABASE_DEFAULT ,
SourceModuleID	= qp.objectid,
handle		= CONVERT(varchar(64), qs.plan_handle, 1),
Statement	= LTRIM(SUBSTRING(sqltext.text, (qs.statement_start_offset / 2) + 1, 
        (CASE qs.statement_end_offset 
        WHEN -1 THEN DATALENGTH(sqltext.text) 
        ELSE qs.statement_end_offset 
        END - qs.statement_start_offset) / 2 + 1)),
FullStatement  = stmt.value(N'(@StatementText)[1]',  N'nvarchar(max)'),
FindingType = 'PlanCache'
FROM sys.dm_exec_query_stats as qs
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
CROSS APPLY query_plan.nodes
(N'/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS batch(stmt)
CROSS APPLY stmt.nodes
(N'.//IndexScan[@ForcedIndex=1]/Object[@Index=sql:variable("@IndexName")]') AS idx(obj)
CROSS APPLY sys.dm_exec_sql_text(qs.plan_handle) AS sqltext
OPTION(MAXDOP 1, RECOMPILE);  

RAISERROR(N'Found %d entries in PlanCache', 0, 1, @@ROWCOUNT) WITH NOWAIT;

SELECT *
FROM #results;