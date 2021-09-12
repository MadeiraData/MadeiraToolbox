SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
IF OBJECT_ID('tempdb..#ObjectsWithTableParams') IS NOT NULL DROP TABLE #ObjectsWithTableParams;
CREATE TABLE #ObjectsWithTableParams (objectId int, databaseId int, PRIMARY KEY CLUSTERED (objectId, databaseId) WITH(IGNORE_DUP_KEY=ON));

EXEC sp_MSforeachdb N'
IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND name = ''?'' AND state_desc = ''ONLINE'')
INSERT INTO #ObjectsWithTableParams
SELECT DISTINCT ob.object_id, DB_ID(''?'') AS database_id
FROM [?].sys.objects as ob
left join [?].sys.parameters AS p
on p.object_id = ob.object_id
left join [?].sys.types as t
on p.user_type_id = t.user_type_id
and p.system_type_id = t.system_type_id
where ob.is_ms_shipped = 0 AND
(t.is_table_type = 1 OR ob.[type] IN( ''TF'',''FN''))'
;
IF OBJECT_ID('tempdb..#tmp') IS NOT NULL DROP TABLE #tmp;

WITH XMLNAMESPACES (DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan', N'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p) 
select
  DatabaseName = DB_NAME(t.dbid)
, CommandWithVarTable = CONVERT(nvarchar(max), QUOTENAME(DB_NAME(t.dbid)) + '.' + QUOTENAME(OBJECT_SCHEMA_NAME(t.objectid, t.dbid)) + '.' + QUOTENAME(OBJECT_NAME(t.objectid, t.dbid)))
, PlanCount = COUNT(*)
, TotalUseCount = SUM(cp.usecounts)
into #tmp
from sys.dm_exec_cached_plans as cp
cross apply sys.dm_exec_query_plan(cp.plan_handle) as p
cross apply sys.dm_exec_sql_text(cp.plan_handle) as t
WHERE p.query_plan.exist('//Object[substring(@Table,1,2) = "[@"]') = 1
 AND t.dbid > 4 and DB_NAME(t.dbid) NOT IN('ReportServer','MadeiraPerformanceMonitoring','DBA')
 AND ISNULL(OBJECT_NAME(t.objectid, t.dbid), N'') NOT IN('IndexOptimize','CommandExecute')
 AND ISNULL(OBJECT_NAME(t.objectid, t.dbid), N'') NOT LIKE 'aspnet[_]%'
 AND t.text NOT LIKE N'%@jdbc_temp_fkeys_result%'
 AND cp.objtype = 'Proc'
-- ignore databases in secondary AG replicas
 AND DB_NAME(t.dbid) NOT IN
 (
	 SELECT adc.database_name
	 FROM sys.availability_databases_cluster adc
	 INNER JOIN sys.dm_hadr_availability_group_states ags ON adc.group_id = ags.group_id
	 INNER JOIN sys.availability_replicas ar ON ar.group_id = ags.group_id
	 WHERE ags.primary_replica <> @@SERVERNAME
	 AND ar.replica_server_name = @@SERVERNAME
	 AND ar.secondary_role_allow_connections_desc = 'NO'
 )
 -- ignore objects with table valued parameters and table functions
 AND NOT EXISTS
 (
	 SELECT NULL
	 FROM #ObjectsWithTableParams AS tmp
	 WHERE tmp.databaseId = t.dbid
	 AND tmp.objectId = t.objectId
 )
 GROUP BY t.objectid, t.dbid
--DROP TABLE #ObjectsWithTableParams;

ALTER TABLE #tmp ADD ModuleDefinition nvarchar(max) NULL;

DECLARE @CurrDB sysname, @CurrObject nvarchar(1000), @CurrDefinition nvarchar(max), @spExecuteSQL nvarchar(1000);

DECLARE Obj CURSOR
LOCAL FAST_FORWARD
FOR
SELECT DatabaseName, CommandWithVarTable
FROM #tmp

OPEN Obj;
WHILE 1=1
BEGIN
	FETCH NEXT FROM Obj INTO @CurrDB, @CurrObject
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @CurrDefinition = NULL;
	SET @spExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql'
	EXEC @spExecuteSQL N'SELECT @def = OBJECT_DEFINITION(OBJECT_ID(@ObjectName))', N'@def nvarchar(max) OUTPUT, @ObjectName nvarchar(1000)', @CurrDefinition OUTPUT, @CurrObject;

	UPDATE #tmp SET ModuleDefinition = @CurrDefinition
	WHERE DatabaseName = @CurrDB AND CommandWithVarTable = @CurrObject
END

CLOSE Obj;
DEALLOCATE Obj;

SELECT *
FROM #tmp
ORDER BY TotalUseCount DESC
