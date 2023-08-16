USE [master]
GO
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
IF OBJECT_ID('tempdb..#ObjectsWithTableParams') IS NOT NULL DROP TABLE #ObjectsWithTableParams;
CREATE TABLE #ObjectsWithTableParams (objectId int, databaseId int, PRIMARY KEY CLUSTERED (objectId, databaseId) WITH(IGNORE_DUP_KEY=ON));

EXEC sp_MSforeachdb N'IF EXISTS (SELECT * FROM sys.databases WHERE database_id > 4 AND name = ''?'' AND state_desc = ''ONLINE'')
INSERT INTO #ObjectsWithTableParams
SELECT DISTINCT ob.object_id, DB_ID(''?'') AS database_id
FROM [?].sys.objects as ob
left join [?].sys.parameters AS p
on p.object_id = ob.object_id
left join [?].sys.types as t
on p.user_type_id = t.user_type_id
and p.system_type_id = t.system_type_id
where ob.is_ms_shipped = 0 AND
(t.is_table_type = 1 OR ob.[type] IN( ''TF'',''FN''))
OPTION(RECOMPILE)'
;

WITH XMLNAMESPACES (DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan', N'http://schemas.microsoft.com/sqlserver/2004/07/showplan' AS p) 
select 
DatabaseName = DB_NAME(p.dbid),
cp.objtype,
CommandWithVarTable = ISNULL(CONVERT(nvarchar(max), QUOTENAME(DB_NAME(t.dbid)) + '.' + QUOTENAME(OBJECT_SCHEMA_NAME(t.objectid, t.dbid)) + '.' + QUOTENAME(OBJECT_NAME(t.objectid, t.dbid))), t.text),
GetDefinitionCmd = N'USE ' + QUOTENAME(DB_NAME(p.dbid)) + N'; SELECT Def = OBJECT_DEFINITION(OBJECT_ID(' + QUOTENAME(QUOTENAME(OBJECT_SCHEMA_NAME(t.objectid, t.dbid)) + '.' + QUOTENAME(OBJECT_NAME(t.objectid, t.dbid)), N'''') + N'));',
cp.plan_handle,
p.query_plan,
cp.usecounts,
cp.size_in_bytes
from sys.dm_exec_cached_plans as cp
cross apply sys.dm_exec_query_plan(cp.plan_handle) as p
cross apply sys.dm_exec_sql_text(cp.plan_handle) as t
WHERE p.query_plan.exist('//Object[substring(@Table,1,2) = "[@"]') = 1
AND t.dbid > 4 and DB_NAME(t.dbid) NOT IN('ReportServer','MadeiraPerformanceMonitoring','DBA','SQLWATCH','distribution','HangFire')
AND ISNULL(OBJECT_NAME(t.objectid, t.dbid),'(null)') NOT IN('IndexOptimize','CommandExecute')
AND t.text NOT LIKE N'%@jdbc_temp_fkeys_result%'
AND t.text NOT LIKE N'%@inserted0%'
 AND
 (
 -- include all specific column references (seek predicates) 
    p.query_plan.exist('//Object[substring(@Table,1,2) = "[@"]/../SeekPredicates/SeekPredicateNew/SeekKeys/Prefix/RangeColumns/ColumnReference') = 1
 --include all joins with table varibles by checking that no specific columns are referenced in the execution plan
 OR p.query_plan.exist('//ColumnReference[substring(@Table,1,1) = "@"]') = 1
 )
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
ORDER BY cp.usecounts DESC, cp.size_in_bytes DESC
OPTION(RECOMPILE);

--DROP TABLE #ObjectsWithTableParams;
