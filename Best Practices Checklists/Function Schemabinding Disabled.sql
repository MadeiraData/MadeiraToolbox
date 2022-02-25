DECLARE
	 @WithDependencies BIT = NULL -- optionally filter only on functions with/without dependencies (1 = with, 0 = without, NULL = all)
	,@IgnoreFunctionsDependentOnSynonyms BIT = 1 -- optionally filter out functions that depend on synonyms (cannot be schemabound)
	,@IgnoreFunctionsWithConstraintDependencies BIT = 1 -- optionally filter out functions that have constraints dependant on them (cannot be altered)
	,@IgnoreFunctionsDependentOnLinkedServers BIT = 1 -- optionally filter out functions that depend on linked servers (cannot be schemabound)

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#temp_Schemabinding') IS NOT NULL DROP TABLE #temp_Schemabinding;
CREATE TABLE #temp_Schemabinding
(
	[database_name] SYSNAME,
	[schema_name] SYSNAME,
	[object_name] SYSNAME,
	[definition] NVARCHAR(MAX),
	[has_dependencies] BIT,
	[has_synonym_dependencies] BIT,
	[has_constraint_dependencies] BIT,
	[name] AS (QUOTENAME([schema_name]) + N'.' + QUOTENAME([object_name]))
);

DECLARE @CMD_Template NVARCHAR(MAX)

SET @CMD_Template  = N'WITH Exclusions AS
('
-- exclude modules invalid for schemabinding
+ N'
SELECT d.referenced_major_id AS object_id
from sys.sql_dependencies AS d
WHERE OBJECTPROPERTY(d.referenced_major_id, ''IsScalarFunction'') = 1 AND (
d.referenced_major_id = d.object_id
OR OBJECT_DEFINITION(d.referenced_major_id) LIKE N''%sp_OACreate%sp_OA%''
OR LOWER(OBJECT_DEFINITION(d.referenced_major_id)) LIKE N''%exec%sp_executesql%''
OR OBJECT_SCHEMA_NAME(d.referenced_major_id) IN (''tSQLt'',''sys'')
)
UNION ALL
select d.referenced_id
from sys.sql_expression_dependencies AS d
WHERE OBJECTPROPERTY(d.referenced_id, ''IsScalarFunction'') = 1 AND (
d.referencing_id = d.referenced_id
OR OBJECT_DEFINITION(d.referenced_id) LIKE N''%sp_OACreate%sp_OA%''
OR LOWER(OBJECT_DEFINITION(d.referenced_id)) LIKE N''%exec%sp_executesql%''
OR OBJECT_SCHEMA_NAME(d.referenced_id) IN (''tSQLt'',''sys'')
)'
-- dependant on synonyms
+ CASE WHEN @IgnoreFunctionsDependentOnSynonyms = 1 THEN N'
UNION ALL
select d.object_id
from sys.sql_dependencies AS d
INNER JOIN sys.synonyms AS syn ON d.referenced_major_id = syn.object_id
UNION ALL
select d.referencing_id
from sys.sql_expression_dependencies AS d
INNER JOIN sys.synonyms AS syn ON d.referenced_id = syn.object_id'
ELSE N''
END
-- has constraint dependencies
+ CASE WHEN @IgnoreFunctionsWithConstraintDependencies = 1 THEN N'
UNION ALL
select d.referenced_major_id
from sys.sql_dependencies AS d
INNER JOIN sys.sysconstraints AS con ON d.object_id = con.constid
WHERE OBJECTPROPERTY(d.referenced_major_id, ''IsScalarFunction'') = 1
UNION ALL
select d.referenced_id
from sys.sql_expression_dependencies AS d
INNER JOIN sys.sysconstraints AS con ON d.referencing_id = con.constid
WHERE OBJECTPROPERTY(d.referenced_id, ''IsScalarFunction'') = 1'
ELSE N''
END
-- Recursive depdendencies
+ N'
), ExclusionTree1 AS
(
select d.object_id
from sys.sql_dependencies AS d
INNER JOIN Exclusions AS Tree ON d.referenced_major_id = Tree.object_id
WHERE OBJECTPROPERTY(d.referenced_major_id, ''IsScalarFunction'') = 1
AND d.referenced_major_id <> d.object_id
UNION ALL
select d.object_id
from sys.sql_dependencies AS d
INNER JOIN ExclusionTree1 AS Tree ON d.referenced_major_id = Tree.object_id
WHERE OBJECTPROPERTY(d.referenced_major_id, ''IsScalarFunction'') = 1
AND d.referenced_major_id <> d.object_id
)
SELECT DB_NAME(), OBJECT_SCHEMA_NAME(OB.id), OB.name, MO.[definition]
, HasDependencies = CASE WHEN EXISTS
(
select NULL
from sys.sql_dependencies AS d
WHERE d.object_id = OB.id
UNION ALL
select NULL
from sys.sql_expression_dependencies AS d
WHERE d.referencing_id = OB.id
) THEN 1 ELSE 0 END
, HasSynonymDependencies = CASE WHEN EXISTS
(
select NULL
from sys.sql_dependencies AS d
INNER JOIN sys.synonyms AS syn ON d.referenced_major_id = syn.object_id
WHERE d.object_id = OB.id
UNION ALL
select NULL
from sys.sql_expression_dependencies AS d
INNER JOIN sys.synonyms AS syn ON d.referenced_id = syn.object_id
WHERE d.referencing_id = OB.id
) THEN 1 ELSE 0 END
, HasConstraintDependencies = CASE WHEN EXISTS
(
select NULL
from sys.sql_dependencies AS d
INNER JOIN sys.sysconstraints AS con ON d.object_id = con.constid
WHERE d.referenced_major_id = OB.id
UNION ALL
select NULL
from sys.sql_expression_dependencies AS d
INNER JOIN sys.sysconstraints AS con ON d.referencing_id = con.constid
WHERE d.referenced_id = OB.id
) THEN 1 ELSE 0 END
FROM sys.sysobjects OB
INNER JOIN sys.sql_modules MO
ON OB.id = MO.object_id
AND OB.type = ''FN''
AND MO.is_schema_bound = 0
WHERE MO.definition NOT LIKE N''%sp_OACreate%sp_OA%''
AND LOWER(MO.definition) NOT LIKE N''%exec%sp_executesql%''
AND OB.name NOT IN (''fn_diagramobjects'')
AND OBJECT_SCHEMA_NAME(OB.id) NOT IN (''tSQLt'',''sys'')
AND OB.id NOT IN (select object_id FROM Exclusions)
AND OB.id NOT IN (select object_id FROM ExclusionTree1)
OPTION(MAXRECURSION 100)'

IF CONVERT(int, SERVERPROPERTY('EngineEdition')) = 5 -- Azure SQL DB
BEGIN
	INSERT INTO #temp_Schemabinding
	exec (@CMD_Template)
END
ELSE
BEGIN
	DECLARE @CurrDB sysname, @spExecuteSql nvarchar(1000)

	DECLARE DBs CURSOR LOCAL FAST_FORWARD FOR
	SELECT [name]
	FROM sys.databases
	WHERE database_id > 4
	AND is_distributor = 0
	AND source_database_id IS NULL
	AND LOWER([name]) NOT IN ('reportserver','reportservertemp','distribution','ssisdb')
	AND state = 0
	AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'

	OPEN DBs

	WHILE 1=1
	BEGIN
		FETCH NEXT FROM DBs INTO @CurrDB;
		IF @@FETCH_STATUS <> 0 BREAK;

		SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

		INSERT INTO #temp_Schemabinding
		exec @spExecuteSql @CMD_Template
	END

	CLOSE DBs;
	DEALLOCATE DBs;

	-- Remove functions that reference linked servers
	IF @IgnoreFunctionsDependentOnLinkedServers = 1
	BEGIN
		DELETE T
		FROM #temp_Schemabinding  AS T
		INNER JOIN sys.servers AS srv
		ON srv.server_id > 0
		AND (LOWER(T.[definition]) LIKE N'%' + LOWER(srv.name) + N'.%.%.%' 
		OR LOWER(T.[definition]) LIKE N'%_' + LOWER(srv.name) + N'_.%.%.%')
		OPTION (RECOMPILE);
	END
END

-- General summary:
SELECT 'In server: ' + @@SERVERNAME + ', database: ' + QUOTENAME([database_name]) + ', fuction: ' + [name] + ', schemabinding option is disabled', *
FROM #temp_Schemabinding
WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
OPTION(RECOMPILE)