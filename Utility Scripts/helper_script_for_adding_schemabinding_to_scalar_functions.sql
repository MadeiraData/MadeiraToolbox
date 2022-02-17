/*
Author: Eitan Blumin | https://www.eitanblumin.com

Description:
Helper T-SQL script for adding SCHEMABINDING on scalar functions (checks in ALL databases on the server)
Added support for Azure SQL DB: Performs the same check across schemas instead of across databases

The script ignores the following functions:
- Self-referencing functions.
- Functions that contain code that uses sp_executesql or OLE Automation procedures.
- Functions in schemas "sys" and "tSQLt".
- Functions dependent on synonyms (can be disabled by setting @IgnoreFunctionsDependentOnSynonyms to 0)
- Functions that have constraints dependent on them (can be disabled by setting @IgnoreFunctionsWithConstraintDependencies to 0)
- Functions dependent on linked servers (can be disabled by setting @IgnoreFunctionsDependentOnLinkedServers to 0)
- Functions referencing functions fitting any of the above criteria (recursive).

Instructions:

1. Run the script to detect all scalar functions with disabled SCHEMABINDING, that can potentially have it enabled.
   Optionally set the @WithDependencies parameter to filter only on functions with/without dependencies.
2. Review the 1st resultset for the full list of detected functions.
3. Review the 2nd resultset to see how many times each function has identical definition in several databases (in case of Azure SQL DB: several schemas).
4. Use the 3rd resultset by copying and pasting the "CreateScript" column to a different query window,
	replace all "CREATE FUNCTION" commands with "ALTER FUNCTION", and add WITH SCHEMABINDING where needed.
	You may also use this opportunity to add RETURNS NULL ON NULL INPUT where possible.
5. Use the 4th resultset similarly to the 3rd as explained in the previous step.
	The difference here is that functions here exists in multiple databases (in case of Azure SQL DB: multiple schemas),
	all with an identical definition. So you'd have less work in modifying them.

*/
DECLARE
	 @WithDependencies BIT = NULL -- optionally filter only on functions with/without dependencies (1 = with, 0 = without, NULL = all)
	,@IgnoreFunctionsDependentOnSynonyms BIT = 1 -- optionally filter out functions that depend on synonyms (cannot be schemabound)
	,@IgnoreFunctionsWithConstraintDependencies BIT = 1 -- optionally filter out functions that have constraints dependant on them (cannot be altered)
	,@IgnoreFunctionsDependentOnLinkedServers BIT = 0 -- optionally filter out functions that possibly depend on linked servers (cannot be schemabound)

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

DECLARE @CMD_Template NVARCHAR(MAX);

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
)'
-- Recursive expression-based dependencies
/*
+ N'
, ExclusionTree2 AS
(
select d.referencing_id AS object_id
from sys.sql_expression_dependencies AS d
INNER JOIN Exclusions AS Tree ON d.referenced_id = Tree.object_id
WHERE d.referencing_id <> d.referenced_id
UNION ALL
select d.referencing_id
from sys.sql_expression_dependencies AS d
INNER JOIN ExclusionTree2 AS Tree ON d.referenced_id = Tree.object_id
WHERE d.referencing_id <> d.referenced_id
)'*/
+ N'
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

		RAISERROR(N'Found %d potential functions without schemabinding in database %s',0,1,@@ROWCOUNT, @CurrDB) WITH NOWAIT;
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

		RAISERROR(N'Ignoring %d functions referencing linked servers',0,1,@@ROWCOUNT) WITH NOWAIT;
	END
END

-- General summary:
SELECT 'In server: ' + @@SERVERNAME + ', database: ' + QUOTENAME([database_name]) + ', fuction: ' + [name] + ', schemabinding option is disabled', *
FROM #temp_Schemabinding
WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
AND (@IgnoreFunctionsDependentOnSynonyms = 0 OR [has_synonym_dependencies] = 0)
AND (@IgnoreFunctionsWithConstraintDependencies = 0 OR [has_constraint_dependencies] = 0)
OPTION(RECOMPILE);

IF CONVERT(varchar(4000), SERVERPROPERTY('Edition')) = 'SQL Azure'
BEGIN
	-- Check how many times each identical function exists in multiple schemas
	SELECT [object_name], NormalizedDefinition + N'
GO' AS [definition], COUNT(*) AS num_of_schemas
	FROM #temp_Schemabinding
	CROSS APPLY (VALUES(
		REPLACE([definition],QUOTENAME([schema_name]),'[{SchemaName}]')
		)) AS v(NormalizedDefinition)
	WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
	AND (@IgnoreFunctionsDependentOnSynonyms = 0 OR [has_synonym_dependencies] = 0)
	AND (@IgnoreFunctionsWithConstraintDependencies = 0 OR [has_constraint_dependencies] = 0)
	GROUP BY [object_name], NormalizedDefinition
	
	-- Generate CREATE script for all unique or "rare" functions
	SELECT [object_name], [schema_name], FullObjectName = QUOTENAME([database_name]) + N'.' + [name]
	, CreationScript = [definition] + N'
GO'
	, a.has_dependencies, a.has_constraint_dependencies, a.has_synonym_dependencies
	FROM (
	SELECT *, COUNT(*) OVER (PARTITION BY [object_name]) AS InsNum
	FROM #temp_Schemabinding
	CROSS APPLY (VALUES(
		REPLACE([definition],QUOTENAME([schema_name]),'[{SchemaName}]')
		)) AS v(NormalizedDefinition)
	) AS a
	WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
	AND (@IgnoreFunctionsDependentOnSynonyms = 0 OR [has_synonym_dependencies] = 0)
	AND (@IgnoreFunctionsWithConstraintDependencies = 0 OR [has_constraint_dependencies] = 0)
	AND (InsNum < 5
	OR [object_name] IN (SELECT [object_name] FROM #temp_Schemabinding 
						CROSS APPLY (VALUES(
							REPLACE([definition],QUOTENAME([schema_name]),'[{SchemaName}]')
						)) AS v(NormalizedDefinition) GROUP BY [object_name] 
						HAVING COUNT(DISTINCT NormalizedDefinition) > 1)
		)
	ORDER BY 1, 2, 3
	
	-- Generate cursor command to CREATE function in all schemas that contain it
	SELECT [object_name], COUNT(*) AS num_of_schemas
	, CreationLoopScript = N'DECLARE @Template NVARCHAR(MAX), @CMD NVARCHAR(MAX), @CurrSchema SYSNAME;
PRINT N''' + QUOTENAME([object_name]) + N'''
SET @Template = N''' + REPLACE(NormalizedDefinition,N'', N'''''') + N'''
DECLARE CurSch CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.schemas
WHERE schema_id NOT BETWEEN 16384 AND 16393
AND name NOT IN (''guest'',''sys'',''INFORMATION_SCHEMA'')

OPEN CurSch
FETCH NEXT FROM CurSch INTO @CurrSchema

WHILE @@FETCH_STATUS = 0
BEGIN
	IF OBJECT_ID(QUOTENAME(@CurrSchema) + ''.' + QUOTENAME([object_name]) + N''') IS NOT NULL
	BEGIN
		PRINT QUOTENAME(@CurrSchema) + N''...''
		SET @CMD = REPLACE(@Template, ''[{SchemaName}]'', QUOTENAME(@CurrSchema))
		EXEC (@CMD)
	END
	
	FETCH NEXT FROM CurSch INTO @CurrSchema
END
CLOSE CurSch
DEALLOCATE Sch
GO'
	FROM #temp_Schemabinding
	CROSS APPLY (VALUES(
		REPLACE([definition],QUOTENAME([schema_name]),'[{SchemaName}]')
		)) AS v(NormalizedDefinition)
	WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
	AND (@IgnoreFunctionsDependentOnSynonyms = 0 OR [has_synonym_dependencies] = 0)
	AND (@IgnoreFunctionsWithConstraintDependencies = 0 OR [has_constraint_dependencies] = 0)
	GROUP BY [object_name], NormalizedDefinition
	HAVING COUNT(*) >= 3
	AND [object_name] NOT IN (SELECT [object_name] FROM #temp_Schemabinding 
						CROSS APPLY (VALUES(
							REPLACE([definition],QUOTENAME([schema_name]),'[{SchemaName}]')
						)) AS v(NormalizedDefinition) GROUP BY [object_name] 
						HAVING COUNT(DISTINCT NormalizedDefinition) > 1)
	ORDER BY 1
END
ELSE
BEGIN
	-- Check how many times each identical function exists in multiple databases
	SELECT [schema_name], [object_name], [definition], COUNT(*) AS num_of_databases
	FROM #temp_Schemabinding
	WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
	AND (@IgnoreFunctionsDependentOnSynonyms = 0 OR [has_synonym_dependencies] = 0)
	AND (@IgnoreFunctionsWithConstraintDependencies = 0 OR [has_constraint_dependencies] = 0)
	GROUP BY [schema_name], [object_name], [definition]
	
	-- Generate CREATE script for all unique or "rare" functions
	SELECT [database_name], [object_name], [schema_name], FullObjectName = QUOTENAME([database_name]) + N'.' + [name]
	, CreationScript = N'USE ' + QUOTENAME([database_name]) + N';
GO
' + [definition] + N'
GO'
	, a.has_dependencies, a.has_constraint_dependencies, a.has_synonym_dependencies
	FROM (
	SELECT *, COUNT(*) OVER (PARTITION BY [name]) AS InsNum
	FROM #temp_Schemabinding
	) AS a
	WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
	AND (@IgnoreFunctionsDependentOnSynonyms = 0 OR [has_synonym_dependencies] = 0)
	AND (@IgnoreFunctionsWithConstraintDependencies = 0 OR [has_constraint_dependencies] = 0)
	AND (InsNum < 5
	OR [name] IN (SELECT [name] FROM #temp_Schemabinding GROUP BY [name] HAVING COUNT(DISTINCT [definition]) > 1)
	)
	ORDER BY 1, 2, 3

	-- Generate sp_MSforeachdb command to CREATE function in all databases that contain it
	SELECT [name], COUNT(*) AS num_of_databases
	, CreationLoopScript = N'EXEC sp_MSforeachdb N''USE [?];
IF OBJECT_ID(''''' + [name] + N''''') IS NOT NULL
EXEC(N''''' + REPLACE([definition],N'''', N'''''''''') + N''''');''
GO'
	FROM #temp_Schemabinding
	WHERE (@WithDependencies IS NULL OR @WithDependencies = has_dependencies)
	AND (@IgnoreFunctionsDependentOnSynonyms = 0 OR [has_synonym_dependencies] = 0)
	AND (@IgnoreFunctionsWithConstraintDependencies = 0 OR [has_constraint_dependencies] = 0)
	GROUP BY [name], [definition]
	HAVING COUNT(*) >= 5
	AND [name] NOT IN (SELECT [name] FROM #temp_Schemabinding GROUP BY [name] HAVING COUNT(DISTINCT [definition]) > 1)
	ORDER BY 1

END