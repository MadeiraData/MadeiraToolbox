
/**************************************************************************************************/
/*					Generate Procedure Unit Test with Automatic Comparison						  */
/**************************************************************************************************/
--	Author: Eitan Blumin
--	Date:	2018-11-21
--	Description: Use this script to generate and run a "unit test" for two stored procedures.
--			Each procedure is considered to be affecting one or more database tables.
--			The contents of these tables can be compared before and after each unit test,
--			and the results of each of the two stored procedures can be compared.
--			This script is good as a "sanity check" of sorts, that makes sure the operational effect
--			of one procedure (e.g. a new or updated procedure), is identical to that of another
--			procedure (e.g. an older procedure).
--			The script also prints out the duration of each procedure in milliseconds.
/**************************************************************************************************/

DECLARE 
	 @ProcNameA						SYSNAME			= N'dbo.MyProcedure_Old'
	,@ProcNameB						SYSNAME			= N'dbo.MyProcedure_New'
	,@CustomInitializationScript	NVARCHAR(MAX)	= N'
DECLARE @SiteId INT = 8, @MailId INT = 42293, @ServiceId INT, @SendDate DATETIME = GETDATE()
SELECT @ServiceId = ServiceId FROM t_settings with (nolock) WHERE SiteId=@SiteId'
	,@ParameterSetForProc			NVARCHAR(MAX)	= N' @MailId		= @MailId
,@ServiceId		= @ServiceId
,@SendDate		= @SendDate
,@ServerId		= 20'

--/*
-- You can use this query to find which tables are affected by your procedures. Identity columns will automatically be excluded:

select DISTINCT TableName = QUOTENAME(OBJECT_SCHEMA_NAME(referenced_major_id)) + '.' + QUOTENAME(OBJECT_NAME(referenced_major_id))
, ExcludeColumns = (
	SELECT [Column] = c.[name]
	FROM sys.columns AS c
	WHERE c.[object_id] = d.referenced_major_id
	AND c.is_identity = 1
	FOR XML PATH('ExcludeColumns'), ELEMENTS
)
from sys.sql_dependencies AS d
where [object_id] IN (OBJECT_ID(@ProcNameA), OBJECT_ID(@ProcNameB))
AND OBJECTPROPERTY(referenced_major_id, 'IsUserTable') = 1
AND d.is_updated = 1
--*/

DECLARE @QATables AS TABLE (TableName SYSNAME, Appendage NVARCHAR(MAX), ExcludeColumns XML)
INSERT INTO @QATables
VALUES
 ('t_contacts', N' where SiteId = @SiteId',NULL)
,('t_mails', N' where mailID = @mailId',NULL)
,('t_queue', N' where mailID = @mailId','<ExcludeColumns><Column>Id</Column><Column>DateAdded</Column><Column>SendDate</Column></ExcludeColumns>')


/**************************************************************************************************/
/*				!!!!		DONT CHANGE ANYTHING BELOW THIS LINE		!!!!					  */
/**************************************************************************************************/




SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ARITHABORT ON;

DECLARE @CMD NVARCHAR(MAX)

SET @CMD = N'
DECLARE @DataStates AS TABLE (ProcName SYSNAME, TablePhase NVARCHAR(200), DataState XML);
DECLARE @ExecutionStartTime DATETIME;

' + @CustomInitializationScript

SELECT @CMD = @CMD + N'
INSERT INTO @DataStates SELECT ProcName = @ProcNameA, TablePhase = ''BEFORE: ' + TableName + N''', (SELECT '
+ CASE WHEN ExcludeColumns IS NULL THEN N'*'
		ELSE STUFF((
	SELECT ',' + QUOTENAME(name)
	FROM
	(SELECT name
	FROM sys.columns
	WHERE object_id = OBJECT_ID(T.TableName)
	EXCEPT
	SELECT X.value('(text())[1]','sysname') AS col
	FROM T.ExcludeColumns.nodes('ExcludeColumns/Column') AS T(X)
	) AS q
	FOR XML PATH('')
	),1,1,'')
	END + N' FROM ' + TableName + Appendage + N' FOR XML PATH(''row''), ROOT(''data''), ELEMENTS)'
FROM @QATables AS T

SET @CMD = @CMD + N'
BEGIN TRANSACTION

SET @ExecutionStartTime = GETDATE()

PRINT CONCAT(N''--'',@ProcNameA,N'':'')

EXEC @ProcNameA
' + @ParameterSetForProc + N'

PRINT CONCAT(N''--Execution time: '',DATEDIFF(ms, @ExecutionStartTime, GETDATE()), N'' ms'');
'

SELECT @CMD = @CMD + N'
INSERT INTO @DataStates SELECT ProcName = @ProcNameA, TablePhase = ''AFTER: ' + TableName + N''', (SELECT '
+ CASE WHEN ExcludeColumns IS NULL THEN N'*'
		ELSE STUFF((
	SELECT ',' + QUOTENAME(name)
	FROM
	(SELECT name
	FROM sys.columns
	WHERE object_id = OBJECT_ID(T.TableName)
	EXCEPT
	SELECT X.value('(text())[1]','sysname') AS col
	FROM T.ExcludeColumns.nodes('ExcludeColumns/Column') AS T(X)
	) AS q
	FOR XML PATH('')
	),1,1,'')
	END + N' FROM ' + TableName + Appendage + N' FOR XML PATH(''row''), ROOT(''data''), ELEMENTS)'
FROM @QATables AS T

SET @CMD = @CMD + N'
ROLLBACK TRANSACTION

'

SELECT @CMD = @CMD + N'
INSERT INTO @DataStates SELECT ProcName = @ProcNameB, TablePhase = ''BEFORE: ' + TableName + N''', (SELECT '
+ CASE WHEN ExcludeColumns IS NULL THEN N'*'
		ELSE STUFF((
	SELECT ',' + QUOTENAME(name)
	FROM
	(SELECT name
	FROM sys.columns
	WHERE object_id = OBJECT_ID(T.TableName)
	EXCEPT
	SELECT X.value('(text())[1]','sysname') AS col
	FROM T.ExcludeColumns.nodes('ExcludeColumns/Column') AS T(X)
	) AS q
	FOR XML PATH('')
	),1,1,'')
	END + N' FROM ' + TableName + Appendage + N' FOR XML PATH(''row''), ROOT(''data''), ELEMENTS)'
FROM @QATables AS T

SET @CMD = @CMD + N'
BEGIN TRANSACTION

SET @ExecutionStartTime = GETDATE()

PRINT CONCAT(N''--'',@ProcNameB,N'':'')

EXEC @ProcNameB
' + @ParameterSetForProc + N'

PRINT CONCAT(N''--Execution time: '',DATEDIFF(ms, @ExecutionStartTime, GETDATE()), N'' ms'');
'

SELECT @CMD = @CMD + N'
INSERT INTO @DataStates SELECT ProcName = @ProcNameB, TablePhase = ''AFTER: ' + TableName + N''', (SELECT '
+ CASE WHEN ExcludeColumns IS NULL THEN N'*'
		ELSE STUFF((
	SELECT ',' + QUOTENAME(name)
	FROM
	(SELECT name
	FROM sys.columns
	WHERE object_id = OBJECT_ID(T.TableName)
	EXCEPT
	SELECT X.value('(text())[1]','sysname') AS col
	FROM T.ExcludeColumns.nodes('ExcludeColumns/Column') AS T(X)
	) AS q
	FOR XML PATH('')
	),1,1,'')
	END + N' FROM ' + TableName + Appendage + N' FOR XML PATH(''row''), ROOT(''data''), ELEMENTS)'
FROM @QATables AS T

SET @CMD = @CMD + N'
ROLLBACK TRANSACTION

SELECT A.TablePhase, A.DataState AS DataStateProcA, B.DataState AS DataStateProcB,
DifferenceFound = CASE WHEN EXISTS (
			(SELECT CONVERT(nvarchar(max), T.x.query(''.'')) FROM A.DataState.nodes(''data/row'') AS T(x) EXCEPT SELECT CONVERT(nvarchar(max), T.x.query(''.'')) FROM B.DataState.nodes(''data/row'') AS T(x))
			UNION ALL
			(SELECT CONVERT(nvarchar(max), T.x.query(''.'')) FROM B.DataState.nodes(''data/row'') AS T(x) EXCEPT SELECT CONVERT(nvarchar(max), T.x.query(''.'')) FROM A.DataState.nodes(''data/row'') AS T(x))
			) THEN 1 ELSE 0 END
FROM @DataStates AS A
INNER JOIN @DataStates AS B
ON A.TablePhase = B.TablePhase
WHERE
	A.ProcName = @ProcNameA
AND B.ProcName = @ProcNameB'

PRINT @CMD
EXEC sp_executesql @CMD, N'@ProcNameA SYSNAME, @ProcNameB SYSNAME', @ProcNameA, @ProcNameB;
GO
