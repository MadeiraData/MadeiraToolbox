SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @SqlStatement VARCHAR(max) = ''

SELECT @SqlStatement += '
	SELECT DatabaseName = ' + QUOTENAME([name], N'''') + '
	,SchemaName = OBJECT_SCHEMA_NAME(object_id)
	,TableName = OBJECT_NAME(object_id)
	,NumberOfRows = (
		SELECT SUM(rows)
		FROM ' + QUOTENAME([name]) + '.sys.partitions AS p
		WHERE p.object_id = i.object_id AND p.index_id = i.index_id
		)
		,SampleDate = GETDATE()
FROM ' + QUOTENAME([name]) + '.sys.indexes AS i
WHERE index_id = 0' + CHAR(10)
FROM sys.databases
WHERE [name] NOT IN ('master', 'model', 'msdb', 'tempdb')
AND is_distributor = 0
AND state = 0
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'

--PRINT (@SqlStatement)
EXEC (@SqlStatement)
GO


