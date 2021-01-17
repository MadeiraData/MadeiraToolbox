/************** Find Orphaned Records **************
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
More info: https://eitanblumin.com/2018/11/06/find-and-fix-untrusted-foreign-keys-in-all-databases/
****************************************************/
DECLARE
	  @ForeignKeyName	SYSNAME		= 'FK_MyTable_MyOtherTable'
	, @CountOnly		BIT		= 0
	, @PrintOnly		BIT		= 0
	, @Top			INT		= NULL
	, @OrderBy		NVARCHAR(MAX)	= NULL -- N'ColumnName DESC'

DECLARE
	@FKId INT,
	@ChildTableID INT,
	@ParentTableID INT,
	@CMD NVARCHAR(MAX),
	@ColumnsSetNull NVARCHAR(MAX),
	@ColumnNullabilityCheck NVARCHAR(MAX) = N''

SELECT
	@FKId = object_id,
	@ChildTableID = parent_object_id,
	@ParentTableID = referenced_object_id
FROM sys.foreign_keys
WHERE name = @ForeignKeyName

IF @FKId IS NULL
BEGIN
	RAISERROR(N'Foreign Key %s was not found in current database!', 16, 1, @ForeignKeyName);
	GOTO Quit;
END

SELECT
	@CMD = ISNULL(@CMD + CHAR(13) + CHAR(10) + N'AND ', N'') + N'ctable.' + QUOTENAME(cc.name) + N' = ptable.' + QUOTENAME(pc.name)
	, @ColumnNullabilityCheck = @ColumnNullabilityCheck + CHAR(13) + CHAR(10) + N'AND ctable.' + QUOTENAME(cc.name) + N' IS NOT NULL'
	, @ColumnsSetNull = ISNULL(@ColumnsSetNull + N', ', N'') + QUOTENAME(cc.name) + N' = NULL'
	--ChildTable = QUOTENAME(OBJECT_SCHEMA_NAME(fkc.parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(fkc.parent_object_id))
	--, ChildColumn = QUOTENAME(cc.name)
	--, ParentTable = QUOTENAME(OBJECT_SCHEMA_NAME(fkc.referenced_object_id)) + '.' + QUOTENAME(OBJECT_NAME(fkc.referenced_object_id))
	--, ParentColumn = QUOTENAME(pc.name)
FROM sys.foreign_key_columns AS fkc
INNER JOIN sys.columns AS cc
ON fkc.parent_object_id = cc.object_id
AND fkc.parent_column_id = cc.column_id
INNER JOIN sys.columns AS pc
ON fkc.referenced_object_id = pc.object_id
AND fkc.referenced_column_id = pc.column_id
WHERE fkc.constraint_object_id = @FKId

SET @CMD = N'SELECT '
+ CASE WHEN @CountOnly = 1 THEN N'COUNT(*)'
ELSE
ISNULL(N'TOP (' + CONVERT(nvarchar,@Top) + N')', N'') + N' ctable.*'
END + N'

-- DELETE ' + ISNULL(N'TOP (' + CONVERT(nvarchar,@Top) + N')', N'') + N' ctable

-- UPDATE ' + ISNULL(N'TOP (' + CONVERT(nvarchar,@Top) + N')', N'') + N' ctable SET ' + @ColumnsSetNull + N'

FROM ' + QUOTENAME(OBJECT_SCHEMA_NAME(@ChildTableID)) + '.' + QUOTENAME(OBJECT_NAME(@ChildTableID)) + N' AS ctable
WHERE NOT EXISTS
(SELECT NULL FROM ' + QUOTENAME(OBJECT_SCHEMA_NAME(@ParentTableID)) + '.' + QUOTENAME(OBJECT_NAME(@ParentTableID)) + N' AS ptable
WHERE ' + @CMD + N')'
+ ISNULL(@ColumnNullabilityCheck, N'')
+ CASE WHEN @CountOnly = 0 THEN ISNULL(N'
ORDER BY ' + @OrderBy, N'') ELSE N'' END

PRINT @CMD
IF @PrintOnly = 0
	EXEC (@CMD);
Quit:
