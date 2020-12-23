/************** Find Invalid Records ***************
Author: Eitan Blumin
More info: https://eitanblumin.com/2018/11/06/find-and-fix-untrusted-foreign-keys-in-all-databases/
****************************************************/
DECLARE
	  @Constraint	SYSNAME		= 'CK_Name'
	, @PrintOnly	BIT		= 0
	, @Top		INT		= NULL
	, @CountOnly	BIT		= 0
	, @OrderBy	NVARCHAR(MAX)	= NULL -- N'ColumnName DESC'

DECLARE
	@TableID INT,
	@CheckDefinition NVARCHAR(MAX),
	@CMD NVARCHAR(MAX)

SELECT
	@TableID = parent_object_id,
	@CheckDefinition = [definition]
FROM sys.check_constraints
WHERE name = @Constraint

IF @CheckDefinition IS NULL
BEGIN
	RAISERROR(N'Check constraint %s was not found in current database.', 16, 1, @Constraint);
	GOTO Quit;
END

SET @CMD = N'SELECT '
+ CASE WHEN @CountOnly = 1 THEN N'COUNT(*)'
ELSE
ISNULL(N'TOP (' + CONVERT(nvarchar,@Top) + N')', N'') + N' ctable.*'
END + N'
-- DELETE ctable
FROM ' + QUOTENAME(OBJECT_SCHEMA_NAME(@TableID)) + '.' + QUOTENAME(OBJECT_NAME(@TableID)) + N' AS ctable
WHERE NOT 
(' + @CheckDefinition + N')'
+ CASE WHEN @CountOnly = 0 THEN ISNULL(N'
ORDER BY ' + @OrderBy, N'') ELSE N'' END

PRINT @CMD
IF @PrintOnly = 0
	EXEC (@CMD);
Quit:
