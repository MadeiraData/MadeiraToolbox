/************** Find Invalid Records ***************
Author: Eitan Blumin
****************************************************/
DECLARE
	@Constraint SYSNAME = 'CK_Name'
	, @PrintOnly BIT	= 0

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

SET @CMD = N'SELECT *
FROM ' + QUOTENAME(OBJECT_SCHEMA_NAME(@TableID)) + '.' + QUOTENAME(OBJECT_NAME(@TableID)) + N'
WHERE NOT 
(' + @CheckDefinition + N')'

PRINT @CMD
IF @PrintOnly = 0
	EXEC (@CMD);
Quit:
