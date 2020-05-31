/*
Re-Number Identity Column
=================================
Author: Eitan Blumin | https://www.eitanblumin.com
Create Date: 2020-03-24
Description:
	Use this script to re-number a table with an identity column, which has very large number gaps.
	The specified parameter @ChunkSize must be smaller than the current minimum value
	in the table.
*/
/*
-- POC Experiment set-up
USE [tempdb];
GO
DROP TABLE IF EXISTS MyTableWithIdentity;
CREATE TABLE MyTableWithIdentity
(
	autoID BIGINT NOT NULL IDENTITY(9238,1) PRIMARY KEY CLUSTERED,
	charCol VARCHAR(255) DEFAULT(NEWID())
);
INSERT INTO MyTableWithIdentity(charCol)
SELECT TOP 1000000 NEWID()
FROM sys.all_columns AS a CROSS JOIN sys.all_columns b;

DELETE  T
FROM (
SELECT TOP (832720) *
FROM MyTableWithIdentity
ORDER BY charCol
) AS T;

SELECT * FROM MyTableWithIdentity
*/
GO
DECLARE
	  @TableName		SYSNAME	= 'MyTableWithIdentity'	-- Specify the name of the table to re-number
	, @ChunkSize		INT	= 1000			-- Must be smaller than the current minimum value in the data that's to be re-numbered
	, @ReNumberOffset	FLOAT	= 0			-- Re-numbering will begin from this offset parameter + 1
	, @StartExistingOffset	FLOAT	= 0			-- Specify a value here if you're resuming a previously stopped re-numbering, 
								-- or you just want to start re-numbering from the middle of the table
	, @ReAlignIdentOnly	BIT	= 0			-- Set this to 1 if you want to skip re-numbering, and only re-align the 
								-- identity seed to current max value

SET NOCOUNT, XACT_ABORT, ARITHABORT ON;

DECLARE @CurrTimeString VARCHAR(30), @RCount INT, @CMD NVARCHAR(MAX);
DECLARE @DestinationColumnsList NVARCHAR(MAX), @SourceColumnsList NVARCHAR(MAX);
DECLARE @DestinationBufferColumns NVARCHAR(MAX), @SourceBufferColumns NVARCHAR(MAX);
DECLARE @IdentColumn SYSNAME, @IdentDataType SYSNAME, @Msg NVARCHAR(MAX);

-- Validations
IF ISNULL(@ChunkSize,0) <= 0
BEGIN
	RAISERROR(N'@ChunkSize must be a positive integer.',16,1);
	GOTO Quit;
END
IF ISNULL(@TableName,N'') = N''
BEGIN
	RAISERROR(N'@TableName must be specified.',16,1);
	GOTO Quit;
END
IF ISNULL(OBJECTPROPERTY(OBJECT_ID(@TableName,'U'), 'TableHasIdentity'),0) = 0
BEGIN
	RAISERROR(N'"%s" is not a valid table with an IDENTITY column.',16,1,@TableName);
	GOTO Quit;
END

DECLARE @PreviousNewIdent FLOAT;
SET @PreviousNewIdent = ISNULL(@ReNumberOffset,0);
SET @TableName = QUOTENAME(OBJECT_SCHEMA_NAME(OBJECT_ID(@TableName))) + '.' + QUOTENAME(OBJECT_NAME(OBJECT_ID(@TableName)))

-- duplicate table definition
DECLARE @ShowFields TABLE
(
FieldID INT IDENTITY(1,1),
DatabaseName SYSNAME,
TableOwner SYSNAME,
TableName SYSNAME,
FieldName SYSNAME,
ColumnPosition INT,
IsNullable BIT,
DataType SYSNAME,
MaxLength INT,
NumericPrecision INT,
NumericScale INT,
DomainName SYSNAME NULL,
FieldListingName NVARCHAR(300),
FieldDefinition NVARCHAR(4000),
IdentityColumn BIT,
IdentitySeed INT,
IdentityIncrement INT,
IsCharColumn BIT,
UNIQUE CLUSTERED (ColumnPosition ASC)
);
INSERT INTO @ShowFields
(
DatabaseName,
TableOwner,
TableName,
FieldName,
ColumnPosition,
IsNullable,
DataType,
MaxLength,
NumericPrecision,
NumericScale,
DomainName,
FieldListingName,
FieldDefinition,
IdentityColumn,
IdentitySeed,
IdentityIncrement,
IsCharColumn
) 
SELECT
DB_NAME(),
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
CAST(ORDINAL_POSITION AS INT),
CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
DATA_TYPE,
CAST(CHARACTER_MAXIMUM_LENGTH AS INT),
CAST(NUMERIC_PRECISION AS INT),
CAST(NUMERIC_SCALE AS INT),
DOMAIN_NAME,
QUOTENAME(COLUMN_NAME) + ',',
comp.definition + CASE WHEN comp.is_persisted = 1 THEN ' PERSISTED' ELSE '' END AS FieldDefinition,
CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS IdentityColumn,
CAST(ISNULL(ic.seed_value,0) AS INT) AS IdentitySeed,
CAST(ISNULL(ic.increment_value,0) AS INT) AS IdentityIncrement,
CASE WHEN st.collation_name IS NOT NULL THEN 1 ELSE 0 END AS IsCharColumn
FROM
INFORMATION_SCHEMA.COLUMNS c
JOIN sys.columns sc ON c.TABLE_NAME = OBJECT_NAME(sc.object_id) AND c.COLUMN_NAME = sc.Name
LEFT JOIN sys.identity_columns ic ON c.TABLE_NAME = OBJECT_NAME(ic.object_id) AND c.COLUMN_NAME = ic.Name
JOIN sys.types st ON COALESCE(c.DOMAIN_NAME,c.DATA_TYPE) = st.name
LEFT OUTER JOIN sys.objects dobj ON dobj.object_id = sc.default_object_id AND dobj.type = 'D'
LEFT OUTER JOIN [sys].[computed_columns] comp ON comp.object_id = sc.object_id AND sc.column_id = comp.column_id
WHERE sc.object_id = OBJECT_ID(@TableName)
AND (comp.definition IS NULL) -- Do not include computed columns
ORDER BY
c.TABLE_NAME, c.ORDINAL_POSITION;

SELECT 
	@IdentColumn = QUOTENAME(FieldName),
	@IdentDataType = 
CASE WHEN DomainName IS NOT NULL THEN QUOTENAME(DomainName) + CASE WHEN IsNullable = 1 THEN ' NULL ' ELSE ' NOT NULL ' END
ELSE QUOTENAME(UPPER(DataType)) +
CASE WHEN IsCharColumn = 1 THEN '(' + ISNULL(NULLIF(CAST(MaxLength AS VARCHAR(10)),'-1'),'MAX') + ')' ELSE '' END +
CASE WHEN IsNullable = 1 THEN ' NULL ' ELSE ' NOT NULL ' END
END
FROM @ShowFields
WHERE IdentityColumn = 1;

IF @ReAlignIdentOnly = 1
	GOTO ReAlignIdent;

SELECT
	@DestinationColumnsList = ISNULL(@DestinationColumnsList + N', ', N'') + QUOTENAME(FieldName),
	@SourceBufferColumns = ISNULL(@SourceBufferColumns + N', ', N'') + 
		CASE
			WHEN IdentityColumn = 0 THEN 'Src.' + QUOTENAME(FieldName)
			ELSE N'Buff.NewIdent AS ' + QUOTENAME(FieldName)
		END
FROM @ShowFields
;

DECLARE @PreviousOldIdent FLOAT;
SET @CMD = N'SELECT @PreviousOldIdent = MIN('+ @IdentColumn + N') - 1 FROM '+ @TableName + N' WHERE '+ @IdentColumn + N' > @StartExistingOffset;';
EXEC sp_executesql @CMD, N'@PreviousOldIdent FLOAT OUTPUT, @StartExistingOffset FLOAT', @PreviousOldIdent OUTPUT, @StartExistingOffset;

select @CMD

select @PreviousOldIdent as prev;
IF @PreviousOldIdent + 1 BETWEEN @PreviousNewIdent + 1 AND @PreviousNewIdent + @ChunkSize
BEGIN
	RAISERROR(N'The minimum value in the table is smaller than @ChunkSize! Please increase @PreviousNewIdent or decrease @ChunkSize.',16,1);
	GOTO Quit;
END

SET @CMD = N'IF OBJECT_ID(''tempdb..#BufferTable'') IS NOT NULL DROP TABLE #BufferTable;
CREATE TABLE #BufferTable
(NewIdent ' + @IdentDataType + N', OldIdent ' + @IdentDataType + N'
, UNIQUE CLUSTERED (OldIdent));

SET XACT_ABORT, ARITHABORT ON;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

INSERT INTO #BufferTable
(NewIdent, OldIdent)
SELECT TOP (@ChunkSize)
  @PreviousNewIdent + ROW_NUMBER() OVER (ORDER BY ' + @IdentColumn + N' ASC)
, ' + @IdentColumn + N'
FROM ' + @TableName + N' WITH(ROWLOCK, HOLDLOCK)
WHERE ' + @IdentColumn + ' > @PreviousOldIdent
ORDER BY ' + @IdentColumn + ' ASC;

SET @Rcount = @@ROWCOUNT;

IF @Rcount = 0
	RETURN;
	
SELECT
	@PreviousNewIdent = MAX(NewIdent),
	@PreviousOldIdent = MAX(OldIdent)
FROM #BufferTable;

BEGIN TRAN

	SELECT
	 ' + @SourceBufferColumns + N'
	INTO #temp
	FROM ' + @TableName + N' AS Src
	INNER JOIN #BufferTable AS Buff
	ON Src.' + @IdentColumn + N' = Buff.OldIdent;

	DELETE ' + @TableName + N'
	WHERE ' + @IdentColumn + N' IN
	(SELECT OldIdent FROM #BufferTable);

	SET IDENTITY_INSERT ' + @TableName + N' ON;

	INSERT INTO ' + @TableName + N'
	(' + @DestinationColumnsList + N')
	SELECT
	 ' + @DestinationColumnsList + N'
	FROM #temp;

	SET IDENTITY_INSERT ' + @TableName + N' OFF;

COMMIT TRAN;'

SELECT @CMD
--GOTO Quit;
-- update by chunks
WHILE 1=1
BEGIN
	SET @CurrTimeString = CONVERT(varchar(23), GETDATE(), 121)
	SET @Msg = CONCAT(@CurrTimeString, N' - Processing ID range starting after value ', @PreviousOldIdent)
	RAISERROR(N'%s',0,1,@Msg) WITH NOWAIT;

	SET @RCount = 0;
	EXEC sp_executesql @CMD
		, N'@RCount INT OUTPUT, @ChunkSize INT, @PreviousNewIdent FLOAT OUTPUT, @PreviousOldIdent FLOAT OUTPUT'
		, @RCount OUTPUT, @ChunkSize, @PreviousNewIdent OUTPUT, @PreviousOldIdent OUTPUT
	
	IF @RCount = 0
		BREAK;
END

SET @CurrTimeString = CONVERT(varchar(23), GETDATE(), 121)
RAISERROR(N'%s - Done.',0,1,@CurrTimeString);

-- Re-Aligning IDENTITY seed:
ReAlignIdent:

DECLARE @LastValue BIGINT, @CurrMaxValue BIGINT, @Result NVARCHAR(MAX)

SELECT @LastValue = CONVERT(bigint, c.last_value)
FROM sys.identity_columns AS c
INNER JOIN sys.tables AS t
ON c.object_id = t.object_id
WHERE c.object_id = OBJECT_ID(@TableName,'U')

SET @CMD = N'
SELECT
	@CurrMaxValue = MAX(' + @IdentColumn + N'),
	@Result = N''DBCC CHECKIDENT(''''' + @TableName + N''''', RESEED, '' + CONVERT(nvarchar(max), MAX(' + @IdentColumn + N')) + N'') -- previously: ' + CONVERT(nvarchar(max), @LastValue) + N'''
FROM ' + @TableName + N'
HAVING MAX(' + @IdentColumn + N') <> @LastValue'

EXEC sp_executesql @CMD, N'@Result NVARCHAR(MAX) OUTPUT, @CurrMaxValue BIGINT OUTPUT, @LastValue BIGINT', @Result OUTPUT, @CurrMaxValue OUTPUT, @LastValue;
IF @Result IS NOT NULL
BEGIN
	PRINT @Result
	SET @CurrTimeString = CONVERT(varchar(23), GETDATE(), 121)
	RAISERROR(N'%s - Re-Aligning Identity to actual max value...',0,1,@CurrTimeString);
	EXEC(@Result);
END
ELSE
	PRINT CONCAT(N'No need to realign. Identity Last Value: ', @LastValue)
Quit: