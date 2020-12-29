/*
---------------------------------------------------------------------------
sp_GenerateTableDDLScript version 1.0 by Eitan Blumin
https://github.com/EitanBlumin/sp_GenerateTableDDLScript
---------------------------------------------------------------------------
Copyright 2019 by Eitan Blumin at https://www.eitanblumin.com all rights reserved
---------------------------------------------------------------------------
Purpose:
--------------------------------------
	This procedure can be used to generate a CREATE TABLE script for a given table.
	You may create this procedure in the master database, and use the following
	command to turn it into a system stored procedure, usable anywhere in the instance:
	
	EXECUTE sp_MS_marksystemobject 'sp_GenerateTableDDLScript'
---------------------------------------------------------------------------
License:
--------------------------------------
@TableName SYSNAME,			-- The name of the source table. This parameter is mandatory.
					-- If the table's schema is not default (dbo), then please specify the 
					-- schema name as well as part of the parameter.
@NewTableName SYSNAME = NULL,		-- The name of the new (target) table. You may also include database and schema as part of the name.
					-- If not specified, same name as source table will be used.
@Result NVARCHAR(MAX) OUTPUT,		-- Output textual parameter that will contain the result TSQL command for creating the table.
@IncludeDefaults BIT = 1,		-- Set whether to include default constraints
@IncludeCheckConstraints BIT = 1,	-- Set whether to include check constraints
@IncludeForeignKeys BIT = 1,		-- Set whether to include foreign key constraints
@IncludeIndexes BIT = 1,		-- Set whether to include indexes
@IncludePrimaryKey BIT = 1,		-- Set whether to include primary key constraints
@IncludeIdentity BIT = 1,		-- Set whether to include identity property
@IncludeUniqueIndexes BIT = 1,		-- Set whether to include unique index constraints
@IncludeComputedColumns BIT = 1,	-- Set whether to include computed columns (if not, they will also be automatically ignored by constraints and indexes)
@UseSystemDataTypes BIT = 0,		-- Set whether to use system data type names instead of user data type names
@ConstraintsNameAppend SYSNAME = '',	-- This is an optional text string to append to constraint names, 
					-- in order to avoid the duplicate object name exception.
					-- This is useful when creating the new table within the same database.
@Verbose BIT = 0			-- Optional parameter. If set to 1, will display informative messages, and will output a table representing the table fields
---------------------------------------------------------------------------
Example Usages:
--------------------------------------

	-- Example use case 1: Creating a table in an archive database, without foreign keys and identity property:

	DECLARE @CMD NVARCHAR(MAX)
	EXEC sp_GenerateTableDDLScript 'Sales.OrderDetails', 'ArchiveDB.Sales.OrderDetails', @CMD OUTPUT, @IncludeForeignKeys = 0, @IncludeIdentity = 0
	SELECT @CMD

	-- Example use case 2: Duplicating a table within the same database;

	DECLARE @CMD NVARCHAR(MAX)
	EXEC sp_GenerateTableDDLScript 'Sales.OrderDetails', 'Sales.OrderDetails_New', @CMD OUTPUT, @ConstraintsNameAppend = '_New'
	SELECT @CMD

	-- Example use case 3: Duplicating a table as a temporary table, without computed columns:

	DECLARE @CMD NVARCHAR(MAX)
	EXEC sp_GenerateTableDDLScript 'Sales.OrderDetails', '#temp_OrderDetails', @CMD OUTPUT, @ConstraintsNameAppend = '_Temp', @IncludeComputedColumns = 0
	SELECT @CMD
---------------------------------------------------------------------------
Remarks:
--------------------------------------
- The source table must exist, otherwise an exception will be raised.
- The script does not check whether the target table already exists,
  it falls on you to make sure that it doesn't before running the result script.
- The script does not check whether constraint names already exist,
  it falls on you to use the @ConstraintsNameAppend parameter to generate unique names.
- The script (at the moment) does NOT support the following:
	- Column Sets
	- Collations different from Database Default
	- Filestream columns
	- Sparse columns
	- Not for replication property
	- XML document collections
	- Rule objects
	- Non-default Filegroups
	- In-Memory tables
---------------------------------------------------------------------------
Acknowledgements:
--------------------------------------
	The script is mainly based off of the sp_ScriptTable stored procedure
	originally published by Tim Chapman in this URL:
	https://www.techrepublic.com/blog/the-enterprise-cloud/script-table-definitions-using-tsql/
---------------------------------------------------------------------------
Version History:
--------------------------------------
2019-04-17: First publication
2019-07-09: Fixed bug returning -1 character length for MAX length columns
2019-07-09: Added optional @Verbose parameter
---------------------------------------------------------------------------
*/
CREATE PROCEDURE [dbo].[sp_GenerateTableDDLScript]
(
@TableName NVARCHAR(500),
@NewTableName SYSNAME = NULL,
@Result NVARCHAR(MAX) OUTPUT,
@IncludeDefaults BIT = 1,
@IncludeCheckConstraints BIT = 1,
@IncludeForeignKeys BIT = 1,
@IncludeIndexes BIT = 1,
@IncludePrimaryKey BIT = 1,
@IncludeIdentity BIT = 1,
@IncludeUniqueIndexes BIT = 1,
@IncludeComputedColumns BIT = 1,
@UseSystemDataTypes BIT = 0,
@ConstraintsNameAppend SYSNAME = '',
@Verbose BIT = 0
)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @MainDefinition TABLE
(
FieldValue NVARCHAR(4000)
)

DECLARE @TableObjId INT
DECLARE @ClusteredPK BIT
DECLARE @TableSchema NVARCHAR(255)
DECLARE @RCount INT

SELECT @TableName = name, @TableObjId = id, @TableSchema = OBJECT_SCHEMA_NAME(id) FROM sysobjects WHERE id = OBJECT_ID(@TableName);

IF OBJECT_ID(@TableName) IS NULL OR @TableObjId IS NULL
BEGIN
	RAISERROR(N'Table %s not found within current database!', 16, 1, @TableName);
	RETURN -1;
END

SET @NewTableName = ISNULL(@NewTableName, QUOTENAME(DB_NAME(DB_ID())) + '.' + QUOTENAME(@TableSchema) + '.' + @TableName);

DECLARE @ShowFields TABLE
(
FieldID INT IDENTITY(1,1),
DatabaseName SYSNAME,
TableOwner SYSNAME,
TableName SYSNAME,
FieldName SYSNAME,
ColumnPosition INT,
ColumnDefaultValue NVARCHAR(1000),
ColumnDefaultName SYSNAME NULL,
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
IsCharColumn BIT
) 

DECLARE @HoldingArea TABLE
(
FldID SMALLINT IDENTITY(1,1),
Flds VARCHAR(4000),
FldValue CHAR(1) DEFAULT(0)
)

DECLARE @PKObjectID TABLE
(
ObjectID INT
)

DECLARE @Uniques TABLE
(
ObjectID INT
) 

DECLARE @HoldingAreaValues TABLE
(
FldID SMALLINT IDENTITY(1,1),
Flds VARCHAR(4000),
FldValue CHAR(1) DEFAULT(0)
)

DECLARE @Definition TABLE
(
DefinitionID SMALLINT IDENTITY(1,1),
FieldValue NVARCHAR(4000)
)

INSERT INTO @ShowFields
(
DatabaseName,
TableOwner,
TableName,
FieldName,
ColumnPosition,
ColumnDefaultValue,
ColumnDefaultName,
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
COLUMN_DEFAULT,
dobj.name AS ColumnDefaultName,
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
WHERE sc.object_id = @TableObjId
AND (comp.definition IS NULL OR @IncludeComputedColumns = 1)
ORDER BY
c.TABLE_NAME, c.ORDINAL_POSITION

SET @RCount = @@ROWCOUNT
IF @Verbose = 1 RAISERROR(N'Found %d fields',0,1,@RCount) WITH NOWAIT;
IF @Verbose = 1 SELECT * FROM @ShowFields;

INSERT INTO @HoldingArea (Flds) VALUES('(')

INSERT INTO @Definition(FieldValue)
VALUES('CREATE TABLE ' + @NewTableName)

INSERT INTO @Definition(FieldValue)
VALUES('(')

INSERT INTO @Definition(FieldValue)
SELECT
CHAR(10) + QUOTENAME(FieldName) + ' ' +
CASE
WHEN FieldDefinition IS NOT NULL THEN 'AS ' + FieldDefinition
WHEN DomainName IS NOT NULL AND @UseSystemDataTypes = 0 THEN QUOTENAME(DomainName) + CASE WHEN IsNullable = 1 THEN ' NULL ' ELSE ' NOT NULL ' END
ELSE QUOTENAME(UPPER(DataType)) +
CASE WHEN IsCharColumn = 1 THEN '(' + ISNULL(NULLIF(CAST(MaxLength AS VARCHAR(10)),'-1'),'MAX') + ')' ELSE '' END +
CASE WHEN @IncludeIdentity = 1 AND IdentityColumn = 1 THEN ' IDENTITY(' + CAST(IdentitySeed AS VARCHAR(5))+ ',' + CAST(IdentityIncrement AS VARCHAR(5)) + ')' ELSE '' END +
CASE WHEN IsNullable = 1 THEN ' NULL ' ELSE ' NOT NULL ' END +
CASE WHEN ColumnDefaultName IS NOT NULL AND @IncludeDefaults = 1 THEN ' CONSTRAINT ' + QUOTENAME(ColumnDefaultName + @ConstraintsNameAppend) + ' DEFAULT' + UPPER(ColumnDefaultValue) ELSE '' END
END +
CASE WHEN FieldID = (SELECT MAX(FieldID) FROM @ShowFields) THEN '' ELSE ',' END
FROM @ShowFields

INSERT INTO @Definition(FieldValue)
SELECT
', CONSTRAINT ' + QUOTENAME(name + @ConstraintsNameAppend) + ' FOREIGN KEY (' + ParentColumns + ') REFERENCES ' + ReferencedObject + '(' + ReferencedColumns + ')'
FROM
(
SELECT
ReferencedObject = QUOTENAME(OBJECT_SCHEMA_NAME(fk.referenced_object_id)) + '.' + QUOTENAME(OBJECT_NAME(fk.referenced_object_id)),
ParentObject = QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' + QUOTENAME(OBJECT_NAME(parent_object_id)),
fk.name,
REVERSE(SUBSTRING(REVERSE((
SELECT QUOTENAME(cp.name) + ','
FROM
sys.foreign_key_columns fkc
JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
WHERE fkc.constraint_object_id = fk.object_id
AND cp.name IN (SELECT FieldName FROM @ShowFields)
FOR XML PATH('')
)), 2, 8000)) ParentColumns,
REVERSE(SUBSTRING(REVERSE((
SELECT cr.name + ','
FROM
sys.foreign_key_columns fkc
JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
WHERE fkc.constraint_object_id = fk.object_id
AND cr.name IN (SELECT FieldName FROM @ShowFields)
FOR XML PATH('')
)), 2, 8000)) ReferencedColumns
FROM sys.foreign_keys fk
WHERE fk.parent_object_id = @TableObjId
AND @IncludeForeignKeys = 1
) a

SET @RCount = @@ROWCOUNT
IF @Verbose = 1 RAISERROR(N'Found %d foreign keys',0,1,@RCount) WITH NOWAIT;

INSERT INTO @Definition(FieldValue)
SELECT CHAR(10) + ', CONSTRAINT ' + QUOTENAME(name + @ConstraintsNameAppend) + ' CHECK ' + definition FROM sys.check_constraints
WHERE parent_object_id = @TableObjId
AND @IncludeCheckConstraints = 1

SET @RCount = @@ROWCOUNT
IF @Verbose = 1 RAISERROR(N'Found %d check constraints',0,1,@RCount) WITH NOWAIT;

INSERT INTO @PKObjectID(ObjectID)
SELECT DISTINCT
PKObject = cco.object_id
FROM
sys.key_constraints cco
JOIN sys.index_columns cc ON cco.parent_object_id = cc.object_id AND cco.unique_index_id = cc.index_id
JOIN sys.indexes i ON cc.object_id = i.object_id AND cc.index_id = i.index_id
WHERE
parent_object_id = @TableObjId AND
i.type = 1 AND
is_primary_key = 1
AND @IncludePrimaryKey = 1

SET @RCount = @@ROWCOUNT
IF @Verbose = 1 RAISERROR(N'Found %d primary key',0,1,@RCount) WITH NOWAIT;

INSERT INTO @Uniques(ObjectID)
SELECT DISTINCT
PKObject = cco.object_id
FROM
sys.key_constraints cco
JOIN sys.index_columns cc ON cco.parent_object_id = cc.object_id AND cco.unique_index_id = cc.index_id
JOIN sys.indexes i ON cc.object_id = i.object_id AND cc.index_id = i.index_id
WHERE
parent_object_id = @TableObjId AND
i.type = 2 AND
is_primary_key = 0 AND
is_unique_constraint = 1
AND @IncludeUniqueIndexes = 1

SET @RCount = @@ROWCOUNT
IF @Verbose = 1 RAISERROR(N'Found %d unique indexes',0,1,@RCount) WITH NOWAIT;

SET @ClusteredPK = CASE WHEN @RCount > 0 THEN 1 ELSE 0 END

INSERT INTO @Definition(FieldValue)
SELECT CHAR(10) + ', CONSTRAINT ' + QUOTENAME(name + @ConstraintsNameAppend) + CASE type WHEN 'PK' THEN ' PRIMARY KEY ' + CASE WHEN pk.ObjectID IS NULL THEN ' NONCLUSTERED ' ELSE ' CLUSTERED ' END
WHEN 'UQ' THEN ' UNIQUE ' END + CASE WHEN u.ObjectID IS NOT NULL THEN ' NONCLUSTERED ' ELSE '' END + '(' +
REVERSE(SUBSTRING(REVERSE((
SELECT
QUOTENAME(c.name) + CASE WHEN cc.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END + ','
FROM
sys.key_constraints ccok
INNER JOIN sys.index_columns cc ON ccok.parent_object_id = cc.object_id AND cco.unique_index_id = cc.index_id
INNER JOIN sys.columns c ON cc.object_id = c.object_id AND cc.column_id = c.column_id AND c.name IN (SELECT FieldName FROM @ShowFields)
INNER JOIN sys.indexes i ON cc.object_id = i.object_id AND cc.index_id = i.index_id
WHERE
i.object_id = ccok.parent_object_id AND
ccok.object_id = cco.object_id
FOR XML PATH('')
)), 2, 8000)) + ')'
FROM
sys.key_constraints cco
LEFT JOIN @PKObjectID pk ON cco.object_id = pk.ObjectID
LEFT JOIN @Uniques u ON cco.object_id = u.objectID
WHERE
cco.parent_object_id = @TableObjId
AND (@IncludePrimaryKey = 1 OR @IncludeUniqueIndexes = 1)

IF @IncludeIndexes = 1
BEGIN
INSERT INTO @Definition(FieldValue)
SELECT
CHAR(10) + ', INDEX ' + QUOTENAME([name]) COLLATE SQL_Latin1_General_CP1_CI_AS + ' ' + type_desc + ' (' +
REVERSE(SUBSTRING(REVERSE((
SELECT QUOTENAME(name) + CASE WHEN sc.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END + ','
FROM
sys.index_columns sc
JOIN sys.columns c ON sc.object_id = c.object_id AND sc.column_id = c.column_id
WHERE
sc.object_id = @TableObjId AND
sc.object_id = i.object_id AND
sc.index_id = i.index_id AND
c.name IN (SELECT FieldName FROM @ShowFields)
ORDER BY index_column_id ASC
FOR XML PATH('')
)), 2, 8000)) + ')'
FROM sys.indexes i
WHERE
object_id = @TableObjId
AND CASE WHEN @ClusteredPK = 1 AND is_primary_key = 1 AND type = 1 THEN 0 ELSE 1 END = 1
AND is_unique_constraint = 0
AND is_primary_key = 0

SET @RCount = @@ROWCOUNT
IF @Verbose = 1 RAISERROR(N'Found %d indexes',0,1,@RCount) WITH NOWAIT;
END

INSERT INTO @Definition(FieldValue)
VALUES(CHAR(10) + ')')

INSERT INTO @MainDefinition(FieldValue)
SELECT FieldValue FROM @Definition
ORDER BY DefinitionID ASC

SET @RCount = @@ROWCOUNT
IF @Verbose = 1 RAISERROR(N'Collected %d rows for main definition',0,1,@RCount) WITH NOWAIT;

SET @Result = N'';

SELECT @Result = @Result + CHAR(13) + FieldValue FROM @MainDefinition WHERE FieldValue IS NOT NULL;

END
GO


