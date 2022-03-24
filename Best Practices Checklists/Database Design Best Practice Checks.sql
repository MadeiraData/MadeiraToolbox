SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

-- User Table columns created with ANSI_PADDING OFF
SELECT DISTINCT 'Column with ANSI_PADDING OFF' AS Finding
, SCHEMA_NAME(t.schema_id) AS schemaName, t.name AS tableName
, c.name AS columnName
, ct.name AS dataType
, c.is_nullable, c.max_length, c.collation_name
FROM sys.columns AS c
INNER JOIN sys.types AS ct ON c.system_type_id = ct.system_type_id
INNER JOIN sys.objects AS t ON c.object_id = t.object_id
WHERE t.is_ms_shipped = 0
AND SCHEMA_NAME(t.schema_id) <> 'sys'
AND c.is_ansi_padded = 0
AND ct.name IN ('char','nchar','varchar','nvarchar','text','ntext','binary','varbinary','image')


-- User Table columns with sql_variant, text, ntext, or image data types
-- or varchar/nvarchar/varbinary data types with short max length
-- or floating-point data types (real/float)
SELECT DISTINCT
 CASE WHEN ct.name IN ('sql_variant') THEN N'Column with SQL_VARIANT data type'
      WHEN ct.name IN ('text','ntext','image') THEN 'Column with deprecated data type'
      WHEN ct.name IN ('real','float') THEN 'Column with floating-point data type'
      ELSE N'Variable-length column with short max length'
 END AS Finding
, SCHEMA_NAME(t.schema_id) AS schemaName, t.name AS tableName
, c.name AS columnName
, ct.name AS dataType
, c.is_nullable, c.max_length, c.collation_name, c.is_ansi_padded
FROM sys.columns AS c
INNER JOIN sys.types AS ct ON c.system_type_id = ct.system_type_id
INNER JOIN sys.objects AS t ON c.object_id = t.object_id
WHERE t.is_ms_shipped = 0
AND SCHEMA_NAME(t.schema_id) <> 'sys'
AND (ct.name IN ('sql_variant','text','ntext','image','real','float')
OR (ct.name IN ('varchar','nvarchar','varbinary') AND c.max_length BETWEEN 1 AND 2)
)

-- Parameters with sql_variant, text, ntext, or image data types
-- or varchar/nvarchar/varbinary data types with short max length
-- or floating-point data types (real/float)
SELECT DISTINCT
 CASE WHEN ct.name IN ('sql_variant') THEN N'Parameter with SQL_VARIANT data type'
      WHEN ct.name IN ('text','ntext','image') THEN 'Parameter with deprecated data type'
      WHEN ct.name IN ('real','float') THEN 'Parameter with floating-point data type'
      ELSE N'Variable-length Parameter with short max length'
 END AS Finding
, SCHEMA_NAME(t.schema_id) AS schemaName, t.name AS moduleName, t.type_desc AS moduleType
, c.name AS parameterName
, ct.name AS dataType
, c.default_value, c.max_length
FROM sys.parameters AS c
INNER JOIN sys.types AS ct ON c.system_type_id = ct.system_type_id
INNER JOIN sys.objects AS t ON c.object_id = t.object_id
WHERE t.is_ms_shipped = 0
AND SCHEMA_NAME(t.schema_id) <> 'sys'
AND (ct.name IN ('sql_variant','text','ntext','image','real','float')
OR (ct.name IN ('varchar','nvarchar','varbinary') AND c.max_length BETWEEN 1 AND 2)
)

-- User Table Triggers
SELECT DISTINCT N'Trigger on user table' AS Finding
, SCHEMA_NAME(t.schema_id) AS schemaName, t.name AS tableName
, trg.name AS triggerName
, OBJECT_DEFINITION(trg.object_id) AS triggerDefinition
, trg.is_disabled
, trg.is_instead_of_trigger
FROM sys.triggers AS trg
INNER JOIN sys.objects AS t ON trg.parent_id = t.object_id
WHERE t.is_ms_shipped = 0
AND SCHEMA_NAME(t.schema_id) <> 'sys'
AND trg.is_ms_shipped = 0


-- Disabled foreign keys and check constraints
SELECT N'Disabled constraint' AS Finding
, OBJECT_SCHEMA_NAME(d.parent_object_id) AS schemaName, OBJECT_NAME(d.parent_object_id) AS tableName
, [name] AS constraintName, d.type_desc AS constraintType
FROM
(
	SELECT name, parent_object_id, type_desc FROM sys.foreign_keys WHERE is_disabled = 1
	UNION ALL
	SELECT name, parent_object_id, type_desc FROM sys.check_constraints WHERE is_disabled = 1
) AS d


-- Disabled indexes
SELECT N'Disabled index' AS Finding
, OBJECT_SCHEMA_NAME(object_id) AS schemaName, OBJECT_NAME(object_id) AS tableName
, [name] AS indexName, type_desc AS indexType
FROM sys.indexes
WHERE is_disabled = 1


-- Indexed views
SELECT N'Indexed view' AS Finding
, SCHEMA_NAME(vw.schema_id) AS schemaName, vw.name AS viewName
, OBJECT_DEFINITION(vw.object_id) AS viewDefinition
FROM sys.views AS vw
WHERE EXISTS (SELECT NULL FROM sys.indexes AS ix WHERE ix.object_id = vw.object_id)


-- Heap tables
SELECT N'Heap table' AS Finding
, SCHEMA_NAME(t.schema_id) AS schemaName, t.name AS tableName
FROM sys.indexes AS ix
INNER JOIN sys.objects AS t ON ix.object_id = t.object_id
WHERE t.is_ms_shipped = 0
AND SCHEMA_NAME(t.schema_id) <> 'sys'
AND ix.index_id = 0

-- Foreign Keys without corresponding index
SELECT  'Foreign Key without corresponding index' AS Finding ,
	schemaName		= OBJECT_SCHEMA_NAME (ForeignKeysWithColumns.ObjectId) ,
	tableName		= OBJECT_NAME (ForeignKeysWithColumns.ObjectId) ,
	foreignKeyName		= ForeignKeysWithColumns.ForeignKeyName ,
	foreignKeyColumns	= ForeignKeysWithColumns.ForeignKeyColumnList ,
	isDisabled		= ForeignKeysWithColumns.IsDisabled ,
	isNotTrusted		= ForeignKeysWithColumns.IsNotTrusted
FROM
	(
		SELECT
			ObjectId		= ForeignKeys.parent_object_id ,
			ForeignKeyColumnList	= ForeignKeyColumns.ForeignKeyColumnList ,
			ForeignKeyName		= ForeignKeys.name ,
			IsDisabled		= ForeignKeys.is_disabled ,
			IsNotTrusted		= ForeignKeys.is_not_trusted
		FROM
			sys.foreign_keys AS ForeignKeys
		CROSS APPLY
			(
				SELECT STUFF((
						SELECT
							N',' + QUOTENAME(Columns.name)
						FROM
							sys.foreign_key_columns AS ForeignKeyColumns
						INNER JOIN
							sys.columns AS Columns
						ON
							ForeignKeyColumns.parent_object_id = Columns.object_id
						AND
							ForeignKeyColumns.parent_column_id = Columns.column_id
						WHERE
							ForeignKeyColumns.constraint_object_id = ForeignKeys.object_id
						ORDER BY
							ForeignKeyColumns.constraint_column_id ASC
						FOR XML PATH (N'')
					), 1, 1, N'') AS ForeignKeyColumnList
			)
			AS ForeignKeyColumns
	)
	AS ForeignKeysWithColumns
LEFT OUTER JOIN
	(
		SELECT
			ObjectId	= Indexes.object_id ,
			IndexKeysList	= IndexKeys.IndexKeysList
		FROM
			sys.indexes AS Indexes
		CROSS APPLY
			(
				SELECT STUFF((
						SELECT
							N',' + QUOTENAME(Columns.name)
						FROM
							sys.index_columns AS IndexColumns
						INNER JOIN
							sys.columns AS Columns
						ON
							IndexColumns.object_id = Columns.object_id
						AND
							IndexColumns.column_id = Columns.column_id
						WHERE
							IndexColumns.object_id = Indexes.object_id
						AND
							IndexColumns.index_id = Indexes.index_id
						ORDER BY
							IndexColumns.index_column_id ASC
						FOR XML PATH (N'')
					), 1, 1, N'') AS IndexKeysList
			)
			AS IndexKeys
	)
	AS IndexesWithColumns
ON
	ForeignKeysWithColumns.ObjectId = IndexesWithColumns.ObjectId
AND (
	IndexesWithColumns.IndexKeysList LIKE REPLACE(REPLACE(ForeignKeysWithColumns.ForeignKeyColumnList,'[','_'),']','_') + N'%'
	OR
	ForeignKeysWithColumns.ForeignKeyColumnList LIKE REPLACE(REPLACE(IndexesWithColumns.IndexKeysList,'[','_'),']','_') + N'%'
	)
WHERE IndexesWithColumns.ObjectId IS NULL;

