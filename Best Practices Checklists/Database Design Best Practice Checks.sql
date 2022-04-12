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


-- GUID Leading Index Columns
;WITH 
partition_size AS
(
SELECT object_id,
       used_page_count,
       row_count
FROM sys.dm_db_partition_stats
WHERE index_id <= 1
UNION ALL
-- special index types
SELECT it.parent_object_id,
       ps.used_page_count,
       0 AS row_count
FROM sys.dm_db_partition_stats AS ps
INNER JOIN sys.internal_tables AS it
ON ps.object_id = it.object_id
WHERE it.internal_type_desc IN (
                               'XML_INDEX_NODES','SELECTIVE_XML_INDEX_NODE_TABLE', -- XML indexes
                               'EXTENDED_INDEXES', -- spatial indexes
                               'FULLTEXT_INDEX_MAP','FULLTEXT_AVDL','FULLTEXT_COMP_FRAGMENT','FULLTEXT_DOCID_STATUS','FULLTEXT_INDEXED_DOCID','FULLTEXT_DOCID_FILTER','FULLTEXT_DOCID_MAP', -- fulltext indexes
                               'SEMPLAT_DOCUMENT_INDEX_TABLE','SEMPLAT_TAG_INDEX_TABLE' -- semantic search indexes
                               )
),
object_size AS
(
SELECT object_id,
       SUM(used_page_count) / 128.0 AS object_size_mb,
       SUM(row_count) AS object_row_count
FROM partition_size
GROUP BY object_id
HAVING SUM(used_page_count) > 1024 * 128 -- consider larger tables only > 1 GB
),
guid_index AS
(
SELECT  'GUID leading index column' AS Finding ,
       OBJECT_SCHEMA_NAME(o.object_id) COLLATE DATABASE_DEFAULT AS schemaName, 
       o.name COLLATE DATABASE_DEFAULT AS objectName,
       i.name COLLATE DATABASE_DEFAULT AS indexName,
       i.type_desc COLLATE DATABASE_DEFAULT AS indexType,
       os.object_size_mb,
       os.object_row_count
FROM sys.objects AS o
INNER JOIN sys.indexes AS i
ON o.object_id = i.object_id
INNER JOIN sys.index_columns AS ic
ON i.object_id = ic.object_id
   AND i.index_id = ic.index_id
INNER JOIN sys.columns AS c
ON i.object_id = c.object_id
   AND ic.object_id = c.object_id
   AND ic.column_id = c.column_id
INNER JOIN sys.types AS t
ON c.system_type_id = t.system_type_id
INNER JOIN object_size AS os
ON o.object_id = os.object_id
WHERE i.type_desc IN ('CLUSTERED','NONCLUSTERED') -- Btree indexes
      AND ic.key_ordinal = 1 -- leading column
      AND t.name = 'uniqueidentifier'
      AND i.is_hypothetical = 0
      AND i.is_disabled = 0
      AND o.is_ms_shipped = 0
)
SELECT *
FROM guid_index
