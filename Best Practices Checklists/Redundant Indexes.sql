WITH
	IndexDetails
(
	SchemaName ,
	ObjectName ,
	ObjectId ,
	ObjectType ,
	IndexId ,
	IndexName ,
	IndexType ,
	ConstraintType ,
	FilterDefinition ,
	IndexKeys ,
	IncludedColumns
)
AS
(
	SELECT
		SchemaName			= SCHEMA_NAME (Objects.schema_id) ,
		ObjectName			= Objects.name ,
		ObjectId			= Indexes.object_id ,
		ObjectType			=	CASE Objects.type
									WHEN 'U'	THEN N'Table'
									WHEN 'V'	THEN N'View'
								END ,
		IndexId				= Indexes.index_id ,
		IndexName			= Indexes.name ,
		IndexType			= Indexes.type_desc ,
		ConstraintType		=	CASE
									WHEN Indexes.is_primary_key = 1			THEN N'Primary Key'
									WHEN Indexes.is_unique_constraint = 1	THEN N'Unique Constraint'
									WHEN Indexes.is_unique = 1				THEN N'Unique Index'
									ELSE N''
								END ,
		FilterDefinition	= Indexes.filter_definition ,
		IndexKeys			=
			STRING_AGG
			(
				CASE
					WHEN IndexColumns.is_included_column = 0
						THEN Columns.name
					ELSE
						NULL
				END ,
				N' , '
			)
			WITHIN GROUP (ORDER BY IndexColumns.key_ordinal ASC) ,
		IncludedColumns		=
			STRING_AGG
			(
				CASE
					WHEN IndexColumns.is_included_column = 1
						THEN Columns.name
					ELSE
						NULL
				END ,
				N' , '
			)
			WITHIN GROUP (ORDER BY IndexColumns.key_ordinal ASC)
	FROM
		sys.indexes AS Indexes
	INNER JOIN
		sys.objects AS Objects
	ON
		Indexes.object_id = Objects.object_id
	INNER JOIN
		sys.index_columns AS IndexColumns
	ON
		Indexes.object_id = IndexColumns.object_id
	AND
		Indexes.index_id = IndexColumns.index_id
	INNER JOIN
		sys.columns AS Columns
	ON
		IndexColumns.object_id = Columns.object_id
	AND
		IndexColumns.column_id = Columns.column_id
	WHERE
		Objects.type IN ('U' , 'V')	-- Table or view
	AND
		Indexes.is_disabled = 0
	AND
		Indexes.is_hypothetical = 0
	GROUP BY
		Objects.schema_id ,
		Objects.name ,
		Indexes.object_id ,
		Objects.type ,
		Indexes.index_id ,
		Indexes.name ,
		Indexes.type_desc ,
		Indexes.is_primary_key ,
		Indexes.is_unique_constraint ,
		Indexes.is_unique ,
		Indexes.filter_definition
)
SELECT
	SchemaName					= LongIndexes.SchemaName ,
	ObjectName					= LongIndexes.ObjectName ,
	ObjectId					= LongIndexes.ObjectId ,
	ObjectType					= LongIndexes.ObjectType ,
	LongIndexId					= LongIndexes.IndexId ,
	LongIndexName				= LongIndexes.IndexName ,
	LongIndexType				= LongIndexes.IndexType ,
	LongIndexConstraintType		= LongIndexes.ConstraintType ,
	LongIndexFilterDefinition	= LongIndexes.FilterDefinition ,
	LongIndexKeys				= LongIndexes.IndexKeys ,
	LongIndexIncludedColumns	= LongIndexes.IncludedColumns ,
	ShortIndexId				= ShortIndexes.IndexId ,
	ShortIndexName				= ShortIndexes.IndexName ,
	ShortIndexType				= ShortIndexes.IndexType ,
	ShortIndexConstraintType	= ShortIndexes.ConstraintType ,
	ShortIndexFilterDefinition	= ShortIndexes.FilterDefinition ,
	ShortIndexKeys				= ShortIndexes.IndexKeys ,
	ShortIndexIncludedColumns	= ShortIndexes.IncludedColumns
FROM
	IndexDetails AS LongIndexes
INNER JOIN
	IndexDetails AS ShortIndexes
ON
	LongIndexes.ObjectId = ShortIndexes.ObjectId
AND
	(
		LongIndexes.IndexKeys LIKE ShortIndexes.IndexKeys + N' , %'
	AND
		(LongIndexes.IncludedColumns LIKE ShortIndexes.IncludedColumns + N' , %' OR LongIndexes.IncludedColumns IS NOT NULL AND ShortIndexes.IncludedColumns IS NULL)
	OR
		LongIndexes.IndexKeys = ShortIndexes.IndexKeys
	AND
		(LongIndexes.IncludedColumns LIKE ShortIndexes.IncludedColumns + N' , %' OR LongIndexes.IncludedColumns IS NOT NULL AND ShortIndexes.IncludedColumns IS NULL)
	OR
		LongIndexes.IndexKeys LIKE ShortIndexes.IndexKeys + N' , %'
	AND
		ISNULL (LongIndexes.IncludedColumns , N'') = ISNULL (ShortIndexes.IncludedColumns , N'')
	OR
		LongIndexes.IndexKeys = ShortIndexes.IndexKeys
	AND
		ISNULL (LongIndexes.IncludedColumns , N'') = ISNULL (ShortIndexes.IncludedColumns , N'')
	AND
		LongIndexes.IndexId < ShortIndexes.IndexId
	)
ORDER BY
	LongIndexes.SchemaName	ASC ,
	LongIndexes.ObjectName	ASC ,
	LongIndexes.IndexId		ASC ,
	ShortIndexes.IndexId	ASC;
GO
