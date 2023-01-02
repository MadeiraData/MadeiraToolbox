IF OBJECT_ID(N'tempdb..#AllTables', N'U') IS NOT NULL
DROP TABLE #AllTables

CREATE TABLE #AllTables
					(
					[DBName]			NVARCHAR(128),
					[SchemaName]		NVARCHAR(128),
					[TableName]			NVARCHAR(128),
					[NRowsTable]		BIGINT,
					[IdentityColumn]	SYSNAME,
					[Seed]				SQL_VARIANT,
					[Increment by]		SQL_VARIANT,
					[LastUsedValue]		BIGINT,
					[DataType]			SYSNAME
					)

INSERT INTO #AllTables
EXEC sp_MSforeachdb 'USE [?]

SELECT DISTINCT
	DB_NAME(),
	SCHEMA_NAME(o.schema_id),
	OBJECT_NAME(ic.[Object_id]),
	ps.row_count,
	ic.Name,
	ic.seed_value,
	ic.increment_value,
	CONVERT(BIGINT, ISNULL(ic.last_value,0)),
	b.name
FROM
	sys.identity_columns ic
	INNER JOIN sys.types b ON ic.system_type_id = b.system_type_id
	INNER JOIN sys.dm_db_partition_stats ps ON ps.[object_id] = ic.[object_id] 
	INNER JOIN sys.indexes i ON ps.index_id = i.index_id  AND ps.[object_id] = i.[object_id]
	INNER JOIN sys.objects o ON ps.[object_id] = o.[object_id] AND o.[type_desc] = ''USER_TABLE''
';


WITH
	DataTypes_CTE
AS
	(
		SELECT N'bigint'	AS DataType, 9223372036854775808	AS [MaxDataTypeValue]
		UNION ALL
		SELECT N'int'		AS DataType, 2147483648				AS [MaxDataTypeValue]
		UNION ALL
		SELECT N'smallint'	AS DataType, 32768					AS [MaxDataTypeValue]
		UNION ALL
		SELECT N'tinyint'	AS DataType, 255					AS [MaxDataTypeValue]
	),
	Main_CTE
AS
	(
		SELECT
			a.[DBName],
			a.[SchemaName],
			a.[TableName],
			a.[NRowsTable],
			a.[IdentityColumn],
			a.[Seed],
			a.[Increment by],
			a.[LastUsedValue],

			dt.DataType									AS [DataType],
			dt.[MaxDataTypeValue],
			CONVERT(NUMERIC(18,2), ((CONVERT(FLOAT, a.[LastUsedValue]) / CONVERT(FLOAT, [MaxDataTypeValue])) * 100))	AS [PercentOfUsage]
		FROM
			#AllTables a
			INNER JOIN DataTypes_CTE dt ON a.[DataType] = dt.DataType
	)

SELECT
	[DBName],
	[SchemaName],
	[TableName],
	[IdentityColumn],
	[DataType],
	[PercentOfUsage],
	100 - [PercentOfUsage]	AS [PercentLeft],
	[LastUsedValue],
	[MaxDataTypeValue],
	[NRowsTable],
	CONCAT(CAST([Seed] AS VARCHAR(16)), '/', CAST([Increment by] AS VARCHAR(16))) AS [Seed/Increment]
FROM
	Main_CTE
ORDER BY
	[PercentOfUsage] DESC
