SET NOCOUNT ON;
DECLARE @MinimumRowsInTable INT = 200000;
IF OBJECT_ID('tempdb..#FindOnThisDB') IS NOT NULL DROP TABLE #FindOnThisDB;
;WITH Indexes AS
(
select
sets.schema_id,
sets.table_o_id,
sets.key_column_list,
sets.include_column_list,
sets.is_unique,
sets.index_number,
sets.filter_definition
from
(
SELECT
SCHEMA_DATA.schema_id,
TABLE_DATA.object_id as table_o_id,
INDEX_DATA.object_id as index_o_id,
INDEX_DATA.index_id as index_number,
INDEX_DATA.is_unique,
INDEX_DATA.name,
(
SELECT QUOTENAME(cast(keyCol.column_id as varchar) + CASE WHEN keyCol.is_descending_key = 1 THEN 'd' ELSE 'a' END, '{')
FROM sys.tables AS T
INNER JOIN sys.indexes idx ON T.object_id = idx.object_id
INNER JOIN sys.index_columns keyCol ON idx.object_id = keyCol.object_id AND idx.index_id = keyCol.index_id
WHERE INDEX_DATA.object_id = idx.object_id
AND INDEX_DATA.index_id = idx.index_id
AND keyCol.is_included_column = 0
ORDER BY keyCol.key_ordinal
FOR XML PATH('')
) AS key_column_list ,
(
SELECT QUOTENAME(cast(keyColINC.column_id as varchar), '{')
FROM sys.tables AS T
INNER JOIN sys.indexes idxINC ON T.object_id = idxINC.object_id
INNER JOIN sys.index_columns keyColINC ON idxINC.object_id = keyColINC.object_id AND idxINC.index_id = keyColINC.index_id
WHERE
INDEX_DATA.object_id = idxINC.object_id
AND INDEX_DATA.index_id = idxINC.index_id
AND keyColINC.is_included_column = 1
ORDER BY keyColINC.column_id
FOR XML PATH('')
) AS include_column_list ,
INDEX_DATA.filter_definition
FROM sys.indexes INDEX_DATA
INNER JOIN sys.tables TABLE_DATA ON TABLE_DATA.object_id = INDEX_DATA.object_id
INNER JOIN sys.schemas SCHEMA_DATA ON SCHEMA_DATA.schema_id = TABLE_DATA.schema_id

WHERE TABLE_DATA.is_ms_shipped = 0
and INDEX_DATA.is_disabled = 0
AND INDEX_DATA.data_space_id > 0
) AS sets
LEFT JOIN sys.partitions p
ON sets.table_o_id = p.OBJECT_ID AND sets.index_number = p.index_id
where key_column_list is not null
GROUP BY sets.schema_id, sets.table_o_id, sets.index_number, sets.is_unique, sets.key_column_list, sets.include_column_list, sets.filter_definition
HAVING sum(p.rows) >= @MinimumRowsInTable
)
SELECT
DISTINCT
DUPE1.schema_id as schema_id,
DUPE1.table_o_id as table_object_id,
DUPE1.index_number as redundant_index_id ,
DUPE2.index_number as containing_index_id ,
DUPE1.filter_definition as redundant_index_filter,
DUPE2.filter_definition as containing_index_filter,
DUPE1.key_column_list, DUPE1.include_column_list
into #FindOnThisDB
FROM Indexes DUPE1
INNER JOIN Indexes DUPE2
ON
DUPE1.schema_id = DUPE2.schema_id
AND DUPE1.table_o_id = DUPE2.table_o_id
AND DUPE1.index_number <> 1 -- do not consider clustered indexes as redundant
AND DUPE1.is_unique = 0 -- do not consider unique indexes as redundant
AND
(
DUPE1.key_column_list = LEFT(DUPE2.key_column_list, LEN(DUPE1.key_column_list))
and
(
DUPE1.include_column_list is null
or
DUPE1.include_column_list = LEFT(DUPE2.include_column_list, LEN(DUPE1.include_column_list))
)
)
AND DUPE1.index_number <> DUPE2.index_number
AND ISNULL(DUPE1.filter_definition, '') = ISNULL(DUPE2.filter_definition, '')
;

SELECT
database_name = DB_NAME(),
--tb.object_id,
--redundant_index_id = ind1.index_id,
--containing_index_id = ind2.index_id,
schema_name = sch.name,
table_name = tb.name,
ind1.name as redundant_index_name,
redundant_key_columns = STUFF
((
SELECT ', ' + QUOTENAME(col.name) + CASE WHEN keyCol.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
FROM sys.index_columns keyCol
inner join sys.columns col on keyCol.object_id = col.object_id AND keyCol.column_id = col.column_id
WHERE ind1.object_id = keyCol.object_id
AND ind1.index_id = keyCol.index_id
AND keyCol.is_included_column = 0
ORDER BY keyCol.key_ordinal
FOR XML PATH('')), 1, 2, ''),
redundant_include_columns = STUFF
((
SELECT ', ' + QUOTENAME(col.name)
FROM sys.index_columns keyCol
inner join sys.columns col on keyCol.object_id = col.object_id AND keyCol.column_id = col.column_id
WHERE ind1.object_id = keyCol.object_id
AND ind1.index_id = keyCol.index_id
AND keyCol.is_included_column = 1
ORDER BY keyCol.key_ordinal
FOR XML PATH('')), 1, 2, ''),
tbl.redundant_index_filter,
redundant_index_seeks = us1.user_seeks,
redundant_index_scans = us1.user_scans,
redundant_index_updates = us1.user_updates,
containing_index_name = ind2.name,
containing_key_columns = STUFF
((
SELECT ', ' + QUOTENAME(col.name) + CASE WHEN keyCol.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
FROM sys.index_columns keyCol
inner join sys.columns col on keyCol.object_id = col.object_id AND keyCol.column_id = col.column_id
WHERE ind2.object_id = keyCol.object_id
AND ind2.index_id = keyCol.index_id
AND keyCol.is_included_column = 0
ORDER BY keyCol.key_ordinal
FOR XML PATH('')), 1, 2, ''),
containing_include_columns = STUFF
((
SELECT ', ' + QUOTENAME(col.name)
FROM sys.index_columns keyCol
inner join sys.columns col on keyCol.object_id = col.object_id AND keyCol.column_id = col.column_id
WHERE ind2.object_id = keyCol.object_id
AND ind2.index_id = keyCol.index_id
AND keyCol.is_included_column = 1
ORDER BY keyCol.key_ordinal
FOR XML PATH('')), 1, 2, ''),
tbl.containing_index_filter,
containing_index_seeks = us2.user_seeks,
containing_index_scans = us2.user_scans,
containing_index_updates = us2.user_updates,
DropCmd = N'USE ' + QUOTENAME(DB_NAME()) + N'; DROP INDEX ' + QUOTENAME(ind1.name) + N' ON ' + QUOTENAME(sch.name) + N'.' + QUOTENAME(tb.name) + N';'
from #FindOnThisDB AS tbl
INNER JOIN sys.tables tb
ON tb.object_id = tbl.table_object_id
INNER JOIN sys.schemas sch
ON sch.schema_id = tbl.schema_id
INNER JOIN sys.indexes ind1
ON ind1.object_id = tbl.table_object_id and ind1.index_id = tbl.redundant_index_id
INNER JOIN sys.indexes ind2
ON ind2.object_id = tbl.table_object_id and ind2.index_id = tbl.containing_index_id
LEFT JOIN sys.dm_db_index_usage_stats AS us1 ON us1.database_id = DB_ID() AND us1.object_id = ind1.object_id AND us1.index_id = ind1.index_id
LEFT JOIN sys.dm_db_index_usage_stats AS us2 ON us2.database_id = DB_ID() AND us2.object_id = ind2.object_id AND us2.index_id = ind2.index_id
WHERE ind1.index_id > 0 AND ind2.index_id > 0

--DROP TABLE #FindOnThisDB