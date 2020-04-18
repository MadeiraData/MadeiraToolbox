;with ind as (
	select
		a.object_id
		, a.index_id
		, CAST(col_list.list as varchar(max)) as list
	from
		(select distinct OBJECT_ID, index_id from sys.index_columns) a
		CROSS APPLY
		(select cast(column_id as varchar(16)) + ',' as [text()]
		from sys.index_columns b
		JOIN sys.indexes i
		on b.object_id = i.object_id
		and b.index_id = i.index_id
		where a.object_id = b.object_id
		and a.index_id = b.index_id
		AND i.is_primary_key = 0
		for xml path(''), type
		) col_list (list)
)
SELECT
	object_name(a.object_id) AS TableName
	, asi.name AS FatherIndex
	, bsi.name AS RedundantIndex
FROM ind a
JOIN sys.sysindexes asi
on asi.id = a.object_id
AND asi.indid = a.index_id
JOIN ind b
ON a.object_id = b.object_id
AND LEN(a.list) > LEN(b.list)
and LEFT(a.list, LEN(b.list)) = b.list
JOIN sys.sysindexes bsi
ON bsi.id = b.object_id
AND bsi.indid = b.index_id
ORDER BY 1, 2