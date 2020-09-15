;with ind as (
	select
		a.object_id
		, a.index_id
		, CAST(col_list.list as varchar(max)) as list
	from
		(select distinct object_id, index_id from sys.index_columns) a
		CROSS APPLY
		(select top 100 percent cast(column_id as varchar(16)) + ',' as [text()]
		from sys.index_columns b
		JOIN sys.indexes i
		on b.object_id = i.object_id
		and b.index_id = i.index_id
		where a.object_id = b.object_id
		and a.index_id = b.index_id
		and i.is_primary_key = 0
		and b.is_included_column = 0
		order by b.key_ordinal ASC
		for xml path(''), type
		) col_list (list)
)
SELECT
	  db_name() AS DatabaseName
	, object_schema_name(a.object_id) AS SchemaName
	, object_name(a.object_id) AS TableName
	, [RowCount] = asi.rowcnt

	, asi.name AS FatherIndex
	, bsi.name AS RedundantIndex
	, FatherIndexColumns =
		(select TOP 100 PERCENT QUOTENAME(c.[name]) + ' ' + CASE WHEN ic.is_included_column = 1 THEN N'(INCLUDE)' WHEN ic.is_descending_key = 1 THEN N'DESC' ELSE N'ASC' END + ',' as [text()]
		from sys.index_columns ic
		inner join sys.columns c on c.object_id = ic.object_id and ic.column_id = c.column_id
		where a.object_id = ic.object_id and a.index_id = ic.index_id
		order by ic.is_included_column ASC, ic.key_ordinal ASC
		for xml path('')
		)
	, RedundantIndexColumns =
		(select TOP 100 PERCENT QUOTENAME(c.[name]) + ' ' + CASE WHEN ic.is_included_column = 1 THEN N'(INCLUDE)' WHEN ic.is_descending_key = 1 THEN N'DESC' ELSE N'ASC' END + ',' as [text()]
		from sys.index_columns ic
		inner join sys.columns c on c.object_id = ic.object_id and ic.column_id = c.column_id
		where b.object_id = ic.object_id and b.index_id = ic.index_id
		order by ic.is_included_column ASC, ic.key_ordinal ASC
		for xml path('')
		)

	, FatherIndex_InRowDataKB	= asi.dpages * 8
	, RedundantIndex_InRowDataKB	= bsi.dpages * 8

	, FatherIndex_UserSeeks		= usa.user_seeks
	, RedundantIndex_UserSeeks	= usb.user_seeks

	, FatherIndex_UserScans		= usa.user_scans
	, RedundantIndex_UserScans	= usb.user_scans

	, FatherIndex_UserUpdates	= usa.user_updates
	, RedundantIndex_UserUpdates	= usb.user_updates

	, FatherIndex_LastUserSeeks	= usa.last_user_seek
	, RedundantIndex_LastUserSeeks	= usb.last_user_seek

	, FatherIndex_LastUserScans	= usa.last_user_scan
	, RedundantIndex_LastUserScans	= usb.last_user_scan

	, FatherIndex_LastUserUpdates	= usa.last_user_update
	, RedundantIndex_LastUserUpdates= usb.last_user_update

FROM ind a
INNER JOIN sys.sysindexes asi on asi.id = a.object_id AND asi.indid = a.index_id
INNER JOIN ind b ON a.object_id = b.object_id AND LEN(a.list) > LEN(b.list) and LEFT(a.list, LEN(b.list)) = b.list
INNER JOIN sys.sysindexes bsi ON bsi.id = b.object_id AND bsi.indid = b.index_id
LEFT JOIN sys.dm_db_index_usage_stats usa ON usa.database_id = DB_ID() AND usa.object_id = a.object_id AND usa.index_id = a.index_id
LEFT JOIN sys.dm_db_index_usage_stats usb ON usb.database_id = DB_ID() AND usb.object_id = b.object_id AND usb.index_id = b.index_id
WHERE asi.rowcnt = bsi.rowcnt -- verify same row count
ORDER BY 1, 2