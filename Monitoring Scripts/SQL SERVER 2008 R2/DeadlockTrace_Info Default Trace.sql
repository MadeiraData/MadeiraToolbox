declare @filename nvarchar(200)

select @filename = convert(nvarchar(200), value)
from ::fn_trace_getinfo(null)
where property = 2
and convert(nvarchar(200), value) LIKE '%deadlocks%'

PRINT @filename

select StartTime, convert(xml, TextData) AS Deadlock_Graph, ServerName
from ::fn_trace_gettable(@filename,default)
WHERE TextData IS NOT NULL
order by 1 desc
