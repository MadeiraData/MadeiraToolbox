declare @filename nvarchar(200)

select TOP 1 @filename = convert(nvarchar(200), value)
from ::fn_trace_getinfo(null)
where property = 2
and [value] IS NOT NULL
--and convert(nvarchar(200), value) LIKE '%deadlocks%'
ORDER BY traceid ASC

PRINT @filename

select StartTime, convert(xml, TextData) AS Deadlock_Graph, ServerName
from ::fn_trace_gettable(@filename,default)
WHERE TextData IS NOT NULL
order by 1 desc
