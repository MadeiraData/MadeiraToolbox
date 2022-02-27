DECLARE @curr_tracefilename varchar(500), @base_tracefilename varchar(500), @indx int ;
 
SELECT @curr_tracefilename = path from sys.traces where is_default = 1 ; 
SET @curr_tracefilename = reverse(@curr_tracefilename);
 
SELECT @indx  = patindex('%\%', @curr_tracefilename) ;
SET @curr_tracefilename = reverse(@curr_tracefilename) ;
 
SET @base_tracefilename = left( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '\log.trc' ; 

PRINT @base_tracefilename

select StartTime, convert(xml, TextData) AS Deadlock_Graph, ServerName
from ::fn_trace_gettable(@base_tracefilename,default)
WHERE TextData IS NOT NULL
AND TextData LIKE N'%deadlock%'
order by 1 desc
