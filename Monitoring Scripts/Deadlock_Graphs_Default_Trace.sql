DECLARE @curr_tracefilename varchar(500), @base_tracefilename varchar(500), @indx int ;
 
SELECT @curr_tracefilename = path from sys.traces where is_default = 1 ; 
SET @curr_tracefilename = reverse(@curr_tracefilename);
 
SELECT @indx  = patindex('%\%', @curr_tracefilename) ;
SET @curr_tracefilename = reverse(@curr_tracefilename) ;
 
SET @base_tracefilename = left( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '\log.trc' ; 

PRINT @base_tracefilename

SELECT event_timestamp
,event_data.value('(event/data/value/deadlock/process-list/process/executionStack/frame/@procname)[1]','SYSNAME') AS deadlock_procedure
,event_data.query('.') AS deadlock_graph
,victimProcess.victim_process_xml
,victim_process_xml.value('(process/inputbuf/text())[1]','nvarchar(max)') AS victimInputBuf
FROM
(
select StartTime AS event_timestamp, convert(xml, TextData) AS event_data
from ::fn_trace_gettable(@base_tracefilename,default)
WHERE TextData IS NOT NULL
AND TextData LIKE N'%<deadlock>%'
) AS d
CROSS APPLY
(SELECT victim_process_xml = event_data.query('
for $victimId in distinct-values(/event/data[@name=''xml_report'']/value/deadlock/victim-list/victimProcess/@id)
	return /event/data[@name=''xml_report'']/value/deadlock/process-list/process[@id = $victimId]
')
) AS victimProcess
ORDER BY 1 DESC