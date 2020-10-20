DECLARE @MinutesToCheckBack INT = NULL

IF (SELECT convert(int,value_in_use) FROM sys.configurations WHERE name = 'default trace enabled')  = 1
BEGIN
 
DECLARE @curr_tracefilename varchar(500), @base_tracefilename varchar(500), @indx int ;
 
SELECT @curr_tracefilename = path from sys.traces where is_default = 1 ; 
SET @curr_tracefilename = reverse(@curr_tracefilename);
 
SELECT @indx  = patindex('%\%', @curr_tracefilename) ;
SET @curr_tracefilename = reverse(@curr_tracefilename) ;
 
SET @base_tracefilename = left( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '\log.trc' ; 
         
 
SELECT
ServerName,
EventTime = StartTime,
ActionClass = CASE WHEN tc.name = 'Objects' THEN 'DDL' ELSE tc.name END,
ActionType = CASE te.Name WHEN 'Object:Created' THEN 'CREATE' WHEN 'Object:Altered' THEN 'ALTER' WHEN 'Object:Deleted' THEN 'DROP' ELSE te.Name END,
DatabaseName,
DatabaseID,
ObjectName = ISNULL(OBJECT_NAME(ObjectID, DatabaseID), ObjectName),
SubObjectName = CASE WHEN OBJECT_NAME(ObjectID, DatabaseID) IS NOT NULL THEN ObjectName END,
ObjectID,
ObjectID2,
IndexID,
ObjectType =
CASE t.ObjectType
WHEN 8259 THEN 'Check Constraint'
WHEN 8260 THEN 'Default (constraint or standalone)'
WHEN 8262 THEN 'Foreign-key Constraint'
WHEN 8272 THEN 'Stored Procedure'
WHEN 8274 THEN 'Rule'
WHEN 8275 THEN 'System Table'
WHEN 8276 THEN 'Trigger on Server'
WHEN 8277 THEN '(User-defined) Table'
WHEN 8278 THEN 'View'
WHEN 8280 THEN 'Extended Stored Procedure'
WHEN 16724 THEN 'CLR Trigger'
WHEN 16964 THEN 'Database'
WHEN 16975 THEN 'Object'
WHEN 17222 THEN 'FullText Catalog'
WHEN 17232 THEN 'CLR Stored Procedure'
WHEN 17235 THEN 'Schema'
WHEN 17475 THEN 'Credential'
WHEN 17491 THEN 'DDL Event'
WHEN 17741 THEN 'Management Event'
WHEN 17747 THEN 'Security Event'
WHEN 17749 THEN 'User Event'
WHEN 17985 THEN 'CLR Aggregate Function'
WHEN 17993 THEN 'Inline Table-valued SQL Function'
WHEN 18000 THEN 'Partition Function'
WHEN 18002 THEN 'Replication Filter Procedure'
WHEN 18004 THEN 'Table-valued SQL Function'
WHEN 18259 THEN 'Server Role'
WHEN 18263 THEN 'Microsoft Windows Group'
WHEN 19265 THEN 'Asymmetric Key'
WHEN 19277 THEN 'Master Key'
WHEN 19280 THEN 'Primary Key'
WHEN 19283 THEN 'ObfusKey'
WHEN 19521 THEN 'Asymmetric Key Login'
WHEN 19523 THEN 'Certificate Login'
WHEN 19538 THEN 'Role'
WHEN 19539 THEN 'SQL Login'
WHEN 19543 THEN 'Windows Login'
WHEN 20034 THEN 'Remote Service Binding'
WHEN 20036 THEN 'Event Notification on Database'
WHEN 20037 THEN 'Event Notification'
WHEN 20038 THEN 'Scalar SQL Function'
WHEN 20047 THEN 'Event Notification on Object'
WHEN 20051 THEN 'Synonym'
WHEN 20549 THEN 'End Point'
WHEN 20801 THEN 'Adhoc Queries which may be cached'
WHEN 20816 THEN 'Prepared Queries which may be cached'
WHEN 20819 THEN 'Service Broker Service Queue'
WHEN 20821 THEN 'Unique Constraint'
WHEN 21057 THEN 'Application Role'
WHEN 21059 THEN 'Certificate'
WHEN 21075 THEN 'Server'
WHEN 21076 THEN 'Transact-SQL Trigger'
WHEN 21313 THEN 'Assembly'
WHEN 21318 THEN 'CLR Scalar Function'
WHEN 21321 THEN 'Inline scalar SQL Function'
WHEN 21328 THEN 'Partition Scheme'
WHEN 21333 THEN 'User'
WHEN 21571 THEN 'Service Broker Service Contract'
WHEN 21572 THEN 'Trigger on Database'
WHEN 21574 THEN 'CLR Table-valued Function'
WHEN 21577 THEN 'Internal Table (For example, XML Node Table, Queue Table.)'
WHEN 21581 THEN 'Service Broker Message Type'
WHEN 21586 THEN 'Service Broker Route'
WHEN 21587 THEN 'Statistics'
WHEN 21825 THEN 'User'
WHEN 21827 THEN 'User'
WHEN 21831 THEN 'User'
WHEN 21843 THEN 'User'
WHEN 21847 THEN 'User'
WHEN 22099 THEN 'Service Broker Service'
WHEN 22601 THEN 'Index'
WHEN 22604 THEN 'Certificate Login'
WHEN 22611 THEN 'XMLSchema'
WHEN 22868 THEN 'Type'
ELSE 'Unknown Type (' + CONVERT(varchar,t.ObjectType) + ')'
END,
HostName,
ApplicationName,
LoginName
--,t.*
FROM ::fn_trace_gettable( @base_tracefilename, default ) t
INNER JOIN sys.trace_events TE 
ON T.EventClass = TE.trace_event_id 
INNER JOIN sys.trace_categories TC
ON TE.category_id = TC.category_id
LEFT JOIN sys.trace_subclass_values STE 
ON T.EventSubClass = STE.subclass_value 
AND T.EventClass = STE.trace_event_id
WHERE DB_NAme(DatabaseID) <> 'tempdb' -- ignore temporary objects
AND t.ApplicationName <> 'SQLServerCEIP' -- ignore client experience telemetry
AND STE.subclass_name NOT IN ('Begin','Rollback')
AND t.ObjectType NOT IN (
	  21587	-- Statistics
	--, 8277	-- User Defined Table
	)
AND TE.trace_event_id IN (
	 46,164,47	-- DDL
	)
AND (@MinutesToCheckBack IS NULL OR t.StartTime >= DATEADD(minute, -@MinutesToCheckBack, GETDATE()))
 
END
