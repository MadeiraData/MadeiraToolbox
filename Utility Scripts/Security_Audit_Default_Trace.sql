/*
Get Security Audit trace data from the SQL Server Default Trace
===============================================================
This script is based on a sample script provided here:
https://www.red-gate.com/simple-talk/databases/sql-server/performance-sql-server/the-default-trace-in-sql-server-the-power-of-performance-and-security-auditing/
*/
DECLARE @filePath VARCHAR(1000);
SET @filePath = (SELECT TOP (1) [path] FROM sys.traces WHERE is_default = 1);

PRINT @filePath;
SET @filePath = SUBSTRING(@filePath, 0, LEN(@filePath) - CHARINDEX('_', REVERSE(@filePath)) + 1) + '.trc'
PRINT @filePath;

SELECT TOP (1000)
	te.[name] AS [EventName] ,
	v.subclass_name ,
        t.DatabaseName ,
        t.DatabaseID ,
        t.NTDomainName ,
        t.ApplicationName ,
        t.HostName ,
        t.ClientProcessID ,
        t.LoginName ,
        t.SPID ,
        t.StartTime ,
        t.RoleName ,
        t.TargetUserName ,
        t.TargetLoginName ,
        t.SessionLoginName ,
	t.TextData
FROM    sys.fn_trace_gettable(@filePath, default) AS t
        JOIN sys.trace_events te ON t.EventClass = te.trace_event_id
        JOIN sys.trace_subclass_values v ON v.trace_event_id = te.trace_event_id AND v.subclass_value = t.EventSubClass
WHERE te.category_id = 8 -- Security Audit
AND (
	te.[name] LIKE N'%GDR Event%'
	OR  te.[name] IN
		('Audit Addlogin Event', 'Audit Add DB User Event', 'Audit Add Role Event', 'Audit App Role Change Password Event'
		,'Audit Statement Permission Event', 'Audit Schema Object Access Event', 'Audit Database Object Access Event', 'Audit Change Audit Event'
		,'Audit Object Derived Permission Event', 'Audit Database Principal Management Event', 'Audit Server Principal Management Event'
		,'Audit Add Member to DB Role Event', 'Audit Add Login to Server Role Event' 
		--,'Audit Change Database Owner'
		--,'Audit Server Operation Event','Audit Database Operation Event'
		--,'Audit Server Alter Trace Event'
		)
--        AND v.subclass_name IN ( 'add', 'Grant database access' )
)
ORDER BY t.StartTime DESC
