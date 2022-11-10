USE
	[master];
GO


-- Create the event session

CREATE EVENT SESSION
	CaptureErrors
ON
	SERVER
ADD EVENT
	sqlserver.error_reported
	(
		ACTION
		(
			sqlserver.session_server_principal_name ,
			sqlserver.client_hostname ,
			sqlserver.client_app_name ,
			sqlserver.session_id ,
			sqlserver.[database_name] ,
			sqlserver.tsql_frame
		)
		WHERE
			severity > 10	-- Only errors
	)
ADD TARGET
	event_file
	(
		 SET filename = N'C:\XEvents\Errors.xel'
		)
WITH
(
	MAX_DISPATCH_LATENCY = 1 SECONDS
);
GO


-- Start the event session

ALTER EVENT SESSION
	CaptureErrors
ON
	SERVER
STATE = START;
GO


-- Query the event file

WITH
	RawEvents
(
	RawEvent
)
AS
(
	SELECT
		RawEvent = CAST (event_data AS XML)
	FROM
		sys.fn_xe_file_target_read_file (N'C:\XEvents\Errors*.xel' , NULL , NULL , NULL)
) ,

	ExtractedEvents
AS
(
	SELECT
		ErrorDateTime	= CAST (SWITCHOFFSET (RawEvent.value (N'(/event/@timestamp)[1]' , N'DATETIME2(0)') , DATENAME (TZOFFSET , SYSDATETIMEOFFSET())) AS DATETIME2(0)) ,
		ErrorNumber		= RawEvent.value (N'(/event/data[@name="error_number"]/value/text())[1]' , N'INT') ,
		ErrorSeverity	= RawEvent.value (N'(/event/data[@name="severity"]/value/text())[1]' , N'INT') ,
		ErrorState		= RawEvent.value (N'(/event/data[@name="state"]/value/text())[1]' , N'INT') ,
		ErrorMessage	= RawEvent.value (N'(/event/data[@name="message"]/value/text())[1]' , N'NVARCHAR(MAX)') ,
		LoginName		= RawEvent.value (N'(/event/action[@name="session_server_principal_name"]/value/text())[1]' , N'SYSNAME') ,
		ClientHostName	= RawEvent.value (N'(/event/action[@name="client_hostname"]/value/text())[1]' , N'NVARCHAR(MAX)') ,
		ClientAppName	= RawEvent.value (N'(/event/action[@name="client_app_name"]/value/text())[1]' , N'NVARCHAR(MAX)') ,
		SessionId		= RawEvent.value (N'(/event/action[@name="session_id"]/value/text())[1]' , N'INT') ,
		DatabaseName	= RawEvent.value (N'(/event/action[@name="database_name"]/value/text())[1]' , N'SYSNAME') ,
		SQLHandle		= CONVERT (VARBINARY(MAX) , RawEvent.value (N'(/event/action[@name="tsql_frame"]/value/frame/@handle)[1]' , N'NVARCHAR(MAX)') , 1) ,
		LineNumber		= RawEvent.value (N'(/event/action[@name="tsql_frame"]/value/frame/@line)[1]' , N'INT') ,
		StartOffset		= RawEvent.value (N'(/event/action[@name="tsql_frame"]/value/frame/@offsetStart)[1]' , N'INT') ,
		EndOffset		= RawEvent.value (N'(/event/action[@name="tsql_frame"]/value/frame/@offsetEnd)[1]' , N'INT')
	FROM
		RawEvents
)

SELECT
		ErrorDateTime		= ExtractedEvents.ErrorDateTime ,
		ErrorNumber			= ExtractedEvents.ErrorNumber ,
		ErrorSeverity		= ExtractedEvents.ErrorSeverity ,
		ErrorState			= ExtractedEvents.ErrorState ,
		ErrorMessage		= ExtractedEvents.ErrorMessage ,
		DatabaseName		= ExtractedEvents.DatabaseName ,
		ModuleSchemaName	= OBJECT_SCHEMA_NAME (SQLTexts.objectid , SQLTexts.[dbid]) ,
		ModuleName			= OBJECT_NAME (SQLTexts.objectid , SQLTexts.[dbid]) ,
		LineNumber			= ExtractedEvents.LineNumber ,
		StatementText		=
			SUBSTRING
			(
				SQLTexts.[text] ,
				ExtractedEvents.StartOffset / 2 + 1 ,
				(
					CASE ExtractedEvents.EndOffset
						WHEN -1 THEN LEN (SQLTexts.[text])
						ELSE ExtractedEvents.EndOffset / 2
					END
					- ExtractedEvents.StartOffset / 2
				)
				+ 1
			) ,
		LoginName			= ExtractedEvents.LoginName ,
		ClientHostName		= ExtractedEvents.ClientHostName ,
		ClientAppName		= ExtractedEvents.ClientAppName ,
		SessionId			= ExtractedEvents.SessionId
FROM
	ExtractedEvents
CROSS APPLY
	sys.dm_exec_sql_text (SQLHandle) AS SQLTexts
ORDER BY
	ErrorDateTime ASC;
GO


-- Stop the event session

ALTER EVENT SESSION
	CaptureErrors
ON
	SERVER
STATE = STOP;
GO


-- Drop the event session

DROP EVENT SESSION
	CaptureErrors
ON
	SERVER;
GO
