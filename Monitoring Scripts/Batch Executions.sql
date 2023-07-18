/*

Batch Executions
================

Author:			Guy Glantser, https://www.madeiradata.com
Date:			18/07/2023
Description:
	This script creates an event session that captures all types of batch executions.
	It includes two events - sqlserver.sql_batch_completed and sqlserver.rpc_completed.
	As an example, it also includes some filters, but these can be easily modified.
	It then includes a script to extract the data from the target and retrieve for each event
	the event type, the batch text, the event timestamp, and the execution duration in milliseconds.
	Finally, the script includes commands to stop and drop the event session.
*/

USE
	master;
GO


-- Drop the event session if it already exists

IF
	EXISTS
	(
		SELECT
			NULL
		FROM
			sys.server_event_sessions
		WHERE
			name = N'BatchExecutions'
	)
BEGIN

	DROP EVENT SESSION
		BatchExecutions
	ON
		SERVER;

END;
GO


-- Create the event session

CREATE EVENT SESSION
	BatchExecutions
ON
	SERVER
ADD EVENT
	sqlserver.sql_batch_completed
	(
		WHERE
			duration > 100000
	) ,
ADD EVENT
	sqlserver.rpc_completed
	(
		WHERE
			sqlserver.client_app_name = N'AppName'
	)
ADD TARGET
	package0.event_file
	(
		SET filename = N'C:\CourseMaterials\ExtendedEvents\BatchExecutions.xel'
	);
GO


-- Start the event session

ALTER EVENT SESSION
	BatchExecutions
ON
	SERVER
STATE = START;
GO


-- Query the event file

WITH
	TargetData
(
	TargetDataXML
)
AS
(
	SELECT
		TargetDataXML = CAST (event_data AS XML)
	FROM
		sys.fn_xe_file_target_read_file (N'C:\CourseMaterials\ExtendedEvents\BatchExecutions*.xel' , NULL , NULL , NULL)
)
SELECT
	EventType				= SessionEventData.value (N'(@name)[1]' , N'NVARCHAR(MAX)') ,
	BatchText				=
		CASE
			SessionEventData.value (N'(@name)[1]' , N'NVARCHAR(MAX)')
		WHEN
			N'sql_batch_completed'
		THEN
			SessionEventData.value (N'(data[@name="batch_text"]/value/text())[1]' , N'NVARCHAR(MAX)')
		WHEN
			N'rpc_completed'
		THEN
			SessionEventData.value (N'(data[@name="statement"]/value/text())[1]' , N'NVARCHAR(MAX)')
		END ,
	EventDateTime			= SessionEventData.value (N'(@timestamp)[1]' , N'DATETIME2') ,
	Duration_Microseconds	= SessionEventData.value (N'(data[@name="duration"]/value/text())[1]' , N'BIGINT')
FROM
	TargetData
CROSS APPLY
	TargetDataXML.nodes (N'/event') AS SessionEvents (SessionEventData)
ORDER BY
	EventDateTime ASC;
GO


-- Stop the event session

ALTER EVENT SESSION
	BatchExecutions
ON
	SERVER
STATE = STOP;
GO


-- Drop the event session

DROP EVENT SESSION
	BatchExecutions
ON
	SERVER;
GO
