/*

Monitor RPC Executions
======================

Author:			Guy Glantser, https://www.madeiradata.com
Date:			18/07/2023
Description:
	This script creates an event session that captures RPC executions from a specific application.
	It uses the ring buffer target and keeps the last 5 events.
	It then includes a script to extract the data from the ring buffer and retrieve for each event
	the statement text, the event timestamp, and the execution duration in milliseconds.
	Finally, the script includes commands to stop and drop the event session.
	The event session definition can be modified, of course, to include different filters, actions (global fields),
	or even additional events.
	The query that extracts the data can also be modified to extract additional columns of interest.
*/

USE
	master;
GO


-- Create an event session

CREATE EVENT SESSION
	MonitorRPCExecutions
ON
	SERVER
ADD EVENT
	sqlserver.rpc_completed
		(
			WHERE sqlserver.client_app_name = N'AppName'
		)
ADD TARGET
	package0.ring_buffer
		(
			SET max_events_limit = 5
		)
WITH
(
	MAX_DISPATCH_LATENCY = 1 SECONDS
);
GO


-- Start the event session

ALTER EVENT SESSION
	MonitorRPCExecutions
ON
	SERVER
STATE = START;
GO


-- Query the ring buffer for the last 5 executions

WITH
	TargetData
(
	TargetDataXML
)
AS
(
	SELECT
		TargetDataXML = CAST (target_data AS XML)
	FROM
		sys.dm_xe_session_targets AS SessionTargets
	INNER JOIN
		sys.dm_xe_sessions EventSessions
	ON
		SessionTargets.event_session_address = EventSessions.address
	WHERE
		SessionTargets.target_name = N'ring_buffer'
	AND
		EventSessions.name = N'MonitorRPCExecutions'
)
SELECT
	StatemenmtText			= SessionEventData.value (N'(data[@name="statement"]/value/text())[1]' , 'NVARCHAR(MAX)') ,
	EventDateTime			= SessionEventData.value ('(@timestamp)[1]' , 'DATETIME2') ,
	Duration_Microseconds	= SessionEventData.value (N'(data[@name="duration"]/value/text())[1]' , 'BIGINT')
FROM
	TargetData
CROSS APPLY
	TargetDataXML.nodes (N'/RingBufferTarget/event') AS SessionEvents (SessionEventData)
ORDER BY
	EventDateTime ASC;
GO


-- Stop the event session

ALTER EVENT SESSION
	MonitorRPCExecutions
ON
	SERVER
STATE = STOP;
GO


-- Drop the event session

DROP EVENT SESSION
	MonitorRPCExecutions
ON
	SERVER;
GO
