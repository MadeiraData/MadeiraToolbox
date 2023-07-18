/*

Track Recompiles
================

Author:			Guy Glantser, https://www.madeiradata.com
Date:			18/07/2023
Description:
	This script creates an event session that captures SQL statement recompiles along with their recompile cause.
	It uses the asynchronous file target.
	It then includes a script to extract the data from the files and retrieve for each event
	the statement text, the event timestamp, and the recompile cause.
	Finally, the script includes commands to stop and drop the event session.
	This event session can be useful tr troubleshoot recompilations issues.
*/

USE
	master;
GO


-- Create an event session for statement recompiles

IF
	EXISTS
	(
		SELECT
			NULL
		FROM
			sys.server_event_sessions
		WHERE
			name = N'TrackRecompiles'
	)
BEGIN

	DROP EVENT SESSION
		TrackRecompiles
	ON
		SERVER;

END;
GO


CREATE EVENT SESSION
	TrackRecompiles
ON
	SERVER
ADD EVENT
	sqlserver.sql_statement_recompile
		(SET collect_statement = 1)
ADD TARGET
	package0.asynchronous_file_target
(
	SET FILENAME = N'C:\SomeFolder\TrackRecompiles.xel'
)
WITH
(
	MAX_DISPATCH_LATENCY = 1 SECONDS
);
GO


ALTER EVENT SESSION
	TrackRecompiles
ON
	SERVER
STATE = START;
GO


-- Display individual events

WITH
	RecompileEvents
(
	EventDataXML
)
AS
(
	SELECT
		EventDataXML = CAST (RecompileEvents.event_data AS XML)
	FROM
		sys.fn_xe_file_target_read_file (N'C:\SomeFolder\TrackRecompiles*.xel' , NULL , NULL , NULL) AS RecompileEvents
)
SELECT
	EventDateTime	= EventDataXML.value (N'(event/@timestamp)[1]' , N'DATETIME2(0)') ,
	StatementText	= EventDataXML.value (N'(event/data[@name="statement"]/value)[1]' , N'NVARCHAR(MAX)') ,
	RecompileCause	= EventDataXML.value (N'(event/data[@name="recompile_cause"]/text)[1]' , N'NVARCHAR(MAX)')
FROM
	RecompileEvents
GO


-- Stop the event session

ALTER EVENT SESSION
	TrackRecompiles
ON
	SERVER
STATE = STOP;
GO


-- Drop the event session

DROP EVENT SESSION
	TrackRecompiles
ON
	SERVER;
GO
