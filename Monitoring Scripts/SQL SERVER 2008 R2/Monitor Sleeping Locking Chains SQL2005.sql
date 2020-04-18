/*
Author: Eitan Blumin
Date:	2011-02-16
Description:
==================================================================================================
								The session that blocked
==================================================================================================

This script will find BLOCKING sessions
that didn't execute any command for at least an hour
and are currently sleeping.

The script will display:

	1. The blocking session's last executed command.
	2. Session details such as host name, application name, login, last executed command time etc.

If there are seelping blocking sessions found, 
	the script will write the relevant details in the windows event log.
If there are no sleeping blocking sessions, 
	the script will display 'No locks found!'
==================================================================================================
*/
SET NOCOUNT ON;

DECLARE @ThresholdNumOfMinutesSinceRequestEnd INT
SET @ThresholdNumOfMinutesSinceRequestEnd = 60;

DECLARE @HeadBlockers TABLE(spid SMALLINT)
DECLARE @data XML, @msg NVARCHAR(MAX);

INSERT INTO @HeadBlockers
SELECT DISTINCT e.blocking_session_id	-- Get the ID of the blocking session
FROM sys.dm_exec_requests as e
JOIN sys.dm_exec_sessions as s
ON e.blocking_session_id = s.session_id
LEFT JOIN sys.dm_exec_requests as b
ON e.blocking_session_id = b.session_id
WHERE
	e.blocking_session_id <> 0				-- Filter blocking sessions only
AND b.blocking_session_id IS NULL			-- Filter head blockers only
-- Filter where last command finished more than an hour ago
AND s.last_request_end_time < dateadd(minute, -@ThresholdNumOfMinutesSinceRequestEnd, GETDATE())

IF @@ROWCOUNT > 0
BEGIN
	;
	WITH LockingChain
	AS
	(
		SELECT spid AS rootspid, spid
		FROM
			@HeadBlockers
		
		UNION ALL
		
		SELECT HB.rootspid, session_id
		FROM
			sys.dm_exec_requests AS R
		JOIN
			LockingChain AS HB
		ON
			HB.spid = R.blocking_session_id
	), HeadBlockers
	AS
	(
		SELECT rootspid AS spid, COUNT(*) - 1 AS LockingChainSize
		FROM
			LockingChain
		GROUP BY
			rootspid
	)
	SELECT @data =
		(
			SELECT
				GETDATE() AS [@TimeOfCheck],
				S.session_id AS [@SessionID],
				S.login_time,
				S.host_name,
				S.program_name,
				S.login_name,
				S.status,
				S.last_request_start_time,
				S.last_request_end_time,
				S.original_login_name,
				HB.LockingChainSize,
				T.text AS most_recent_sql_text
			FROM
				HeadBlockers AS HB
			JOIN
				sys.dm_exec_sessions AS S
			ON 
				S.session_id = HB.spid
			JOIN
				sys.dm_exec_connections AS C
			ON
				S.session_id = C.session_id
			OUTER APPLY
				sys.dm_exec_sql_text(C.most_recent_sql_handle) AS T
			FOR XML PATH ('Session'), ROOT ('HeadBlockers')
		);

	SET @msg = CONVERT(nvarchar(max),@data);
	
	RAISERROR(@msg,16,1) WITH LOG;
END
ELSE
	PRINT 'No locks found!'