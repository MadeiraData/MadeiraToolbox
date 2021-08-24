/*
Detect Sleeping Sessions with Old Open Transactions
====================================================
Author: Eitan Blumin
Date: 2021-06-01
This script detects when there are sleeping sessions with open transactions older than 10 minutes by default.
Such sessions can cause blocking and can prevent the transaction log from clearing, leading to excessive log file growth and space exhaustion.
Additionally, when snapshot isolation is used, they can prevent version cleanup from occurring in tempdb.

If the issue is still occurring, you can run this script to gather full details about the sleeping session(s) in real-time, and generate corresponding KILL commands.
*/
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
IF OBJECT_ID('tempdb..#SessionsWithLocks') IS NOT NULL DROP TABLE #SessionsWithLocks;
CREATE TABLE #SessionsWithLocks(request_session_id int NOT NULL, resource_database_id int NOT NULL, PRIMARY KEY (request_session_id, resource_database_id));
INSERT INTO #SessionsWithLocks(request_session_id, resource_database_id)
SELECT DISTINCT request_session_id, resource_database_id
FROM sys.dm_tran_locks
WHERE resource_type = N'DATABASE'
AND request_mode = N'S'
AND request_status = N'GRANT'
AND request_owner_type = N'SHARED_TRANSACTION_WORKSPACE';

SELECT
s.session_id
, 'KILL ' + CAST(s.session_id AS NVARCHAR(100)) + ';' AS KillCommand,
c.connect_time AS ConnectionTime,
s.last_request_start_time AS LastRequestStartTime,
s.last_request_end_time AS LastRequestEndTime,
s.open_transaction_count AS OpenTranCount,
DATEDIFF(minute, s.last_request_start_time, GETDATE()) AS OpenTranMinutes,
s.login_name AS LoginName,
s.nt_user_name AS NTUserName,
RTRIM(s.[program_name]) AS ClientProgramName,
RTRIM(s.[host_name]) AS ClientHostName,
s.host_process_id AS ClientHostPID,
c.client_net_address AS ClientHostIP,
db.[resource_database_id] AS DatabaseID,
DB_NAME(db.resource_database_id) AS DatabaseName,
(SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(c.most_recent_sql_handle)) AS LastCmdText,
CASE WHEN EXISTS ( SELECT 1
FROM sys.dm_tran_active_transactions AS tat
JOIN sys.dm_tran_session_transactions AS tst
ON tst.transaction_id = tat.transaction_id
WHERE tat.name = 'implicit_transaction'
AND s.session_id = tst.session_id
) THEN 1
ELSE 0
END AS IsImplicitTransaction
FROM sys.dm_exec_sessions s
INNER JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
INNER JOIN #SessionsWithLocks AS db ON s.session_id = db.request_session_id
WHERE s.database_id <> 32767
AND s.status = 'sleeping'
AND s.open_transaction_count > 0
AND s.last_request_start_time <= s.last_request_end_time
AND s.last_request_end_time < DATEADD(MINUTE, -5, SYSDATETIME())
AND EXISTS(SELECT * FROM sys.dm_tran_locks WHERE request_session_id = s.session_id
AND NOT (resource_type = N'DATABASE' AND request_mode = N'S' AND request_status = N'GRANT' AND request_owner_type = N'SHARED_TRANSACTION_WORKSPACE'))
ORDER BY s.last_request_end_time ASC
OPTION(MAXDOP 1);