/*
Detect Non Secured Connections (SSL) to the SQL Server instance
===============================================================
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Last Update: 2020-07-15
Description: Use this to make sure that all connections to the SQL Server instance are secured with SSL.
*/
SELECT CONCAT('Not secured connection(s) detected of '
, ISNULL(QUOTENAME(COALESCE(ses.original_login_name, ses.nt_user_name, ses.login_name)), 'an unknown login')
, ' from ', ISNULL(QUOTENAME(client_net_address), 'an unknown address')
, ':', ISNULL(QUOTENAME(con.client_tcp_port), 'from an unknown port')
, ' ', QUOTENAME(ISNULL(ses.host_name, 'unknown host'), '(')
, ', ', ISNULL(QUOTENAME(ses.program_name), 'unknown program')
, ', protocol version ', ISNULL(CONVERT(varchar(8000),con.protocol_version), 'unknown')
, ', to ', ISNULL(QUOTENAME(DB_NAME(ses.database_id)), 'an unknown database')
, ', local port ', ISNULL(CONVERT(nvarchar(4000), con.local_tcp_port), 'unknown')
, ', ', ISNULL('last command: ' + t.[text], 'without any command')
, ': ', COUNT(ses.session_id)
), COUNT(con.connection_id)
FROM sys.dm_exec_connections AS con
LEFT JOIN sys.dm_exec_sessions AS ses
ON ses.session_id IN (con.session_id, con.most_recent_session_id)
OUTER APPLY sys.dm_exec_sql_text(con.most_recent_sql_handle) AS t
WHERE encrypt_option = 'FALSE'
AND net_transport = 'TCP'
AND client_net_address NOT LIKE '<%'
AND con.protocol_version > 0 -- ignore pre-shake connections
GROUP BY COALESCE(ses.original_login_name, ses.nt_user_name, ses.login_name)
, con.protocol_version, client_net_address, con.local_tcp_port, con.client_tcp_port, t.[text], ses.host_name, ses.program_name, ses.database_id
