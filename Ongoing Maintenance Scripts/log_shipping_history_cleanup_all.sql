USE [master]

DECLARE @curr_agent_id int, @curr_agent_desc varchar(10), @curr_agent_type tinyint, @curr_agent_type_desc varchar(10)

DECLARE agentsCur CURSOR
FAST_FORWARD LOCAL
FOR
SELECT DISTINCT primary_id, convert(varchar(10), 'primary') as agent_desc, t.agent_type, convert(varchar(10), t.agent_type_desc)
FROM msdb.dbo.log_shipping_monitor_primary
CROSS JOIN (VALUES(0,'backup'),(1,'copy')) AS t(agent_type, agent_type_desc)
UNION ALL
SELECT DISTINCT secondary_id, 'secondary' as agent_desc, t.agent_type, t.agent_type_desc
FROM msdb.dbo.log_shipping_monitor_secondary
CROSS JOIN (VALUES(1,'copy'),(2,'restore')) AS t(agent_type, agent_type_desc)

OPEN agentsCur

WHILE 1=1
BEGIN
	FETCH NEXT FROM agentsCur INTO @curr_agent_id, @curr_agent_desc, @curr_agent_type, @curr_agent_type_desc

	IF @@FETCH_STATUS <> 0
		BREAK;

	RAISERROR(N'Cleaning LogShip History for Agent ID: %d (%s), Agent Type: %d (%s)', 0,1
			, @curr_agent_id, @curr_agent_desc, @curr_agent_type, @curr_agent_type_desc) WITH NOWAIT;

	EXEC sp_cleanup_log_shipping_history  
		@agent_id = @curr_agent_id,  
		@agent_type = @curr_agent_type
END

CLOSE agentsCur
DEALLOCATE agentsCur
