
-- Author: Eitan Blumin
-- Date: 2020-10-28

-- Use this script to fail-over ALL availability groups
-- to the local SECONDARY replica.
-- This code must be run on a SECONDARY replica.

-- !!! BE MINDFUL IN PRODUCTION ENVIRONMENTS AS THIS MAY CAUSE DOWNTIME !!!

-- You can set @WhatIf to 1 to only print the commands without executing them.

DECLARE
	@WhatIf BIT = 1

	  
DECLARE @CMD NVARCHAR(MAX);
DECLARE @TimeString NVARCHAR(25)
DECLARE Commands CURSOR LOCAL FAST_FORWARD
FOR
SELECT N'ALTER AVAILABILITY GROUP ' + QUOTENAME(gr.name) + N' FAILOVER;'
FROM sys.dm_hadr_availability_replica_states AS rs
INNER JOIN sys.availability_groups AS gr ON rs.group_id = gr.group_id
WHERE rs.is_local = 1 AND rs.role_desc = 'SECONDARY'

OPEN Commands
FETCH NEXT FROM Commands INTO @CMD

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @TimeString = CONVERT(nvarchar(25), GETDATE(), 121)
	RAISERROR(N'%s - %s', 0, 1, @TimeString, @CMD) WITH NOWAIT;;

	IF @WhatIf = 0 EXEC(@CMD);

	FETCH NEXT FROM Commands INTO @CMD
END

CLOSE Commands
DEALLOCATE Commands

SET @TimeString = CONVERT(nvarchar(25), GETDATE(), 121)
RAISERROR(N'%s - Done', 0, 1, @TimeString) WITH NOWAIT;;

--SELECT * FROM sys.dm_os_performance_counters WHERE object_name LIKE '%Database Replica%' AND counter_name LIKE '%queue%'