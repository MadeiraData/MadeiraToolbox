DECLARE @LPIM bit;

IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_os_memory_nodes') AND name = 'locked_page_allocations_kb')
BEGIN
	SELECT	@LPIM = CASE WHEN MAX(a.locked_page_allocations_kb) > 0
			     THEN 1
			     ELSE 0
			END
	FROM
		sys.dm_os_memory_nodes a
	INNER	JOIN sys.dm_os_nodes   b ON a.memory_node_id = b.memory_node_id
	WHERE	b.node_state_desc = 'ONLINE'
	OPTION (RECOMPILE);
END
ELSE IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_os_process_memory') AND name = 'locked_page_allocations_kb')
BEGIN
	SELECT @LPIM = CASE WHEN MAX(locked_page_allocations_kb) > 0
			THEN 1
			ELSE 0
		    END
	FROM sys.dm_os_process_memory
	OPTION(RECOMPILE);
END
ELSE IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_os_sys_info') AND name = 'sql_memory_model')
BEGIN
	SELECT @LPIM = CASE WHEN sql_memory_model = 2
			THEN 1
			ELSE 0
		    END
	FROM sys.dm_os_sys_info
	OPTION(RECOMPILE);
END
ELSE IF IS_SRVROLEMEMBER('sysadmin') = 1 AND EXISTS (SELECT * FROM sys.configurations c WHERE c.name = 'xp_cmdshell' AND c.value_in_use = 1)
BEGIN
	DECLARE @Res table ([output] nvarchar(255) NULL);
	INSERT INTO @Res
	EXEC xp_cmdshell 'whoami /priv';

	IF EXISTS (SELECT * FROM @Res WHERE [output] LIKE 'SeLockMemoryPrivilege%')
		SET @LPIM = 1;
	ELSE
		SET @LPIM = 0;
END
ELSE
BEGIN
	DECLARE @MemStatus AS table (detail sysname, KB bigint);
	INSERT INTO @MemStatus
	EXEC(N'DBCC MEMORYSTATUS');
	
	SELECT @LPIM = CASE WHEN MAX(KB) > 0
			THEN 1
			ELSE 0
		    END
	FROM @MemStatus
	WHERE (detail LIKE '%AWE%'
	OR detail LIKE '%Locked%')
	AND KB > 0
END

SELECT msg = N'In server ' + @@SERVERNAME + N', Lock Pages In Memory is: '
	+ CASE @LPIM WHEN 1 THEN N'ENABLED' WHEN 0 THEN N'DISABLED' ELSE N'UNKNOWN' END
	, @LPIM AS IsLPIMEnabled