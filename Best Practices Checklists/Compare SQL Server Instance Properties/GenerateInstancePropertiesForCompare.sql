 ----------------------------------------------------------------------------------
 -- Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
 -- Date: 26/06/18
 -- Description:
 --		Outputs all the server level objects and definitions so that they can be compared against same definitions on another server.
 --
 -- Instructions:
 --		Run on "First" server. Save output to a CSV file.
 --		Run on "Second" server. Save output to a CSV file.
 --		Use the other script ( CompareInstanceProperties.sql ) to load the files into a table, and output any differences
 -- Disclaimer:
 --		Does not check replication or log shipping.
 --		For Full text only checks existence.
 --		Requires sysadmin privliges on both servers.
 ----------------------------------------------------------------------------------
 SET NOCOUNT ON;

	USE master
	GO
	-- Declare global variables
	
	DECLARE @sqlcmd NVARCHAR(max), @params NVARCHAR(500), @sqlmajorver int
	DECLARE @UpTime VARCHAR(12),@StartDate DATETIME
	DECLARE @agt smallint, @ole smallint, @sao smallint, @xcmd smallint
	DECLARE @ErrorSeverity int, @ErrorState int, @ErrorMessage NVARCHAR(4000)
	DECLARE @CMD NVARCHAR(4000)
	DECLARE @path NVARCHAR(2048)
	DECLARE @sqlminorver int, @sqlbuild int, @clustered bit, @osver VARCHAR(5), @ostype VARCHAR(10), @osdistro VARCHAR(20), @server VARCHAR(128), @instancename NVARCHAR(128), @arch smallint, @ossp VARCHAR(25), @SystemManufacturer VARCHAR(128)
	DECLARE @existout int, @FSO int, @FS int, @OLEResult int, @FileID int
	DECLARE @FileName VARCHAR(200), @Text1 VARCHAR(2000), @CMD2 VARCHAR(100)
	DECLARE @src VARCHAR(255), @desc VARCHAR(255), @psavail VARCHAR(20), @psver tinyint
	DECLARE @dbid int, @dbname NVARCHAR(1000), @affined_cpus int
	DECLARE @maxservermem bigint, @minservermem bigint, @systemmem bigint, @systemfreemem bigint, @numa_nodes_afinned tinyint, @LowMemoryThreshold int
	DECLARE @commit_target bigint -- Includes stolen and reserved memory in the memory manager
	DECLARE @committed bigint -- Does not include reserved memory in the memory manager
	DECLARE @mwthreads_count int, @xtp int
	
	-- Initialize global variables (logic copied from Microsoft's Tiger Toolbox BPCheck: https://github.com/Microsoft/tigertoolbox/tree/master/BPCheck )

	SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);
	SET @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff);
	SET @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);
	
	SELECT @systemmem = total_physical_memory_kb/1024 FROM sys.dm_os_sys_memory;

	IF (@sqlmajorver >= 11 AND @sqlmajorver < 14) OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500)
	BEGIN
		SET @sqlcmd = N'SELECT @ostypeOUT = ''Windows'', @osdistroOUT = ''Windows'', @osverOUT = CASE WHEN windows_release IN (''6.3'',''10.0'') AND (@@VERSION LIKE ''%Build 10586%'' OR @@VERSION LIKE ''%Build 14393%'') THEN ''10.0'' ELSE windows_release END, @osspOUT = windows_service_pack_level, @archOUT = CASE WHEN @@VERSION LIKE ''%<X64>%'' THEN 64 WHEN @@VERSION LIKE ''%<IA64>%'' THEN 128 ELSE 32 END FROM sys.dm_os_windows_info (NOLOCK)';
		SET @params = N'@osverOUT VARCHAR(5) OUTPUT, @ostypeOUT VARCHAR(10) OUTPUT, @osdistroOUT VARCHAR(20) OUTPUT, @osspOUT VARCHAR(25) OUTPUT, @archOUT smallint OUTPUT';
		EXECUTE sp_executesql @sqlcmd, @params, @osverOUT=@osver OUTPUT, @ostypeOUT=@ostype OUTPUT, @osdistroOUT=@osdistro OUTPUT, @osspOUT=@ossp OUTPUT, @archOUT=@arch OUTPUT;
	END
	ELSE IF @sqlmajorver >= 14
	BEGIN
		SET @sqlcmd = N'SELECT @ostypeOUT = host_platform, @osdistroOUT = host_distribution, @osverOUT = CASE WHEN host_platform = ''Windows'' AND host_release IN (''6.3'',''10.0'') THEN ''10.0'' ELSE host_release END, @osspOUT = host_service_pack_level, @archOUT = CASE WHEN @@VERSION LIKE ''%<X64>%'' THEN 64 ELSE 32 END FROM sys.dm_os_host_info (NOLOCK)';
		SET @params = N'@osverOUT VARCHAR(5) OUTPUT, @ostypeOUT VARCHAR(10) OUTPUT, @osdistroOUT VARCHAR(20) OUTPUT, @osspOUT VARCHAR(25) OUTPUT, @archOUT smallint OUTPUT';
		EXECUTE sp_executesql @sqlcmd, @params, @osverOUT=@osver OUTPUT, @ostypeOUT=@ostype OUTPUT, @osdistroOUT=@osdistro OUTPUT, @osspOUT=@ossp OUTPUT, @archOUT=@arch OUTPUT;
	END
	ELSE
	BEGIN
		BEGIN TRY
			DECLARE @str VARCHAR(500), @str2 VARCHAR(500), @str3 VARCHAR(500)
			DECLARE @sysinfo TABLE (id int, 
				[Name] NVARCHAR(256), 
				Internal_Value bigint, 
				Character_Value NVARCHAR(256));
			
			INSERT INTO @sysinfo
			EXEC xp_msver;
		
			SELECT @osver = LEFT(Character_Value, CHARINDEX(' ', Character_Value)-1) -- 5.2 is WS2003; 6.0 is WS2008; 6.1 is WS2008R2; 6.2 is WS2012, 6.3 is WS2012R2, 6.3 (14396) is WS2016
			FROM @sysinfo
			WHERE [Name] LIKE 'WindowsVersion%';
		
			SELECT @arch = CASE WHEN RTRIM(Character_Value) LIKE '%x64%' OR RTRIM(Character_Value) LIKE '%AMD64%' THEN 64
				WHEN RTRIM(Character_Value) LIKE '%x86%' OR RTRIM(Character_Value) LIKE '%32%' THEN 32
				WHEN RTRIM(Character_Value) LIKE '%IA64%' THEN 128 END
			FROM @sysinfo
			WHERE [Name] LIKE 'Platform%';
		
			SET @str = (SELECT @@VERSION)
			SELECT @str2 = RIGHT(@str, LEN(@str)-CHARINDEX('Windows',@str) + 1)
			SELECT @str3 = RIGHT(@str2, LEN(@str2)-CHARINDEX(': ',@str2))
			SELECT @ossp = LTRIM(LEFT(@str3, CHARINDEX(')',@str3) -1))
			SET @ostype = 'Windows'
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Windows Version and Architecture subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END;
	DECLARE @port VARCHAR(15), @replication int, @RegKey NVARCHAR(255), @cpuaffin VARCHAR(255), @cpucount int, @numa int
	DECLARE @i int, @cpuaffin_fixed VARCHAR(300), @affinitymask NVARCHAR(64), @affinity64mask NVARCHAR(64), @cpuover32 int

	IF @sqlmajorver < 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild < 2500)
	BEGIN
		BEGIN TRY
			SELECT @RegKey = CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('InstanceName')) IS NULL THEN N'Software\Microsoft\MSSQLServer\MSSQLServer\SuperSocketNetLib\Tcp'
				ELSE N'Software\Microsoft\Microsoft SQL Server\' + CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(128)) + N'\MSSQLServer\SuperSocketNetLib\Tcp' END
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @RegKey, N'TcpPort', @port OUTPUT, NO_OUTPUT
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Instance info subsection - Error raised in TRY block 1. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END
	ELSE
	BEGIN
		BEGIN TRY
			SET @sqlcmd = N'SELECT @portOUT = MAX(CONVERT(VARCHAR(15),port)) FROM sys.dm_tcp_listener_states WHERE is_ipv4 = 1 AND [type] = 0 AND ip_address <> ''127.0.0.1'';';
			SET @params = N'@portOUT VARCHAR(15) OUTPUT';
			EXECUTE sp_executesql @sqlcmd, @params, @portOUT = @port OUTPUT;
			IF @port IS NULL
			BEGIN
				SET @sqlcmd = N'SELECT @portOUT = MAX(CONVERT(VARCHAR(15),port)) FROM sys.dm_tcp_listener_states WHERE is_ipv4 = 0 AND [type] = 0 AND ip_address <> ''127.0.0.1'';';
				SET @params = N'@portOUT VARCHAR(15) OUTPUT';
				EXECUTE sp_executesql @sqlcmd, @params, @portOUT = @port OUTPUT;
			END
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Instance info subsection - Error raised in TRY block 2. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END

	BEGIN TRY
		EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\Replication', N'IsInstalled', @replication OUTPUT, NO_OUTPUT
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Instance info subsection - Error raised in TRY block 3. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH

	SELECT @cpucount = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64
	SELECT @affined_cpus = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64;
	SELECT @numa = COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64;
	
	;WITH bits AS 
	(SELECT 7 AS N, 128 AS E UNION ALL SELECT 6, 64 UNION ALL 
	SELECT 5, 32 UNION ALL SELECT 4, 16 UNION ALL SELECT 3, 8 UNION ALL 
	SELECT 2, 4 UNION ALL SELECT 1, 2 UNION ALL SELECT 0, 1), 
	bytes AS 
	(SELECT 1 M UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
	SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
	SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)
	-- CPU Affinity is shown highest to lowest CPU ID
	SELECT @affinitymask = CASE WHEN [value] = 0 THEN REPLICATE('1', @cpucount)
		ELSE RIGHT((SELECT ((CONVERT(tinyint, SUBSTRING(CONVERT(binary(9), [value]), M, 1)) & E) / E) AS [text()] 
			FROM bits CROSS JOIN bytes
			ORDER BY M, N DESC
			FOR XML PATH('')), @cpucount) END
	FROM sys.configurations (NOLOCK)
	WHERE name = 'affinity mask';

	IF @cpucount > 32
	BEGIN
		;WITH bits AS 
		(SELECT 7 AS N, 128 AS E UNION ALL SELECT 6, 64 UNION ALL 
		SELECT 5, 32 UNION ALL SELECT 4, 16 UNION ALL SELECT 3, 8 UNION ALL 
		SELECT 2, 4 UNION ALL SELECT 1, 2 UNION ALL SELECT 0, 1), 
		bytes AS 
		(SELECT 1 M UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
		SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
		SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)
		-- CPU Affinity is shown highest to lowest CPU ID
		SELECT @affinity64mask = CASE WHEN [value] = 0 THEN REPLICATE('1', @cpucount)
			ELSE RIGHT((SELECT ((CONVERT(tinyint, SUBSTRING(CONVERT(binary(9), [value]), M, 1)) & E) / E) AS [text()] 
				FROM bits CROSS JOIN bytes
				ORDER BY M, N DESC
				FOR XML PATH('')), @cpucount) END
		FROM sys.configurations (NOLOCK)
		WHERE name = 'affinity64 mask';
	END;

	IF @cpucount > 32
	SELECT @cpuover32 = ABS(LEN(@affinity64mask) - (@cpucount-32))

	SELECT @cpuaffin = CASE WHEN @cpucount > 32 THEN REVERSE(LEFT(REVERSE(@affinity64mask),@cpuover32)) + RIGHT(@affinitymask,32) ELSE RIGHT(@affinitymask,@cpucount) END

	SET @cpuaffin_fixed = @cpuaffin

	IF @numa > 1
	BEGIN
		-- format binary mask by node for better reading
		SET @i = @cpucount/@numa + 1
		WHILE @i < @cpucount + @numa
		BEGIN
			SELECT @cpuaffin_fixed = STUFF(@cpuaffin_fixed, @i, 1, '_' + SUBSTRING(@cpuaffin_fixed, @i, 1))
			SET @i = @i + @cpucount/@numa + 1
		END
	END

	-- Prepare temporary table to hold results

	IF OBJECT_ID('tempdb..#InstanceProperties') IS NOT NULL DROP TABLE #InstanceProperties;

	CREATE TABLE #InstanceProperties
	(
		Category NVARCHAR(100) COLLATE database_default,
		ItemName NVARCHAR(500) COLLATE database_default,
		PropertyName NVARCHAR(500) COLLATE database_default,
		PropertyValue NVARCHAR(MAX) COLLATE database_default
	);

-- 0. Operating System and Server properties

	RAISERROR ('Check 0',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	VALUES
	 ('Server Properties', 'Instance', 'Domain', DEFAULT_DOMAIN())
	,('Server Properties', 'Instance', 'Default Collation', CONVERT(nvarchar(max), SERVERPROPERTY('Collation')))
	,('Server Properties', 'Instance', 'SQL Edition', CONVERT(nvarchar(max), SERVERPROPERTY('Edition')))
	,('Server Properties', 'Instance', 'SQL Version', CONVERT(nvarchar(max), SERVERPROPERTY('ProductVersion')))
	,('Server Properties', 'Instance', 'Service Pack Level', CONVERT(nvarchar(max), SERVERPROPERTY('ProductLevel')))
	,('Server Properties', 'Instance', 'Product Update Reference', CONVERT(nvarchar(max), SERVERPROPERTY('ProductUpdateReference')))
	,('Server Properties', 'Instance', 'Is Always On Enabled', CONVERT(nvarchar(max), SERVERPROPERTY('IsHadrEnabled')))
	,('Server Properties', 'Instance', 'Is Clustered', CONVERT(nvarchar(max), SERVERPROPERTY('IsClustered')))
	,('Server Properties', 'Instance', 'Is Full Text Installed', CONVERT(nvarchar(max), SERVERPROPERTY('IsFullTextInstalled')))
	,('Server Properties', 'Instance', 'Is Integrated Security Only', CONVERT(nvarchar(max), SERVERPROPERTY('IsIntegratedSecurityOnly')))
	,('Server Properties', 'Instance', 'Is LocalDB', CONVERT(nvarchar(max), SERVERPROPERTY('IsLocalDB')))
	,('Server Properties', 'Instance', 'Is Polybase Installed', CONVERT(nvarchar(max), SERVERPROPERTY('IsPolybaseInstalled')))
	,('Server Properties', 'Instance', 'Is XTP Supported', CONVERT(nvarchar(max), SERVERPROPERTY('IsXTPSupported')))
	,('Server Properties', 'Instance', 'Is Replication Installed', CASE WHEN @replication = 1 THEN '1' WHEN @replication = 0 THEN '0' ELSE 'INVALID INPUT/ERROR' END)
	,('Server Properties', 'Instance', 'Is Advanced Analytics Installed', CONVERT(nvarchar(max), SERVERPROPERTY('IsAdvancedAnalyticsInstalled')))
	,('Server Properties', 'Instance', 'Filestream Configured Level', CONVERT(nvarchar(max), SERVERPROPERTY('FilestreamConfiguredLevel')))
	,('Server Properties', 'Instance', 'Filestream Effective Level', CONVERT(nvarchar(max), SERVERPROPERTY('FilestreamEffectiveLevel')))
	,('Server Properties', 'Instance', 'Filestream Share Name', CONVERT(nvarchar(max), SERVERPROPERTY('FilestreamShareName')))
	,('Server Properties', 'Instance', 'Port', RTRIM(@port))
	,('Server Properties', 'Instance', N'CPU Affinity Mask', @cpuaffin_fixed)
	,('Server Properties', 'Instance', N'Affined CPUs', LTRIM(STR(@affined_cpus)))
	,('Server Properties', 'OS', N'Windows Version', 
		CASE @osver WHEN '5.2' THEN 'XP/WS2003'
			WHEN '6.0' THEN 'Vista/WS2008'
			WHEN '6.1' THEN 'W7/WS2008R2'
			WHEN '6.2' THEN 'W8/WS2012'
			WHEN '6.3' THEN 'W8.1/WS2012R2'
			WHEN '10.0' THEN 'W10/WS2016'
			ELSE @ostype + ' ' + @osdistro
		END)
	,('Server Properties', 'OS', N'Windows Service Pack Level', CASE WHEN @ostype = 'Windows' THEN @ossp ELSE @osver END)
	,('Server Properties', 'OS', N'Architecture', LTRIM(STR(@arch)))
	,('Server Properties', 'OS', N'CPU Count', LTRIM(STR(@cpucount)))
	,('Server Properties', 'OS', N'Total Memory (MB)', LTRIM(STR(@systemmem)))

	INSERT INTO #InstanceProperties
	SELECT 'Server Properties', 'OS', v.*
	FROM sys.dm_os_sys_info
	CROSS APPLY
	(VALUES
	 (N'CPU Sockets', LTRIM(STR(cpu_count/hyperthread_ratio)))
	,(N'Max Workers Count', LTRIM(STR(max_workers_count)))
	) AS v(PropertyName, PropertyValue)

-- 1. Server Configurations

	RAISERROR ('Check 1',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Server Configurations', 'Instance', name, CONVERT(nvarchar(max), value_in_use)
	FROM master.sys.configurations

-- 2. Credentials

	RAISERROR ('Check 2',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Server Credentials', 'Instance', F.name, F.credential_identity
	FROM master.sys.credentials F

-- 3. Linked Servers
	
	RAISERROR ('Check 3',0,1) WITH NOWAIT;
	
	INSERT INTO #InstanceProperties

	SELECT 'Linked Servers', F.name, v.*
	FROM master.sys.servers F
	CROSS APPLY
	(VALUES
	 (N'data source', ISNULL(F.data_source, F.provider_string))
	,(N'product', F.product)
	,(N'provider', F.provider)
	,(N'connect_timeout', LTRIM(STR(F.connect_timeout)))
	,(N'query_timeout', LTRIM(STR(F.query_timeout)))
	,(N'is_remote_login_enabled', LTRIM(STR(F.is_remote_login_enabled)))
	,(N'is_rpc_out_enabled', LTRIM(STR(F.is_rpc_out_enabled)))
	,(N'is_data_access_enabled', LTRIM(STR(F.is_data_access_enabled)))
	,(N'is_collation_compatible',LTRIM(STR( F.is_collation_compatible)))
	,(N'uses_remote_collation', LTRIM(STR(F.uses_remote_collation)))
	,(N'collation_name', F.collation_name COLLATE database_default)
	,(N'lazy_schema_validation', LTRIM(STR(F.lazy_schema_validation)))
	,(N'is_system', LTRIM(STR(F.is_system)))
	,(N'is_publisher', LTRIM(STR(F.is_publisher)))
	,(N'is_subscriber', LTRIM(STR(F.is_subscriber)))
	,(N'is_distributor', LTRIM(STR(F.is_distributor)))
	,(N'is_nonsql_subscriber', LTRIM(STR(F.is_nonsql_subscriber)))
	,(N'is_remote_proc_transaction_promotion_enabled', LTRIM(STR(F.is_remote_proc_transaction_promotion_enabled)))
	) AS v(PropertyName, PropertyValue)
	WHERE F.is_linked = 1
	AND PropertyValue IS NOT NULL

-- 4. Database settings that might not transfer
	
	RAISERROR ('Check 4',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Databases', F.name, v.*
	FROM sys.databases F
	CROSS APPLY
	(VALUES
	 (N'state', F.state_desc)
	,(N'collation_name', F.collation_name COLLATE database_default)
	,(N'compatibility_level', LTRIM(STR(F.compatibility_level)))
	,(N'user_access', F.user_access_desc COLLATE database_default)
	,(N'is_read_only', LTRIM(STR(F.is_read_only)))
	,(N'is_auto_close_on', LTRIM(STR(F.is_auto_close_on)))
	,(N'is_auto_shrink_on', LTRIM(STR(F.is_auto_shrink_on)))
	,(N'is_in_standby', LTRIM(STR(F.is_in_standby)))
	,(N'is_cleanly_shutdown', LTRIM(STR(F.is_cleanly_shutdown)))
	,(N'is_supplemental_logging_enabled', LTRIM(STR(F.is_supplemental_logging_enabled)))
	,(N'snapshot_isolation_state', F.snapshot_isolation_state_desc)
	,(N'is_read_committed_snapshot_on', LTRIM(STR(F.is_read_committed_snapshot_on)))
	,(N'recovery_model', F.recovery_model_desc)
	,(N'page_verify_option', F.page_verify_option_desc)
	,(N'is_auto_create_stats_on', LTRIM(STR(F.is_auto_create_stats_on)))
	,(N'is_auto_create_stats_incremental_on', LTRIM(STR(F.is_auto_create_stats_incremental_on)))
	,(N'is_auto_update_stats_on', LTRIM(STR(F.is_auto_update_stats_on)))
	,(N'is_auto_update_stats_async_on', LTRIM(STR(F.is_auto_update_stats_async_on)))
	,(N'is_ansi_null_default_on', LTRIM(STR(F.is_ansi_null_default_on)))
	,(N'is_ansi_nulls_on', LTRIM(STR(F.is_ansi_nulls_on)))
	,(N'is_ansi_padding_on', LTRIM(STR(F.is_ansi_padding_on)))
	,(N'is_ansi_warnings_on', LTRIM(STR(F.is_ansi_warnings_on)))
	,(N'is_arithabort_on', LTRIM(STR(F.is_arithabort_on)))
	,(N'is_concat_null_yields_null_on', LTRIM(STR(F.is_concat_null_yields_null_on)))
	,(N'is_numeric_roundabort_on', LTRIM(STR(F.is_numeric_roundabort_on)))
	,(N'is_quoted_identifier_on', LTRIM(STR(F.is_quoted_identifier_on)))
	,(N'is_recursive_triggers_on', LTRIM(STR(F.is_recursive_triggers_on)))
	,(N'is_cursor_close_on_commit_on', LTRIM(STR(F.is_cursor_close_on_commit_on)))
	,(N'is_local_cursor_default', LTRIM(STR(F.is_local_cursor_default)))
	,(N'is_fulltext_enabled', LTRIM(STR(F.is_fulltext_enabled)))
	,(N'is_trustworthy_on', LTRIM(STR(F.is_trustworthy_on)))
	,(N'is_db_chaining_on', LTRIM(STR(F.is_db_chaining_on)))
	,(N'is_parameterization_forced', LTRIM(STR(F.is_parameterization_forced)))
	,(N'is_master_key_encrypted_by_server', LTRIM(STR(F.is_master_key_encrypted_by_server)))
	,(N'is_query_store_on', LTRIM(STR(F.is_query_store_on)))
	,(N'is_published', LTRIM(STR(F.is_published)))
	,(N'is_subscribed', LTRIM(STR(F.is_subscribed)))
	,(N'is_merge_published', LTRIM(STR(F.is_merge_published)))
	,(N'is_distributor', LTRIM(STR(F.is_distributor)))
	,(N'is_sync_with_backup', LTRIM(STR(F.is_sync_with_backup)))
	,(N'is_broker_enabled', LTRIM(STR(F.is_broker_enabled)))
	,(N'is_date_correlation_on', LTRIM(STR(F.is_date_correlation_on)))
	,(N'is_cdc_enabled', LTRIM(STR(F.is_cdc_enabled)))
	,(N'is_encrypted', LTRIM(STR(F.is_encrypted)))
	,(N'is_honor_broker_priority_on', LTRIM(STR(F.is_honor_broker_priority_on)))
	,(N'replica_id', CONVERT(nvarchar(max),F.replica_id))
	,(N'group_database_id', CONVERT(nvarchar(max),F.group_database_id))
	,(N'containment', F.containment_desc)
	,(N'target_recovery_time_in_seconds', LTRIM(STR(F.target_recovery_time_in_seconds)))
	,(N'delayed_durability', F.delayed_durability_desc)
	,(N'is_memory_optimized_elevate_to_snapshot_on', LTRIM(STR(F.is_memory_optimized_elevate_to_snapshot_on)))
	,(N'is_federation_member', LTRIM(STR(F.is_federation_member)))
	,(N'is_mixed_page_allocation_on', LTRIM(STR(F.is_mixed_page_allocation_on)))
	) AS v(PropertyName, PropertyValue)
	WHERE database_id > 4

-- 5. Messages
	
	RAISERROR ('Check 5',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'User Defined Messages', N'msgid ' + CONVERT(nvarchar(max), F.message_id) + N' (lang ' + CONVERT(nvarchar(max),F.language_id) + N')', V.*
	FROM sys.messages F
	CROSS APPLY
	(VALUES
	 (N'text', F.text COLLATE database_default)
	,(N'severity', LTRIM(STR(F.severity)))
	,(N'is_event_logged', LTRIM(STR(F.is_event_logged)))
	) AS v(PropertyName, PropertyValue)
	WHERE F.message_id > 50000 -- user defined messages only

-- 6. Events

	RAISERROR ('Check 6',0,1) WITH NOWAIT;
	
	INSERT INTO #InstanceProperties
	SELECT 'Server Event Notifications', E.type_desc, N'service_name', F.service_name
	FROM msdb.sys.server_event_notifications F
	JOIN msdb.sys.server_events E
	ON  F.object_id = E.object_id

-- 7. Alerts
	
	RAISERROR ('Check 7',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'SQL Agent Alerts', F.name, V.*
	FROM msdb.dbo.sysalerts F
	CROSS APPLY
	(VALUES
	 (N'message_id', LTRIM(STR(F.message_id)))
	,(N'severity', LTRIM(STR(F.severity)))
	,(N'include_event_description', LTRIM(STR(F.include_event_description)))
	,(N'notification_message', F.notification_message COLLATE database_default)
	,(N'database_name', F.database_name COLLATE database_default)
	,(N'event_description_keyword', F.event_description_keyword COLLATE database_default)
	,(N'has_notification', LTRIM(STR(F.has_notification)))
	,(N'flags', LTRIM(STR(F.flags)))
	,(N'performance_condition', F.performance_condition COLLATE database_default)
	) AS v(PropertyName, PropertyValue)

-- 8. Extended Procedures
	
	RAISERROR ('Check 8',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	EXEC sp_MSforeachdb 'SELECT ''Extended Procedures: ?'', F.name, N''DLL'', F.dll_name
	FROM [?].sys.extended_procedures F'

-- 9. Startup Procedures
	
	RAISERROR ('Check 9',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	EXEC sp_MSforeachdb 'SELECT ''Startup Procedures: ?'', F.name, N''Auto Executed'', ''auto executed''
	FROM [?].sys.procedures F
	WHERE is_auto_executed = 1'

-- 10. FullText
	
	RAISERROR ('Check 10',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	EXEC sp_MSforeachdb 'SELECT ''Full Text Catalogs: ?'', name, N''Status'', status
	FROM [?].sys.sysfulltextcatalogs'

-- 11. Check for missing roles on Second server
	
	RAISERROR ('Check 11',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Server Roles', F.name, N'sid', CONVERT(nvarchar(max), F.sid, 1)
	FROM master.sys.server_principals F
	WHERE F.[type] = 'R'

-- 12. Check for missing logins on Second server
	
	RAISERROR ('Check 12',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Server Logins', F.name, V.*
	FROM master.sys.server_principals F
	CROSS APPLY
	(VALUES
	 (N'sid', CONVERT(nvarchar(max), F.sid, 1))
	,(N'type', F.type_desc COLLATE database_default)
	,(N'is_disabled', LTRIM(STR(F.is_disabled)))
	,(N'default_database', F.default_database_name COLLATE database_default)
	) AS v(PropertyName, PropertyValue)
	WHERE F.[type] <> 'R'
	AND F.name NOT LIKE 'NT SERVICE%'
	AND F.name NOT LIKE 'NT AUTHORITY%'
	AND F.name NOT LIKE '##%##'

-- 13. Database Scoped Configurations
	
	IF OBJECT_ID('sys.database_scoped_configurations') IS NOT NULL
	BEGIN
		RAISERROR ('Check 13',0,1) WITH NOWAIT;

		INSERT INTO #InstanceProperties
		EXEC sp_MSforeachdb N'select N''Databases'', ''?'', name, CONVERT(nvarchar(max), value) from [?].sys.database_scoped_configurations'
	END

-- 14. Check logins have the same server roles. 
	
	RAISERROR ('Check 14',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT	'Server Logins: Server Role Memberships', PRN.name, srvrole.name, N'Member'
	FROM sys.server_role_members membership 
	INNER JOIN (SELECT * FROM sys.server_principals  WHERE type_desc='SERVER_ROLE') srvrole 
	ON srvrole.Principal_id= membership.Role_principal_id 
	INNER JOIN sys.server_principals  PRN 
	ON PRN.Principal_id= membership.member_principal_id
	WHERE PRN.Type_Desc NOT IN ('SERVER_ROLE')
	AND PRN.name NOT LIKE 'NT SERVICE%'
	AND PRN.name NOT LIKE 'NT AUTHORITY%'
	AND PRN.name NOT LIKE '##%##'
	

-- 15. Server Level Permissions

	RAISERROR ('Check 15',0,1) WITH NOWAIT;
	
	INSERT INTO #InstanceProperties
	SELECT 'Server Logins: Server Level Permissions'
	 , pr.name COLLATE database_default
	 , pe.permission_name COLLATE database_default + N' ' + pe.class_desc COLLATE database_default + N' ' + CONVERT(nvarchar(max), pe.major_id) + N'.' + CONVERT(nvarchar(max), pe.minor_id)
	 , pe.state_desc COLLATE database_default
	FROM master.sys.server_principals AS pr 
	JOIN master.sys.server_permissions AS pe 
	ON pe.grantee_principal_id = pr.principal_id
	WHERE pr.name NOT LIKE 'NT SERVICE%'
	AND pr.name NOT LIKE 'NT AUTHORITY%'
	AND pr.name NOT LIKE '##%##'


-- 16. Database Permissions

	RAISERROR ('Check 16',0,1) WITH NOWAIT;
	
	INSERT INTO #InstanceProperties
	EXEC sp_MSforeachdb 'SELECT	''Database Roles: ?'', dpm.name AS MemberName, dpr.name ASRoleName, N''Member''
	FROM  [?].sys.database_role_members dr
	JOIN [?].sys.database_principals dpm
	ON dpm.principal_id = dr.member_principal_id
	JOIN [?].sys.database_principals dpr
	on dpr.principal_id = dr.role_principal_id 
	WHERE dpm.type_desc LIKE ''%USER%'''

-- 17. System Objects Permissions
	
	RAISERROR ('Check 17',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'System Objects Permissions'
		, p.name COLLATE database_default
		, dp.[permission_name] COLLATE database_default + ' ON ' + so.name COLLATE database_default + ' TO ' + p.type_desc COLLATE database_default+ ': ' + p.name COLLATE database_default
		, dp.state_desc COLLATE database_default
	FROM master.sys.database_permissions AS dp 
	JOIN master.sys.system_objects AS so
	ON dp.major_id = so.object_id
	JOIN master.sys.database_principals p
	ON dp.grantee_principal_id = p.principal_id
	WHERE dp.class = 1 AND so.parent_object_id = 0 	

-- 18. SQL Agent Operators
	
	RAISERROR ('Check 18',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Operators', F.name, V.*
	FROM msdb.dbo.sysoperators F
	CROSS APPLY
	(VALUES
	 (N'enabled', LTRIM(STR(F.enabled)))
	,(N'weekday_pager_start_time', LTRIM(STR(F.weekday_pager_start_time)))
	,(N'weekday_pager_end_time', LTRIM(STR(F.weekday_pager_end_time)))
	,(N'saturday_pager_start_time', LTRIM(STR(F.saturday_pager_start_time)))
	,(N'saturday_pager_end_time', LTRIM(STR(F.saturday_pager_end_time)))
	,(N'sunday_pager_start_time', LTRIM(STR(F.sunday_pager_start_time)))
	,(N'sunday_pager_end_time', LTRIM(STR(F.sunday_pager_end_time)))
	,(N'pager_days', LTRIM(STR(F.pager_days)))
	,(N'email_address', F.email_address COLLATE database_default)
	) AS v(PropertyName, PropertyValue)

-- 19. Server Triggers
	
	RAISERROR ('Check 19',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Server Triggers', F.name, V.*
	FROM master.sys.server_triggers F
	CROSS APPLY
	(VALUES
	 (N'is_disabled', LTRIM(STR(F.is_disabled)))
	,(N'type', F.type_desc COLLATE database_default)
	) AS v(PropertyName, PropertyValue)

-- 20. DB Owner
	
	RAISERROR ('Check 20',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Databases', db.name, 'owner', sp.name
	FROM sys.databases AS db
	INNER JOIN sys.server_principals AS sp
	ON db.owner_sid = sp.sid

-- 21. SQL Agent Jobs
	
	RAISERROR ('Check 21',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Jobs', F.name, V.*
	FROM msdb.dbo.sysjobs F
	INNER JOIN sys.server_principals AS sp
	ON F.owner_sid = sp.sid
	LEFT JOIN msdb..sysoperators AS op1
	ON F.notify_email_operator_id = op1.id
	LEFT JOIN msdb..sysoperators AS op2
	ON F.notify_netsend_operator_id = op2.id
	LEFT JOIN msdb..sysoperators AS op3
	ON F.notify_page_operator_id = op3.id
	CROSS APPLY
	(VALUES
	 (N'enabled', LTRIM(STR(F.enabled)))
	,(N'start_step_id', LTRIM(STR(F.start_step_id)))
	,(N'notify_level_eventlog', LTRIM(STR(F.notify_level_eventlog)))
	,(N'notify_level_email', LTRIM(STR(F.notify_level_email)))
	,(N'notify_level_netsend', LTRIM(STR(F.notify_level_netsend)))
	,(N'notify_level_page', LTRIM(STR(F.notify_level_page)))
	,(N'delete_level', LTRIM(STR(F.delete_level)))
	,(N'notify_email_operator', op1.name COLLATE database_default)
	,(N'notify_netsend_operator', op2.name COLLATE database_default)
	,(N'notify_page_operator', op3.name COLLATE database_default)
	,(N'owner', sp.name COLLATE database_default)
	) AS v(PropertyName, PropertyValue)

-- 22. SQL Agent Job Steps

	RAISERROR ('Check 22',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Job Steps: ' + FJ.name, F.step_name, V.*
	FROM msdb.dbo.sysjobs FJ
	JOIN msdb.dbo.sysjobsteps F
	ON FJ.job_id = F.job_id
	CROSS APPLY
	(VALUES
	 (N'subsystem', F.subsystem COLLATE database_default)
	,(N'flags', LTRIM(STR(F.flags)))
	,(N'cmdexec_success_code', LTRIM(STR(F.cmdexec_success_code)))
	,(N'on_success_action', LTRIM(STR(F.on_success_action)))
	,(N'on_success_step_id', LTRIM(STR(F.on_success_step_id)))
	,(N'on_fail_action', LTRIM(STR(F.on_fail_action)))
	,(N'on_fail_step_id', LTRIM(STR(F.on_fail_step_id)))
	,(N'server', [server] COLLATE database_default)
	,(N'database_name', F.database_name COLLATE database_default)
	,(N'database_user_name', F.database_user_name COLLATE database_default)
	,(N'retry_attempts', LTRIM(STR(F.retry_attempts)))
	,(N'retry_interval', LTRIM(STR(F.retry_interval)))
	,(N'os_run_priority', LTRIM(STR(F.os_run_priority)))
	,(N'output_file_name', F.output_file_name COLLATE database_default)
	,(N'command (checksum)', CONVERT(nvarchar(max), CHECKSUM(F.command)))
	) AS v(PropertyName, PropertyValue)

-- 23. Server Endpoints

	RAISERROR ('Check 23',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT N'Endpoints', name, V.*
	FROM sys.endpoints F
	CROSS APPLY (
			SELECT PropertyName, PropertyValue
			FROM 
			(VALUES
			 ('state',state_desc COLLATE database_default)
			,('type',type_desc COLLATE database_default)
			,('protocol',protocol_desc COLLATE database_default)
			) as v(PropertyName, PropertyValue)
			WHERE v.PropertyValue IS NOT NULL
			
			UNION ALL

			SELECT PropertyName, PropertyValue
			FROM sys.http_endpoints AS HTTPE
			CROSS APPLY
			(VALUES
			 ('url_path',url_path COLLATE database_default)
			,('is_clear_port_enabled',LTRIM(STR(is_clear_port_enabled)))
			,('clear_port',LTRIM(STR(clear_port)))
			,('is_ssl_port_enabled',LTRIM(STR(is_ssl_port_enabled)))
			,('ssl_port',LTRIM(STR(ssl_port)))
			,('is_anonymous_enabled',LTRIM(STR(is_anonymous_enabled)))
			,('is_basic_auth_enabled',LTRIM(STR(is_basic_auth_enabled)))
			,('is_digest_auth_enabled',LTRIM(STR(is_digest_auth_enabled)))
			,('is_kerberos_auth_enabled',LTRIM(STR(is_kerberos_auth_enabled)))
			,('is_ntlm_auth_enabled',LTRIM(STR(is_ntlm_auth_enabled)))
			,('is_integrated_auth_enabled',LTRIM(STR(is_integrated_auth_enabled)))
			,('authorization_realm',authorization_realm COLLATE database_default)
			,('default_logon_domain',default_logon_domain COLLATE database_default)
			,('is_compression_enabled',LTRIM(STR(is_compression_enabled)))
			) as v(PropertyName, PropertyValue)
			WHERE HTTPE.endpoint_id = F.endpoint_id
			AND (F.protocol_desc = 'HTTP' OR F.type_desc = 'SOAP')
			AND v.PropertyValue IS NOT NULL

			UNION ALL
			
			SELECT PropertyName, PropertyValue
			FROM sys.tcp_endpoints TCPE
			CROSS APPLY
			(VALUES
			 ('port',LTRIM(STR(port)))
			,('is_dynamic_port',LTRIM(STR(is_dynamic_port)))
			,('ip_address',ip_address)
			,('is_admin_endpoint',LTRIM(STR(is_admin_endpoint)))
			) as v(PropertyName, PropertyValue)
			WHERE TCPE.endpoint_id = F.endpoint_id
			AND F.protocol_desc = 'TCP'
			AND v.PropertyValue IS NOT NULL
			
			UNION ALL
			
			SELECT PropertyName, PropertyValue
			FROM sys.database_mirroring_endpoints DBME
			CROSS APPLY
			(VALUES
			 ('role',role_desc)
			,('is_encryption_enabled',LTRIM(STR(is_encryption_enabled)))
			,('connection_auth',connection_auth_desc)
			,('encryption_algorithm',encryption_algorithm_desc)
			) as v(PropertyName, PropertyValue)
			WHERE DBME.endpoint_id = F.endpoint_id
			AND F.type_desc = 'DATABASE_MIRRORING'
			AND v.PropertyValue IS NOT NULL
			
			UNION ALL

			SELECT PropertyName, PropertyValue
			FROM sys.service_broker_endpoints AS SBE
			CROSS APPLY
			(VALUES
			('is_message_forwarding_enabled',LTRIM(STR(is_message_forwarding_enabled)))
			,('message_forwarding_size',LTRIM(STR(message_forwarding_size)))
			,('connection_auth',connection_auth_desc COLLATE database_default)
			,('encryption_algorithm',encryption_algorithm_desc COLLATE database_default)
			) as v(PropertyName, PropertyValue)
			WHERE SBE.endpoint_id = F.endpoint_id
			AND F.type_desc = 'SERVICE_BROKER'
			AND v.PropertyValue IS NOT NULL

		) AS V(PropertyName, PropertyValue)


-- 24. Resource Governor

	RAISERROR ('Check 24',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT 'Resource Governor' AS [Category], N'Instance', 'Classifier Function', CASE WHEN classifier_function_id = 0 THEN 'Default_Configuration' ELSE OBJECT_SCHEMA_NAME(classifier_function_id) + '.' + OBJECT_NAME(classifier_function_id) END
	FROM sys.dm_resource_governor_configuration AS rgc

	UNION ALL
	
	SELECT 'Resource Governor Pools' AS [Category], name, v.*
	FROM sys.dm_resource_governor_resource_pools rp
	CROSS APPLY
	(VALUES
	 (N'max_memory_kb', LTRIM(STR(max_memory_kb)))
	,(N'min_cpu_percent', LTRIM(STR(min_cpu_percent)))
	,(N'max_cpu_percent', LTRIM(STR(max_cpu_percent)))
	,(N'min_memory_percent', LTRIM(STR(min_memory_percent)))
	,(N'max_memory_percent', LTRIM(STR(max_memory_percent)))
	) AS v(PropertyName, PropertyValue)


-- 25. Database Files

	RAISERROR ('Check 25',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	SELECT N'Database Files: ' + DB_NAME([database_id]), [file_id], V.*
	FROM sys.master_files (NOLOCK)
	CROSS APPLY
	(VALUES
	 (N'name', name COLLATE database_default)
	,(N'type_desc', type_desc COLLATE database_default)
	,(N'state_desc', state_desc COLLATE database_default)
	,(N'is_percent_growth', LTRIM(STR(is_percent_growth)))
	,(N'growth', LTRIM(STR(growth)))
	,(N'is_media_read_only', LTRIM(STR(is_media_read_only)))
	,(N'is_read_only', LTRIM(STR(is_read_only)))
	,(N'is_sparse', LTRIM(STR(is_sparse)))
	,(N'is_name_reserved', LTRIM(STR(is_name_reserved)))
	) AS v(PropertyName, PropertyValue)

-- 26. Database Features Usage

	RAISERROR ('Check 26',0,1) WITH NOWAIT;

	INSERT INTO #InstanceProperties
	EXEC sp_MSforeachdb '
SELECT N''Feature Usage'', ''?'' AS [dbname], feature_name, N''In use'' FROM [?].sys.dm_db_persisted_sku_features (NOLOCK)
UNION ALL
SELECT N''Feature Usage'', ''?'' AS [dbname], ''Change_Tracking'', N''In use'' FROM sys.change_tracking_databases (NOLOCK) WHERE database_id = DB_ID(''?'')
UNION ALL
SELECT TOP 1 N''Feature Usage'', ''?'' AS [dbname], ''Fine_grained_auditing'', N''In use'' FROM [?].sys.database_audit_specifications (NOLOCK)
UNION ALL
SELECT TOP 1 N''Feature Usage'', ''?'' AS [dbname], ''Polybase'', N''In use'' FROM [?].sys.external_data_sources (NOLOCK)
UNION ALL
SELECT TOP 1 N''Feature Usage'', ''?'' AS [dbname], ''Row_Level_Security'', N''In use'' FROM [?].sys.security_policies (NOLOCK)
UNION ALL
SELECT TOP 1 N''Feature Usage'', ''?'' AS [dbname], ''Always_Encrypted'', N''In use'' FROM [?].sys.column_master_keys (NOLOCK)
UNION ALL
SELECT TOP 1 N''Feature Usage'', ''?'' AS [dbname], ''Dynamic_Data_Masking'', N''In use'' FROM [?].sys.masked_columns (NOLOCK) WHERE is_masked = 1'

	INSERT INTO #InstanceProperties
	SELECT DISTINCT N'Feature Usage', [name], 'DB Snapshot' AS feature_name, N'In Use' FROM master.sys.databases (NOLOCK) WHERE database_id NOT IN (2,3) AND source_database_id IS NOT NULL
	UNION ALL
	SELECT DISTINCT N'Feature Usage', DB_NAME(database_id), 'Filestream' AS feature_name, N'In Use' FROM sys.master_files (NOLOCK) WHERE database_id NOT IN (2,3) AND [type] = 2 and file_guid IS NOT NULL

-- Return all
SELECT ServerName = CONVERT(varchar(300), SERVERPROPERTY('ServerName')), Category = CONVERT(varchar(100), Category), ItemName = CONVERT(varchar(500), ItemName), PropertyName = CONVERT(varchar(500), PropertyName), PropertyValue = CONVERT(varchar(8000), PropertyValue)
FROM #InstanceProperties
ORDER BY 2, 3, 4

IF OBJECT_ID('tempdb..#InstanceProperties') IS NOT NULL DROP TABLE #InstanceProperties;
