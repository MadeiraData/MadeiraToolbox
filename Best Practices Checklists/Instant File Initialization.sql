DECLARE @ifi bit
SET @ifi = 0

IF CAST(SERVERPROPERTY('Edition') AS VARCHAR(255)) NOT LIKE '%Azure%'
BEGIN
	IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_server_services') AND name = 'instant_file_initialization_enabled')
	BEGIN
		DECLARE @cmd nvarchar(max)
		SET @cmd = N'SELECT @ifi = 1 FROM sys.dm_server_services WHERE instant_file_initialization_enabled = ''Y'''
		EXEC sp_executesql @cmd, N'@ifi bit OUTPUT', @ifi OUTPUT
	END
	ELSE
	BEGIN
		PRINT 'Checking: Instant File Initialization using xp_cmdshell whoami /priv';
		DECLARE @xp_cmdshell_output2 TABLE ([Output] VARCHAR (8000));

		DECLARE @CmdShellOrigValue INT, @AdvancedOptOrigValue INT
		SELECT @CmdShellOrigValue = CONVERT(int, value_in_use) FROM sys.configurations WHERE name = 'xp_cmdshell';

		IF @CmdShellOrigValue = 0
		BEGIN
			PRINT N'temporarily activating xp_cmdshell...'
			SELECT @AdvancedOptOrigValue = CONVERT(int, value_in_use) FROM sys.configurations WHERE name = 'show advanced options';

			IF @AdvancedOptOrigValue = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 1;
				RECONFIGURE;
			END

			EXEC sp_configure 'xp_cmdshell', 1;
			RECONFIGURE;
		END

		INSERT INTO @xp_cmdshell_output2
		EXEC master.dbo.xp_cmdshell 'whoami /priv';

		IF @CmdShellOrigValue = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 0;
			RECONFIGURE;

			IF @AdvancedOptOrigValue = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 0;
				RECONFIGURE;
			END
		END

		IF EXISTS (SELECT * FROM @xp_cmdshell_output2 WHERE [Output] LIKE '%SeManageVolumePrivilege%')
		BEGIN
			SET @ifi = 1;
		END
		ELSE
		BEGIN
			SET @ifi = 0
		END
	END
END
ELSE
BEGIN
	--PRINT N'Instant File Initialization is irrelevant for Azure SQL databases'
	SET @ifi = 1;
END
	
IF @ifi = 1
BEGIN
	PRINT N'Instant File Initialization is enabled'
	SET @ifi = 1;
END
ELSE
BEGIN
	SELECT 'Performance' AS [Category], 'Instant File Initialization' AS [Check], 'Instant File Initialization is disabled.' AS ObjectName, 'This can negatively impact data file autogrowth times' AS [Deviation];
END