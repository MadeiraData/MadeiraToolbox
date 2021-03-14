IF CAST(SERVERPROPERTY('Edition') AS VARCHAR(255)) NOT LIKE '%Azure%'
BEGIN
	PRINT 'Checking: Instant File Initialization';
	DECLARE @ifi bit
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
		PRINT N'Instant File Initialization is enabled'
		SET @ifi = 1;
	END
	ELSE
	BEGIN
		SELECT 'Performance' AS [Category], 'Instant File Initialization' AS [Check], 'Instant File Initialization is disabled.' AS ObjectName, 'This can negatively impact data file autogrowth times' AS [Deviation];
		SET @ifi = 0
	END
END
ELSE
	PRINT N'Instant File Initialization is irrelevant for Azure SQL databases'