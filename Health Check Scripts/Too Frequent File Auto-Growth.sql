SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @temp_growth AS TABLE
(
    DatabaseName sysname,
    filename sysname,
    filetype nvarchar(60) null,
    starttime datetime 
);
DECLARE @ifi bit;
DECLARE @ThresholdGrowthsWithIFI int;

SET @ThresholdGrowthsWithIFI = NULL -- NULL: unlimited

IF (SELECT sqlserver_start_time FROM sys.dm_os_sys_info) < DATEADD(DAY, -7, GETDATE())
BEGIN
 
 IF CAST(SERVERPROPERTY('Edition') AS VARCHAR(255)) NOT LIKE '%Azure%'
 BEGIN
 	IF EXISTS (SELECT * FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.dm_server_services') AND name = 'instant_file_initialization_enabled')
 	BEGIN
 		DECLARE @cmd nvarchar(max)
 		SET @cmd = N'SELECT @ifi = 1 FROM sys.dm_server_services WHERE instant_file_initialization_enabled = ''Y'''
 		BEGIN TRY
 			EXEC sp_executesql @cmd, N'@ifi bit OUTPUT', @ifi OUTPUT
 			SET @ifi = ISNULL(@ifi, 0)
 		END TRY
 		BEGIN CATCH
 			PRINT ERROR_MESSAGE()
 		END CATCH
 	END
 	
 	IF @ifi IS NULL AND IS_SRVROLEMEMBER('sysadmin') = 1
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
 	ELSE
 	BEGIN
 		PRINT N'Insufficient permissions to determine whether IFI is enabled.'
 		SET @ifi = 0
 	END
 END
 ELSE
 BEGIN
 	--PRINT N'Instant File Initialization is irrelevant for Azure SQL databases'
 	SET @ifi = 1;
 END

 DECLARE @path NVARCHAR(260)
 SELECT @path=path FROM sys.traces WHERE is_default = 1
 
 INSERT INTO @temp_growth
 SELECT ISNULL(d.[name], sft.DatabaseName), sft.FileName, f.type_desc, sft.StartTime
 FROM sys.fn_trace_gettable(@path, DEFAULT) sft
 INNER JOIN sys.databases d ON sft.DatabaseID = d.database_id
 INNER JOIN sys.master_files AS f ON f.database_id = sft.DatabaseID AND f.name = sft.FileName
 WHERE 1=1
 AND sft.EventClass IN (92,93)
 AND sft.StartTime > DATEADD(minute,-61,GETDATE())
 AND d.create_date < DATEADD(day, -3, GETDATE()) --- ignore newly created databases
 AND d.source_database_id IS NULL
 ORDER BY sft.DatabaseName, sft.FileName, f.type_desc, sft.StartTime DESC

END

select 'In server: ' + @@SERVERNAME + ' database: ' + QUOTENAME(DatabaseName) + ISNULL(filetype, N'') + ' File: ' + QUOTENAME(filename) + ' auto-grew ' + cast(count(*) as varchar) + ' times during the last hour'
, count(*)
from @temp_growth
group by DatabaseName, filename, filetype
-- don't alert about data file autogrowth unless IFI is disabled or the max threshold is reached
HAVING @ifi = 0 OR filetype = 'LOG' OR COUNT(*) >= @ThresholdGrowthsWithIFI
