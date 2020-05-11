
/****** YOU MUST EXECUTE THE FOLLOWING SCRIPT IN SQLCMD MODE ******/
:setvar path "C:\agTemp"
:r $(path)\RSAG-SQLCMD-Header.sql
GO
:on error exit
GO
/*
Detect SQLCMD mode and disable script execution if SQLCMD mode is not supported.
To re-enable the script after enabling SQLCMD mode, execute the following:
SET NOEXEC OFF; 
*/
:setvar __IsSqlCmdEnabled "True"
GO
IF N'$(__IsSqlCmdEnabled)' NOT LIKE N'True'
    BEGIN
        PRINT N'SQLCMD mode must be enabled to successfully execute this script.';
        SET NOEXEC ON;
    END

GO
-- ======================================
-- Execute the following code on Server B
-- ======================================
:CONNECT $(ServerB) -U $(SysAdminLoginName) -P $(SysAdminLoginPassword)


USE master
GO

PRINT N'Creating Database Master Key...';

IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
	CREATE MASTER KEY
	ENCRYPTION BY
		PASSWORD = '$(ServerB_DBMasterKeyPassword)';

	PRINT N'Backing up Database Master Key to $(ServerB_LocalBackupPath)\DMK_$(ServerB).bak...'

	BACKUP MASTER KEY 
		TO FILE = '$(ServerB_LocalBackupPath)\DMK_$(ServerB).bak'
		ENCRYPTION BY PASSWORD = '$(ServerB_DBMasterKeyPassword)';
END
ELSE
	PRINT N'Database Master Key already exists.'
GO

PRINT N'Creating certificate $(ServerB_AGCertificateName)...'

IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = '$(ServerB_AGCertificateName)')
BEGIN
	CREATE CERTIFICATE
		[$(ServerB_AGCertificateName)]
	WITH
		SUBJECT = 'HADR - $(ServerB_AGCertificateName)';

	PRINT N'Backup certificate to $(ServerB_LocalBackupPath)\$(ServerB_AGCertificateName).cert'

	BACKUP CERTIFICATE
		[$(ServerB_AGCertificateName)]
	TO FILE = '$(ServerB_LocalBackupPath)\$(ServerB_AGCertificateName).cert'
END
ELSE
	PRINT N'Certificate $(ServerB_AGCertificateName) already exists.'
GO

PRINT N'Making sure AlwaysOn_Health extended event session is on...'

IF EXISTS(SELECT * FROM sys.server_event_sessions WHERE name='AlwaysOn_health')
	ALTER EVENT SESSION
		[AlwaysOn_health]
	ON SERVER
	WITH (STARTUP_STATE = ON);

IF NOT EXISTS(SELECT * FROM sys.dm_xe_sessions WHERE name='AlwaysOn_health')
	ALTER EVENT SESSION
		[AlwaysOn_health]
	ON SERVER
	STATE = START;
GO

PRINT N'Creating endpoint $(AGEndPointName) for the Availability Group...'

IF NOT EXISTS (SELECT * FROM sys.database_mirroring_endpoints WHERE name = '$(AGEndPointName)')
	CREATE ENDPOINT
		[$(AGEndPointName)]
	STATE = STARTED
	AS TCP
	(
		LISTENER_PORT = $(AGEndPointPort)
	)
	FOR DATABASE_MIRRORING
	(
		AUTHENTICATION = CERTIFICATE [$(ServerB_AGCertificateName)],
		ROLE = ALL, 
		ENCRYPTION = REQUIRED ALGORITHM AES
	)
ELSE
	PRINT N'Endpoint $(AGEndPointName) already exists.'
GO
PRINT N'============================================================================================'
PRINT N'Phase 1 complete. Now you need to copy the certificates between the two servers.'
PRINT N'Copy $(ServerA_LocalBackupPath)\$(ServerA_AGCertificateName).cert from PRIMARY to $(ServerB_LocalBackupPath)\$(ServerA_AGCertificateName).cert on SECONDARY.'
PRINT N'Copy $(ServerB_LocalBackupPath)\$(ServerB_AGCertificateName).cert from SECONDARY to $(ServerA_LocalBackupPath)\$(ServerB_AGCertificateName).cert on PRIMARY.'
