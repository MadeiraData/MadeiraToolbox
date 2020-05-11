
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
-- Execute the following code on Server A
-- ======================================
:CONNECT $(ServerA) -U $(SysAdminLoginName) -P $(SysAdminLoginPassword)

USE master
GO

PRINT N'Creating Database Master Key...';

IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
	CREATE MASTER KEY
	ENCRYPTION BY
		PASSWORD = '$(ServerA_DBMasterKeyPassword)';

	PRINT N'Backing up Database Master Key to $(ServerA_LocalBackupPath)\DMK_$(ServerA).bak...'

	BACKUP MASTER KEY 
		TO FILE = '$(ServerA_LocalBackupPath)\DMK_$(ServerA).bak'
		ENCRYPTION BY PASSWORD = '$(ServerA_DBMasterKeyPassword)';
END
ELSE
	PRINT N'Database Master Key already exists.'
GO

PRINT N'Creating certificate $(ServerA_AGCertificateName)...'

IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = '$(ServerA_AGCertificateName)')
BEGIN
	CREATE CERTIFICATE
		[$(ServerA_AGCertificateName)]
	WITH
		SUBJECT = 'HADR - $(ServerA_AGCertificateName)';

	-- Backup the public key of the certificate to the filesystem
	PRINT N'Backup certificate to $(ServerA_LocalBackupPath)\$(ServerA_AGCertificateName).cert'

	BACKUP CERTIFICATE
		[$(ServerA_AGCertificateName)]
	TO FILE = '$(ServerA_LocalBackupPath)\$(ServerA_AGCertificateName).cert'
END
ELSE
	PRINT N'Certificate $(ServerA_AGCertificateName) already exists.'
GO

PRINT N'Making sure AlwaysOn_Health extended event session is on...'

IF EXISTS(SELECT * FROM sys.server_event_sessions WHERE name='AlwaysOn_health')
	ALTER EVENT SESSION
		[AlwaysOn_health]
	ON SERVER
	WITH
		(STARTUP_STATE = ON);

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
		AUTHENTICATION = CERTIFICATE [$(ServerA_AGCertificateName)],
		ROLE = ALL, 
		ENCRYPTION = REQUIRED ALGORITHM AES
	)
ELSE
	PRINT N'Endpoint $(AGEndPointName) already exists.'
GO
