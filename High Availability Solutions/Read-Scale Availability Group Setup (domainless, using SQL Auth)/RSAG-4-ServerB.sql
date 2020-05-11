
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

PRINT N'Creating login $(AGLoginName)...'

IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = '$(AGLoginName)')
	CREATE LOGIN
		[$(AGLoginName)]
	WITH PASSWORD = '$(AGLoginPassword)';
ELSE
	PRINT N'Login $(AGLoginName) already exists.'
GO

PRINT N'Creating database user for $(AGLoginName)...'

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = '$(AGLoginName)')
	CREATE USER
		[$(AGLoginName)]
	FOR LOGIN
		[$(AGLoginName)];
ELSE
	PRINT N'Database user $(AGLoginName) already exists.'
GO

PRINT N'Import certificate $(ServerA_AGCertificateName)...'

IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name ='$(ServerA_AGCertificateName)')
	CREATE CERTIFICATE
		[$(ServerA_AGCertificateName)]
	AUTHORIZATION [$(AGLoginName)]
	FROM FILE = '$(ServerB_LocalBackupPath)\$(ServerA_AGCertificateName).cert';
ELSE
	PRINT N'Certificate $(ServerB_AGCertificateName) already exists.'
GO

PRINT N'Grant the CONNECT permission to the login...'

GRANT CONNECT
	ON ENDPOINT::$(AGEndPointName) 
	TO [$(AGLoginName)];
