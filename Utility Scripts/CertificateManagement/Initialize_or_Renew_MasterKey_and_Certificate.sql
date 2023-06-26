:setvar CertificateName DEK_Certificate
:setvar MasterKeyPassword paste_password_here
:setvar CertificatePassword paste_password_here
:setvar BackupFolderPath c:\temp\
:setvar NewExpiryDate 29991231
:setvar IsSqlCMDOn yes
GO
SET NOEXEC OFF;
GO
IF IS_SRVROLEMEMBER('sysadmin') = 0
BEGIN
	RAISERROR(N'Login must have sysadmin permissions to run this script!',16,1);
	SET NOEXEC ON;
END
GO
IF '$(IsSqlCMDOn)' <> 'yes'
BEGIN
	RAISERROR(N'This script must be run in SQLCMD mode!',16,1);
	SET NOEXEC ON;
END
GO
USE [master]
GO
BEGIN TRY
	EXEC xp_create_subdir '$(BackupFolderPath)'
	PRINT N'Created folder: $(BackupFolderPath)'
END TRY
BEGIN CATCH
	PRINT ERROR_MESSAGE()
END CATCH
GO
DECLARE @ToDate VARCHAR(10), @MasterKeyFromDate VARCHAR(10), @CertificateFromDate VARCHAR(10), @PKeyFromDate VARCHAR(10)

SET @ToDate = CONVERT(nvarchar(10), GETDATE(), 112);
	
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
	RAISERROR(N'Creating NEW Master Key...',0,1) WITH NOWAIT;
	CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)';
END

select @MasterKeyFromDate = CONVERT(nvarchar(10), modify_date, 112)
from sys.symmetric_keys
WHERE [name] = '##MS_DatabaseMasterKey##'

SELECT @CertificateFromDate = CONVERT(nvarchar(10), [start_date], 112), @PKeyFromDate = CONVERT(nvarchar(10), pvt_key_last_backup_date, 112)
FROM sys.certificates
WHERE [name] = '$(CertificateName)'

SELECT @MasterKeyFromDate AS [@MasterKeyFromDate], @CertificateFromDate AS [@CertificateFromDate], @PKeyFromDate AS [@PKeyFromDate], @ToDate AS [@ToDate]

DECLARE @CMD NVARCHAR(MAX), @Path NVARCHAR(4000), @Path2 NVARCHAR(4000)
SET @Path = '$(BackupFolderPath)Master_Key_' + @MasterKeyFromDate + '_' + @ToDate + '.key'

RAISERROR(N'Backing up master key to: %s',0,1,@Path) WITH NOWAIT;
	
SET @CMD = N'BACKUP MASTER KEY TO FILE = ' + QUOTENAME(@Path, '''') + N'
	ENCRYPTION BY PASSWORD = ''$(MasterKeyPassword)'';  '

EXEC (@CMD);
	
RAISERROR(N'Regenerating master key...',0,1) WITH NOWAIT;

ALTER MASTER KEY REGENERATE WITH ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)';

SET @Path = '$(BackupFolderPath)Master_Key_' + @ToDate + '_NEW.key'

RAISERROR(N'Backing up NEW master key to: %s',0,1,@Path) WITH NOWAIT;
	
SET @CMD = N'BACKUP MASTER KEY TO FILE = ' + QUOTENAME(@Path, '''') + N'
	ENCRYPTION BY PASSWORD = ''$(MasterKeyPassword)'';  '

EXEC (@CMD);
	
SET @Path = '$(BackupFolderPath)$(CertificateName)_' + @ToDate + '_NEW.cer'
SET @Path2 = '$(BackupFolderPath)$(CertificateName)_' + @ToDate + '_NEW.pkey'
	
IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = '$(CertificateName)')
BEGIN
	RAISERROR(N'Creating NEW Certificate...',0,1) WITH NOWAIT;
	CREATE CERTIFICATE [$(CertificateName)]   
		WITH SUBJECT = 'Database Encryption Certificate',   
		EXPIRY_DATE = '$(NewExpiryDate)';
END

RAISERROR(N'Backing up NEW certificate to: %s',0,1,@Path) WITH NOWAIT;
RAISERROR(N'Backing up NEW certificate private key to: %s',0,1,@Path2) WITH NOWAIT;
	
SET @CMD = N'BACKUP CERTIFICATE [$(CertificateName)] TO FILE = ' + QUOTENAME(@Path, '''') + N'
	WITH PRIVATE KEY ( 
	FILE = ' + QUOTENAME(@Path2, '''') + N' ,   
	ENCRYPTION BY PASSWORD = ''$(CertificatePassword)'' );  '

EXEC (@CMD); 

PRINT N'Done.'
GO
EXEC xp_dirtree '$(BackupFolderPath)', 1, 1
