:setvar CertificateName DEK_Certificate
:setvar CertificateDescription "Database Certificate"
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
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
DECLARE @ToDate VARCHAR(10), @CertificateFromDate VARCHAR(10), @PKeyFromDate VARCHAR(10)

SET @ToDate = CONVERT(nvarchar(10), GETDATE(), 112);

IF NOT EXISTS (SELECT * FROM sys.certificates WHERE name = '$(CertificateName)')
BEGIN
	RAISERROR(N'Creating NEW Certificate...',0,1) WITH NOWAIT;
	CREATE CERTIFICATE [$(CertificateName)]   
		WITH SUBJECT = 'Database Encryption Certificate',   
		EXPIRY_DATE = '$(NewExpiryDate)';
END

SELECT @CertificateFromDate = CONVERT(nvarchar(10), [start_date], 112), @PKeyFromDate = CONVERT(nvarchar(10), pvt_key_last_backup_date, 112)
FROM sys.certificates
WHERE [name] = '$(CertificateName)'

SELECT @CertificateFromDate AS [@CertificateFromDate], @PKeyFromDate AS [@PKeyFromDate], @ToDate AS [@ToDate], '$(NewExpiryDate)' AS [NewExpiryDate]

RAISERROR(N'Opening master key...',0,1) WITH NOWAIT;
OPEN MASTER KEY DECRYPTION BY PASSWORD = '$(MasterKeyPassword)';

DECLARE @CMD NVARCHAR(MAX), @Path NVARCHAR(4000), @Path2 NVARCHAR(4000)

SET @Path = '$(BackupFolderPath)$(CertificateName)_' + @CertificateFromDate + '_' + @ToDate + '.cer'
SET @Path2 = '$(BackupFolderPath)$(CertificateName)_' + @PKeyFromDate + '_' + @ToDate + '.pkey'

RAISERROR(N'Backing up certificate to: %s',0,1,@Path) WITH NOWAIT;
RAISERROR(N'Backing up certificate private key to: %s',0,1,@Path2) WITH NOWAIT;

SET @CMD = N'BACKUP CERTIFICATE [$(CertificateName)] TO FILE = ' + QUOTENAME(@Path, '''') + N'
    WITH PRIVATE KEY ( 
    FILE = ' + QUOTENAME(@Path2, '''') + N' ,   
    ENCRYPTION BY PASSWORD = ''$(CertificatePassword)'' );  '

EXEC (@CMD);
	
RAISERROR(N'Dropping old certificate...',0,1) WITH NOWAIT;
DROP CERTIFICATE [$(CertificateName)];

RAISERROR(N'Creating new certificate...',0,1) WITH NOWAIT;

CREATE CERTIFICATE [$(CertificateName)]   
   WITH SUBJECT = '$(CertificateDescription)',   
   EXPIRY_DATE = '$(NewExpiryDate)'; 
   
   
SET @Path = '$(BackupFolderPath)$(CertificateName)_' + @ToDate + '_$(NewExpiryDate).cer'
SET @Path2 = '$(BackupFolderPath)$(CertificateName)_' + @ToDate + '_$(NewExpiryDate).pkey'

RAISERROR(N'Backing up NEW certificate to: %s',0,1,@Path) WITH NOWAIT;
RAISERROR(N'Backing up NEW certificate private key to: %s',0,1,@Path2) WITH NOWAIT;

SET @CMD = N'BACKUP CERTIFICATE [$(CertificateName)] TO FILE = ' + QUOTENAME(@Path, '''') + N'
    WITH PRIVATE KEY ( 
    FILE = ' + QUOTENAME(@Path2, '''') + N' ,   
    ENCRYPTION BY PASSWORD = ''$(CertificatePassword)'' );  '

EXEC (@CMD);

CLOSE MASTER KEY;

PRINT N'Done.'
GO
EXEC xp_dirtree '$(BackupFolderPath)', 1, 1