/*
---------------------------------------------------------------------
Rotate Database Master Key
---------------------------------------------------------------------
Author: Eitan Blumin
Date: 2020-11-09
Description:

Use this script to regenerate your database master key used for
backup encryption (for example, when you forget its password).

The script assumes that your Database Master Key already exists
(in the "master" database).

Don't forget to change the SQLCMD variable values.

Instructions:
--------------
1. Set the script mode to SQLCMD.
2. Set the SQLCMD variables to correct values as needed.
3. Run the script and review the messages pane for details.

What this script does:
-----------------------
1. Backup the current Database Master Key using the specified password
2. Backup the existing certificate with a private key using the specified password
3. Regenerate the Database Master Key. This will also re-encrypt your certificate(s)
4. Backup the re-encrypted certificate with a private key using the specified password

---------------------------------------------------------------------
SQLCMD Variables:
---------------------------------------------------------------------
CertificateName			The name of the backup certificate to backup
MasterKeyPassword		The password used for encrypting the database master key and its backup
CertificateBackupPassword	The password to be used for the certificate backup private key
BackupFolderPath		Folder path where the certificates and keys should be backed up to
---------------------------------------------------------------------
*/
:setvar CertificateName AutoBackups_Certificate
:setvar MasterKeyPassword database_master_key_password_here
:setvar CertificateBackupPassword backup_certificate_password_here
:setvar BackupFolderPath c:\backup_encryption\

USE [master]
GO
DECLARE @Today VARCHAR(10), @MasterKeyFromDate VARCHAR(10), @CertificateFromDate VARCHAR(10), @PKeyFromDate VARCHAR(10)
DECLARE @CertificateExists BIT, @CMD NVARCHAR(MAX)

SET @Today = CONVERT(nvarchar(10), GETDATE(), 112);

select @MasterKeyFromDate = CONVERT(nvarchar(10), modify_date, 112)
from sys.symmetric_keys
WHERE [name] = '##MS_DatabaseMasterKey##'

SELECT
  @CertificateExists = CASE WHEN certificate_id IS NOT NULL THEN 1 ELSE 0 END
, @CertificateFromDate = CONVERT(nvarchar(10), [start_date], 112)
, @PKeyFromDate = CONVERT(nvarchar(10), pvt_key_last_backup_date, 112)
FROM sys.certificates
WHERE [name] = '$(CertificateName)'

SET @CertificateExists = ISNULL(@CertificateExists, 0)

SELECT @CertificateExists AS [@CertificateExists], @CertificateFromDate AS [@CertificateFromDate], @PKeyFromDate AS [@PKeyFromDate], @Today AS [@Today], @NewExpiryDate AS [NewExpiryDate]

IF NOT EXISTS (SELECT NULL FROM sys.symmetric_keys WHERE [name] = '##MS_DatabaseMasterKey##')
BEGIN
	RAISERROR(N'No database master key found. You need to be creating a new database master key and backup certificate, not regenerating an existing one.', 16,1 );
	GOTO Quit;
END

IF @CertificateExists = 0
BEGIN
	RAISERROR(N'No existing certificate found. You need to be creating a new backup certificate, not regenerating an existing one.', 16,1 );
	GOTO Quit;
END

DECLARE @Path NVARCHAR(4000), @Path2 NVARCHAR(4000)
SET @Path = '$(BackupFolderPath)Master_Key_' + @MasterKeyFromDate + '_' + @Today + '.key'

RAISERROR(N'Backing up master key to: %s',0,1,@Path) WITH NOWAIT;

SET @CMD = N'BACKUP MASTER KEY TO FILE = ' + QUOTENAME(@Path, '''') + N'
    ENCRYPTION BY PASSWORD = ''$(MasterKeyPassword)'';'

EXEC (@CMD);

RAISERROR(N'Regenerating master key...',0,1) WITH NOWAIT;

ALTER MASTER KEY REGENERATE WITH ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)';

SET @Path = '$(BackupFolderPath)Master_Key_' + @Today + '_NEW.key'

RAISERROR(N'Backing up NEW master key to: %s',0,1,@Path) WITH NOWAIT;

SET @CMD = N'BACKUP MASTER KEY TO FILE = ' + QUOTENAME(@Path, '''') + N'
    ENCRYPTION BY PASSWORD = ''$(MasterKeyPassword)'';'

EXEC (@CMD);

SET @Path = '$(BackupFolderPath)$(CertificateName)_' + @Today + '_NEW.cer'
SET @Path2 = '$(BackupFolderPath)$(CertificateName)_' + @Today + '_NEW.pkey'

RAISERROR(N'Backing up NEW certificate to: %s',0,1,@Path) WITH NOWAIT;
RAISERROR(N'Backing up NEW certificate private key to: %s',0,1,@Path2) WITH NOWAIT;

SET @CMD = N'BACKUP CERTIFICATE [$(CertificateName)] TO FILE = ' + QUOTENAME(@Path, '''') + N'
    WITH PRIVATE KEY ( 
    FILE = ' + QUOTENAME(@Path2, '''') + N' ,   
    ENCRYPTION BY PASSWORD = ''$(CertificateBackupPassword)'' );'

EXEC (@CMD);

PRINT N'Done.'
Quit:
GO