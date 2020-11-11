/*
---------------------------------------------------------------------
Rotate Backup Certificates
---------------------------------------------------------------------
Author: Eitan Blumin
Date: 2020-11-09
Description:

Use this script to drop and recreate your certificates used for
backup encryption (for example, when they're expired or about to be).

The script assumes that your backup certificate exists and encrypted using
the Database Master Key in the "master" database.

Don't forget to change the SQLCMD variable values.

Instructions:
--------------
1. Set the script mode to SQLCMD.
2. Set the SQLCMD variables to correct values as needed.
3. Run the script and review the messages pane for details.

What this script does:
-----------------------
1. Open the Database Master Key using the specified password
2. Backup the existing certificate with a private key using the specified password
3. Drop the existing certificate
4. Create a new certificate (with the same name), encrypted by the Database Master Key,
   with the specified expiry date
5. Backup the new certificate with a private key using the specified password

---------------------------------------------------------------------
SQLCMD Variables:
---------------------------------------------------------------------
CertificateName			The name of the backup certificate to drop and re-create
CertificateDescription		The description of the new backup certificate
NewExpiryDate			The expiry date of the new backup certificate
MasterKeyPassword		The password used for encrypting the database master key
CertificateBackupPassword	The password to be used for the certificate backup private key
BackupFolderPath		Folder path where the certificates and keys should be backed up to
---------------------------------------------------------------------
*/
:setvar CertificateName AutoBackups_Certificate
:setvar CertificateDescription "Automatic Backups Certificate"
:setvar NewExpiryDate 20991231
:setvar MasterKeyPassword database_master_key_password_here
:setvar CertificateBackupPassword backup_certificate_password_here
:setvar BackupFolderPath c:\backup_encryption\

USE [master]
GO
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
DECLARE @Today VARCHAR(10), @CertificateFromDate VARCHAR(10), @PKeyFromDate VARCHAR(10), @NewExpiryDate VARCHAR(10)
DECLARE @CertificateExists BIT;

SET @Today = CONVERT(nvarchar(10), GETDATE(), 112);
SET @NewExpiryDate = CONVERT(nvarchar(10), CONVERT(datetime, '$(NewExpiryDate)'), 112);

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

RAISERROR(N'Opening master key...',0,1) WITH NOWAIT;
OPEN MASTER KEY DECRYPTION BY PASSWORD = '$(MasterKeyPassword)';

DECLARE @CMD NVARCHAR(MAX), @Path NVARCHAR(4000), @Path2 NVARCHAR(4000)

SET @Path = '$(BackupFolderPath)$(CertificateName)_' + @CertificateFromDate + '_to_' + @Today + '.cer'
SET @Path2 = '$(BackupFolderPath)$(CertificateName)_' + ISNULL(@PKeyFromDate,@CertificateFromDate) + '_to_' + @Today + '.pkey'

RAISERROR(N'Backing up certificate to: %s',0,1,@Path) WITH NOWAIT;
RAISERROR(N'Backing up certificate private key to: %s',0,1,@Path2) WITH NOWAIT;

SET @CMD = N'BACKUP CERTIFICATE [$(CertificateName)] TO FILE = ' + QUOTENAME(@Path, '''') + N'
    WITH PRIVATE KEY ( 
    FILE = ' + QUOTENAME(@Path2, '''') + N' ,   
    ENCRYPTION BY PASSWORD = ''$(CertificateBackupPassword)'' );'

EXEC (@CMD);
	
RAISERROR(N'Dropping old certificate...',0,1) WITH NOWAIT;
DROP CERTIFICATE [$(CertificateName)];

RAISERROR(N'Creating new certificate...',0,1) WITH NOWAIT;

CREATE CERTIFICATE [$(CertificateName)]   
   WITH SUBJECT = '$(CertificateDescription)',   
   EXPIRY_DATE = '$(NewExpiryDate)'; 
   
   
SET @Path = '$(BackupFolderPath)$(CertificateName)_' + @Today + '_to_$(NewExpiryDate).cer'
SET @Path2 = '$(BackupFolderPath)$(CertificateName)_' + @Today + '_to_$(NewExpiryDate).pkey'

RAISERROR(N'Backing up NEW certificate to: %s',0,1,@Path) WITH NOWAIT;
RAISERROR(N'Backing up NEW certificate private key to: %s',0,1,@Path2) WITH NOWAIT;

SET @CMD = N'BACKUP CERTIFICATE [$(CertificateName)] TO FILE = ' + QUOTENAME(@Path, '''') + N'
    WITH PRIVATE KEY ( 
    FILE = ' + QUOTENAME(@Path2, '''') + N' ,   
    ENCRYPTION BY PASSWORD = ''$(CertificateBackupPassword)'' );'

EXEC (@CMD);

CLOSE MASTER KEY;

PRINT N'Done.'
Quit:
GO
