:setvar CertificateName AutoBackup_Certificate
:setvar MasterKeyPassword paste_password_here
:setvar CertificatePassword paste_password_here
:setvar BackupFolderPath c:\temp\
USE [master]
GO
DECLARE @ToDate VARCHAR(10), @MasterKeyFromDate VARCHAR(10), @CertificateFromDate VARCHAR(10), @PKeyFromDate VARCHAR(10)

SET @ToDate = CONVERT(nvarchar(10), GETDATE(), 112);

select @MasterKeyFromDate = CONVERT(nvarchar(10), modify_date, 112)
from sys.symmetric_keys
WHERE [name] = '##MS_DatabaseMasterKey##'

SELECT @CertificateFromDate = CONVERT(nvarchar(10), [start_date], 112), @PKeyFromDate = CONVERT(nvarchar(10), pvt_key_last_backup_date, 112)
FROM sys.certificates
WHERE [name] = '$(CertificateName)'

SELECT @MasterKeyFromDate AS [@MasterKeyFromDate], @CertificateFromDate AS [@CertificateFromDate], @PKeyFromDate AS [@PKeyFromDate], @ToDate AS [@ToDate]

DECLARE @Path NVARCHAR(4000), @Path2 NVARCHAR(4000)
SET @Path = '$(BackupFolderPath)Master_Key_' + @MasterKeyFromDate + '_' + @ToDate + '.key'

RAISERROR(N'Backing up master key to: %s',0,1,@Path) WITH NOWAIT;

BACKUP MASTER KEY TO FILE = @Path  
    ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)';
	
RAISERROR(N'Regenerating master key...',0,1) WITH NOWAIT;

ALTER MASTER KEY REGENERATE WITH ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)';

SET @Path = '$(BackupFolderPath)Master_Key_' + @ToDate + '_NEW.key'

RAISERROR(N'Backing up NEW master key to: %s',0,1,@Path) WITH NOWAIT;

BACKUP MASTER KEY TO FILE = @Path  
    ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)'
	
SET @Path = '$(BackupFolderPath)$(CertificateName)_' + @ToDate + '_NEW.cer'
SET @Path2 = '$(BackupFolderPath)$(CertificateName)_' + @ToDate + '_NEW.pkey'

RAISERROR(N'Backing up NEW certificate to: %s',0,1,@Path) WITH NOWAIT;
RAISERROR(N'Backing up NEW certificate private key to: %s',0,1,@Path2) WITH NOWAIT;

BACKUP CERTIFICATE [$(CertificateName)] TO FILE = @Path  
    WITH PRIVATE KEY ( 
    FILE = @Path2 ,   
    ENCRYPTION BY PASSWORD = '$(CertificatePassword)' );  

PRINT N'Done.'
GO