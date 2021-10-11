:setvar CertificateName AutoBackup_Certificate
:setvar MasterKeyPassword paste_password_here
:setvar CertificatePassword paste_password_here
:setvar BackupFolderPath c:\temp\
USE [master]
GO 
-- Uncomment below if a master key already exists (don't forget to change the decryption password as needed)
IF EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
OPEN MASTER KEY DECRYPTION BY PASSWORD = '$(MasterKeyPassword)';
GO
RESTORE MASTER KEY   
    FROM FILE = '$(BackupFolderPath)Master_Key.key'   
    DECRYPTION BY PASSWORD = '$(MasterKeyPassword)'   
    ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)'
	--FORCE;  -- Uncomment this if a master key already exists which you cannot decrypt 
GO
OPEN MASTER KEY DECRYPTION BY PASSWORD = '$(MasterKeyPassword)';
GO
CREATE CERTIFICATE [$(CertificateName)]   
    FROM FILE = '$(BackupFolderPath)$(CertificateName).cer'   
    WITH PRIVATE KEY (FILE = '$(BackupFolderPath)$(CertificateName).pkey',   
    DECRYPTION BY PASSWORD = '$(CertificatePassword)');  
GO
CLOSE MASTER KEY;
GO