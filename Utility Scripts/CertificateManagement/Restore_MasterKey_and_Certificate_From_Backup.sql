:setvar CertificateName DEK_Certificate
:setvar MasterKeyPassword paste_password_here
:setvar CertificatePassword paste_password_here
:setvar MasterKeyBackupFilePath c:\TDE\Master_Key.key
:setvar CertificateBackupFilePath c:\TDE\DEK_Certificate.cer
:setvar CertificateBackupFileKeyPath c:\TDE\DEK_Certificate.pkey
USE [master]
GO 
-- Uncomment below if a master key already exists (don't forget to change the decryption password as needed)
IF EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
OPEN MASTER KEY DECRYPTION BY PASSWORD = '$(MasterKeyPassword)';
GO
RESTORE MASTER KEY   
    FROM FILE = '$(MasterKeyBackupFilePath)'   
    DECRYPTION BY PASSWORD = '$(MasterKeyPassword)'   
    ENCRYPTION BY PASSWORD = '$(MasterKeyPassword)'
	--FORCE;  -- Uncomment this if a master key already exists which you cannot decrypt 
GO
OPEN MASTER KEY DECRYPTION BY PASSWORD = '$(MasterKeyPassword)';
GO
IF EXISTS (SELECT * FROM sys.certificates WHERE name = '$(CertificateName)')
BEGIN
	RAISERROR(N'Dropping existing certificate...',0,1) WITH NOWAIT;
	
	DROP CERTIFICATE [$(CertificateName)];
END
GO
CREATE CERTIFICATE [$(CertificateName)]   
    FROM FILE = '$(CertificateBackupFilePath)'   
    WITH PRIVATE KEY (FILE = '$(CertificateBackupFileKeyPath)',   
    DECRYPTION BY PASSWORD = '$(CertificatePassword)');  
GO
CLOSE MASTER KEY;
GO