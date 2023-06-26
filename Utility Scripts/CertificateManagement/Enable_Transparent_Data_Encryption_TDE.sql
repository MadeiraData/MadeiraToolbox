:setvar CertificateName DEK_Certificate
:setvar DatabaseName TestDB
:setvar IsSqlCMDOn yes
GO
SET NOEXEC OFF
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
USE [$(DatabaseName)]
GO
IF NOT EXISTS (
select database_name, backup_start_date, type
from msdb..backupset
where type = 'D'
AND database_name = '$(DatabaseName)'
AND backup_start_date > DATEADD(dd,-1,GETDATE())
)
BEGIN
	RAISERROR(N'Missing a recent Full database backup for $(DatabaseName)',15,1);
	SET NOEXEC ON;
END
GO
IF EXISTS (
SELECT *
FROM sys.dm_database_encryption_keys
WHERE encryption_state_desc <> 'UNENCRYPTED'
AND database_id = DB_ID('$(DatabaseName)')
)
BEGIN
	RAISERROR(N'Database $(DatabaseName) is already being encrypted.',15,1);
	SET NOEXEC ON;
END
GO
CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = AES_256
ENCRYPTION BY SERVER CERTIFICATE [$(CertificateName)]
GO
ALTER DATABASE CURRENT SET ENCRYPTION ON;
GO
SET NOEXEC OFF;
SELECT db_name(database_id) as dbname, *
FROM sys.dm_database_encryption_keys
