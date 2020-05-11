
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


PRINT N'Make a LOG backup of the replica database(s) to $(ServerA_LocalBackupPath)...'

BACKUP LOG [$(DBName)] 
TO DISK = '$(ServerA_LocalBackupPath)\$(DBName).trn'
WITH NOFORMAT, INIT, SKIP, NOREWIND, NOUNLOAD, COMPRESSION,  STATS = 10, CHECKSUM;

GO
PRINT N'============================================================================================='
PRINT N'Phase 2 complete. Now you need to copy the backup files from the PRIMARY to the SECONDARY.'
PRINT N'Copy $(ServerA_LocalBackupPath)\$(DBName).bak from PRIMARY to $(ServerB_LocalBackupPath)\$(DBName).bak on SECONDARY.'
PRINT N'Copy $(ServerA_LocalBackupPath)\$(DBName).trn from PRIMARY to $(ServerB_LocalBackupPath)\$(DBName).trn on SECONDARY.'
