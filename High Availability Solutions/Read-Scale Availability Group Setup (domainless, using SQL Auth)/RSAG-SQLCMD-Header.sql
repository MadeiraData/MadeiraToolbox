
/****** YOU MUST EXECUTE THE FOLLOWING SCRIPT IN SQLCMD MODE ******/

:setvar SysAdminLoginName dba
:setvar SysAdminLoginPassword P@$$w0rd1!

:setvar AGName myAG

:setvar ServerA PRD-SQL1
:setvar ServerA_WithDNSSuffix PRD-SQL1.prod.Local

:setvar ServerB PRD-SQL2
:setvar ServerB_WithDNSSuffix PRD-SQL2.prod.Local

:setvar DBName agTestDB

:setvar AGLoginName ag_user
:setvar AGLoginPassword P@$$w0rd1!

:setvar ServerA_LocalBackupPath D:\BACKUP
:setvar ServerB_LocalBackupPath D:\BACKUP

:setvar ServerA_DBMasterKeyPassword P@$$w0rd1!23
:setvar ServerB_DBMasterKeyPassword P@$$w0rd1!23

:setvar ServerA_AGCertificateName AGCertificate_SQL1
:setvar ServerB_AGCertificateName AGCertificate_SQL2

:setvar AGEndPointName AG_Endpoint
:setvar AGEndPointPort 5022

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
