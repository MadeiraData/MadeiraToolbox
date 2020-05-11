
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
-- Execute the following code on Server B
-- ======================================
:CONNECT $(ServerB) -U $(SysAdminLoginName) -P $(SysAdminLoginPassword)


PRINT N'Join the Availability Group $(AGName)...'

ALTER AVAILABILITY GROUP [$(AGName)]
JOIN WITH (CLUSTER_TYPE = NONE);
