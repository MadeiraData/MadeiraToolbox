
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

USE master;
GO

-- Change the recovery model to full and the AUTO_CLOSE to off on all databases

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$(DBName)' AND recovery_model_desc = 'FULL')
BEGIN
	PRINT N'Changing Recovery Model for $(DBName) to FULL...'
	ALTER DATABASE [$(DBName)]
	SET RECOVERY FULL;
END
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '$(DBName)' AND is_auto_close_on = 0)
BEGIN
	PRINT N'Disabling Auto-Close for $(DBName)...'
	ALTER DATABASE [$(DBName)]
	SET AUTO_CLOSE OFF;
END
GO


PRINT N'Make a full backup of the replica database(s) to $(ServerA_LocalBackupPath)...'

BACKUP DATABASE	[$(DBName)]
TO DISK = '$(ServerA_LocalBackupPath)\$(DBName).bak'
WITH NOFORMAT, INIT, SKIP, NOREWIND, NOUNLOAD, COMPRESSION,  STATS = 10, CHECKSUM;
GO


PRINT N'Create a new Availability Group with replicas...'

CREATE AVAILABILITY GROUP [$(AGName)]
WITH
(
	AUTOMATED_BACKUP_PREFERENCE = PRIMARY,
	DB_FAILOVER = OFF,
	DTC_SUPPORT = NONE,
	CLUSTER_TYPE = NONE,
	REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 0
)
FOR DATABASE [$(DBName)] --, DB2 , ...
REPLICA ON
'$(ServerA)' WITH
(
	ENDPOINT_URL = 'TCP://$(ServerA_WithDNSSuffix):$(AGEndPointPort)', 
	FAILOVER_MODE = MANUAL , 
	AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT , 
	SECONDARY_ROLE
	(
		ALLOW_CONNECTIONS = ALL
	)
),
'$(ServerB)' WITH
(
	ENDPOINT_URL = 'TCP://$(ServerB_WithDNSSuffix):$(AGEndPointPort)', 
	FAILOVER_MODE = MANUAL, 
	AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT, 
	SECONDARY_ROLE
	(
		ALLOW_CONNECTIONS = ALL
	)
);
