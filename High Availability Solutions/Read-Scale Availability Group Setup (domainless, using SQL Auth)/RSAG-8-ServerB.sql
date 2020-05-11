
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


PRINT N'Restore FULL Backup(s) with NORECOVEY...'

RESTORE DATABASE [$(DBName)]
FROM DISK = '$(ServerB_LocalBackupPath)\$(DBName).bak'
WITH NORECOVERY;
GO

PRINT N'Restore LOG Backup(s) with NORECOVEY...'

RESTORE LOG [$(DBName)]
FROM DISK = '$(ServerB_LocalBackupPath)\$(DBName).trn'
WITH NORECOVERY;
GO

PRINT N'Wait for the replica to start communicating...'

begin try
declare @conn bit
declare @count int
declare @replica_id uniqueidentifier 
declare @group_id uniqueidentifier
set @conn = 0
set @count = 30 -- wait for 5 minutes 

if (serverproperty('IsHadrEnabled') = 1)
	and (isnull((select member_state from master.sys.dm_hadr_cluster_members where upper(member_name COLLATE Latin1_General_CI_AS) = upper(cast(serverproperty('ComputerNamePhysicalNetBIOS') as nvarchar(256)) COLLATE Latin1_General_CI_AS)), 0) <> 0)
	and (isnull((select state from master.sys.database_mirroring_endpoints), 1) = 0)
begin
    select @group_id = ags.group_id from master.sys.availability_groups as ags where name = N'$(AGName)'
	select @replica_id = replicas.replica_id from master.sys.availability_replicas as replicas where upper(replicas.replica_server_name COLLATE Latin1_General_CI_AS) = upper(@@SERVERNAME COLLATE Latin1_General_CI_AS) and group_id = @group_id
	while @conn <> 1 and @count > 0
	begin
		set @conn = isnull((select connected_state from master.sys.dm_hadr_availability_replica_states as states where states.replica_id = @replica_id), 1)
		if @conn = 1
		begin
			-- exit loop when the replica is connected, or if the query cannot find the replica status
			break
		end
		waitfor delay '00:00:10'
		set @count = @count - 1
	end
end
end try
begin catch
	-- If the wait loop fails, do not stop execution of the alter database statement
end catch
GO

PRINT N'Move the replica database(s) into the Availability Group $(AGName)...'

ALTER DATABASE [$(DBName)]
SET HADR AVAILABILITY GROUP = [$(AGName)];
GO
PRINT N'====================================='
PRINT N'Phase 3 complete. All done.'
