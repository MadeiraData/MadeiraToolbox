/*
DatabaseIntegrityCheck - Minimal check on PRIMARY, Full check on SECONDARY SNAPSHOT
===================================================================================
Author: Eitan Blumin
Date: 2021-09-15
Description:
This script automatically detects whether the specified database
is part of an availability group and its current role.
If it's PRIMARY -> Run Ola Hallengren's DatabaseIntegrityCheck using PhysicalOnly and NoIndex mode.
If it's SECONDARY -> Create a database snapshot and run Ola Hallengren's DatabaseIntegrityCheck on it in full mode.

Prerequisites:
	- Ola Hallengren's maintenance solution installed. This script must run within the context of the database where it was installed.
	- Ola Hallengren's maintenance solution can be downloaded for free from here: https://ola.hallengren.com
	- SQL Server version 2012 or newer.
	- SQL Server Enterprise Edition (to support database snapshot creation).
	- Specified database must be part of an availability group.
*/
DECLARE
	@CurrentDatabaseName sysname = 'DemoDB',
	@SnapshotFolderPath nvarchar(MAX) = NULL, -- optionally force the SECONDARY snapshot to be created in a specific folder (must NOT end with \). If NULL then will use the same folder as the data file(s).
	@LogToTable	nvarchar(10) = 'Y',
	@Execute	nvarchar(10) = 'N',
	@PrimaryCommands nvarchar(4000) = 'CHECKDB',
	@PrimaryTimeLimitSeconds int = NULL,
	@SecondaryCommands nvarchar(4000) = 'CHECKALLOC,CHECKTABLE',
	@SecondaryTimeLimitSeconds int = 60 * 60 * 5,
	@SnapshotNamePostfix sysname = '_dbcc_checkdb';

SET NOCOUNT, XACT_ABORT, ARITHABORT, QUOTED_IDENTIFIER ON;
SET NOEXEC OFF;
DECLARE @Version int = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)
DECLARE @CurrentAvailabilityGroup sysname, @CurrentAvailabilityGroupRole sysname

IF @Version >= 11 AND CONVERT(int, SERVERPROPERTY('IsHadrEnabled')) = 1
BEGIN
    SELECT @CurrentAvailabilityGroup = ag.name,
           @CurrentAvailabilityGroupRole = replica_states.role_desc
    FROM sys.databases db
    INNER JOIN sys.dm_hadr_availability_replica_states replica_states ON db.replica_id = replica_states.replica_id
    INNER JOIN sys.availability_groups ag ON replica_states.group_id = ag.group_id
    WHERE db.name = @CurrentDatabaseName
	OPTION(RECOMPILE);
END

-- if PRIMARY, run CHECKDB WITH PHYSICAL_ONLY and NOINDEX checks
IF @CurrentAvailabilityGroupRole = 'PRIMARY'
BEGIN
	RAISERROR(N'Availability Group "%s" is "%s". Running minimal check.', 0, 1, @CurrentAvailabilityGroup, @CurrentAvailabilityGroupRole) WITH NOWAIT;

	EXEC dbo.DatabaseIntegrityCheck
		@Databases = @CurrentDatabaseName,
		@CheckCommands = @PrimaryCommands,
		@TimeLimit = @PrimaryTimeLimitSeconds,
		@PhysicalOnly = 'Y',
		@NoIndex = 'Y',
		@ExtendedLogicalChecks = 'N',
		@Updateability = 'ALL',
		@LogToTable= @LogToTable,
		@Execute = @Execute
END
-- if SECONDARY, create a database snapshot and run CHECKDB with full checks on it
ELSE IF @CurrentAvailabilityGroupRole = 'SECONDARY'
BEGIN
	DECLARE @CMD NVARCHAR(MAX), @SnapshotName SYSNAME;

	SET @SnapshotName = @CurrentDatabaseName + @SnapshotNamePostfix;
	
	IF DB_ID(@SnapshotName) IS NOT NULL
	BEGIN
		IF EXISTS (SELECT NULL FROM sys.databases WHERE [name] = @SnapshotName AND source_database IS NOT NULL)
		BEGIN
			RAISERROR(N'Existing snapshot detected: %s',0,1,@SnapshotName) WITH NOWAIT;
			SET @CMD = N'DROP DATABASE ' + QUOTENAME(@SnapshotName);
			PRINT @CMD;
			EXEC (@CMD)
		END
		ELSE
		BEGIN
			RAISERROR(N'Existing non-snapshot database detected: %s. Please change the snapshot postfix "%s", or drop or rename the existing database.',16,1,@SnapshotName,@SnapshotNamePostfix) WITH NOWAIT;
			SET NOEXEC ON;
		END
	END

	RAISERROR(N'Availability Group "%s" is "%s". Creating database snapshot "%s"', 0, 1, @CurrentAvailabilityGroup, @CurrentAvailabilityGroupRole, @SnapshotName) WITH NOWAIT;
	
	SELECT @CMD = ISNULL(@CMD + N',
	', N'') + N'(NAME = ' + QUOTENAME(name) + N'
	, FILENAME = ' + QUOTENAME(
			ISNULL(@SnapshotFolderPath, LEFT(physical_name, LEN(physical_name) - CHARINDEX('\', REVERSE(physical_name))))
			+ '\' + @SnapshotName + N'_' + name + '.ss'
			, N'''')
	+ N')'
	FROM sys.master_files
	WHERE type <> 1
	AND database_id = DB_ID(@CurrentDatabaseName)

	SET @CMD = N'CREATE DATABASE ' + QUOTENAME(@SnapshotName) 
	+ ISNULL(N'
	ON ' + @CMD, N'') 
	+ N'
	AS SNAPSHOT OF ' + QUOTENAME(@CurrentDatabaseName)

	PRINT @CMD
	EXEC (@CMD);

	EXEC dbo.DatabaseIntegrityCheck
		@Databases = @SnapshotName,
		@CheckCommands = @SecondaryCommands,
		@TimeLimit = @SecondaryTimeLimitSeconds,
		@PhysicalOnly = 'N',
		@NoIndex = 'N',
		@ExtendedLogicalChecks = 'Y',
		@Updateability = 'ALL',
		@LogToTable= @LogToTable,
		@Execute = @Execute

	IF DB_ID(@SnapshotName) IS NOT NULL
	BEGIN
		SET @CMD = N'DROP DATABASE ' + QUOTENAME(@SnapshotName);
		PRINT @CMD;
		EXEC (@CMD)
	END
END
ELSE
BEGIN
	RAISERROR(N'Database "%s" is not part of an Availability Group. Found: "%s", "%s"', 16, 1, @CurrentDatabaseName, @CurrentAvailabilityGroup, @CurrentAvailabilityGroupRole) WITH NOWAIT;
END