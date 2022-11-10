/*
DatabaseIntegrityCheck - Minimal check on PRIMARY, Full check on SECONDARY
==========================================================================
Author: Eitan Blumin
Date: 2022-09-22
Description:
If it's PRIMARY -> Run Ola Hallengren's DatabaseIntegrityCheck using PhysicalOnly and NoIndex mode.
If it's SECONDARY -> Run Ola Hallengren's DatabaseIntegrityCheck using full mode.
If it's LOCAL (non-AG) -> Run Ola Hallengren's DatabaseIntegrityCheck using standard mode.

Prerequisites:
	- Ola Hallengren's maintenance solution installed. This script must run within the context of the database where it was installed.
	- Ola Hallengren's maintenance solution can be downloaded for free from here: https://ola.hallengren.com
	- SQL Server version 2012 or newer.
	- SQL Server Enterprise Edition (to support readable secondaries).
	- Specified database must be part of an availability group.
*/

-- Primary (Physical Only)
EXEC dbo.DatabaseIntegrityCheck
	@Databases = 'AVAILABILITY_GROUP_DATABASES',
	@CheckCommands = 'CHECKDB',
	@AvailabilityGroupReplicas='PRIMARY',
	@TimeLimit = 18000,
	@PhysicalOnly = 'Y',
	@NoIndex = 'Y',
	@ExtendedLogicalChecks = 'N',
	@Updateability = 'ALL',
	@LogToTable= 'Y',
	@Execute = 'Y'

-- Secondary (Full Check)
EXEC dbo.DatabaseIntegrityCheck
	@Databases = 'AVAILABILITY_GROUP_DATABASES',
	@CheckCommands = 'CHECKDB',
	@AvailabilityGroupReplicas='SECONDARY', --'CHECKALLOC,CHECKTABLE',
	@TimeLimit = 18000,
	@PhysicalOnly = 'N',
	@NoIndex = 'N',
	@ExtendedLogicalChecks = 'Y',
	@Updateability = 'ALL',
	@LogToTable= 'Y',
	@Execute = 'Y'
	
-- Local Databases (Non-AG)
EXEC dbo.DatabaseIntegrityCheck
	@Databases = 'USER_DATABASES,-AVAILABILITY_GROUP_DATABASES',
	@CheckCommands = 'CHECKDB',
	@TimeLimit = 18000,
	@PhysicalOnly = 'N',
	@NoIndex = 'N',
	--@ExtendedLogicalChecks = 'Y',
	@Updateability = 'ALL',
	@LogToTable= 'Y',
	@Execute = 'Y'
