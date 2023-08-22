/*--==============================================================================================
Author: Eric Rouach, Madeira Data Solutions
Create date: 2023-08-16

Description: 
The following are execution examples of the dbo.DatabaseIntegrityCheck stored procedure
developed by Ola Hallengren https://ola.hallengren.com/sql-server-integrity-check.html

In case you have multiple Availability Groups, you should create a separate job for each
possible replica state (primary or secondary) in each replica so that the relevant check
(PhysicalOnly or full check) is performed.

Pre-requisites: You must install Ola Hallengren's maintenance solution.
https://ola.hallengren.com/downloads.html
--==============================================================================================*/

--Primary Integrity Check Job

--example 1: 
/*
perform a PhysicalOnly check for AG databases on all AGs for which the current replica is primary
*/
EXEC dbo.DatabaseIntegrityCheck
@AvailabilityGroupReplicas = 'PRIMARY',  -- <================
@Databases = 'AVAILABILITY_GROUP_DATABASES',
@CheckCommands = 'CHECKDB',
@TimeLimit = 18000,
@PhysicalOnly = 'Y',  -- <================
@Updateability = 'ALL',
@LogToTable= 'Y',
@Execute = 'Y'

--example 2
/*
perform a PhysicalOnly check for AG+User databases on all AGs for which the current replica is primary
*/
EXEC dbo.DatabaseIntegrityCheck
@AvailabilityGroupReplicas = 'PRIMARY',  -- <================
@Databases = 'AVAILABILITY_GROUP_DATABASES, USER_DATABASES',
@CheckCommands = 'CHECKDB',
@TimeLimit = 18000,
@PhysicalOnly = 'Y',  -- <================
@Updateability = 'ALL',
@LogToTable= 'Y',
@Execute = 'Y'

--Secondary integrity Check Job

--example 1: 
/*
perform a full check for AG databases on all AGs for which the current replica is secondary
*/
EXEC dbo.DatabaseIntegrityCheck
@AvailabilityGroupReplicas = 'SECONDARY',  -- <================
@Databases = 'AVAILABILITY_GROUP_DATABASES',
@CheckCommands = 'CHECKDB',
@TimeLimit = 18000,
@PhysicalOnly = 'N',  -- <================
@Updateability = 'ALL',
@LogToTable= 'Y',
@Execute = 'Y'

--example 2
/*
perform a full check for AG+User databases on all AGs for which the current replica is secondary
*/
EXEC dbo.DatabaseIntegrityCheck
@AvailabilityGroupReplicas = 'SECONDARY',  -- <================
@Databases = 'AVAILABILITY_GROUP_DATABASES, USER_DATABASES',
@CheckCommands = 'CHECKDB',
@TimeLimit = 18000,
@PhysicalOnly = 'N',  -- <================
@Updateability = 'ALL',
@LogToTable= 'Y',
@Execute = 'Y'
