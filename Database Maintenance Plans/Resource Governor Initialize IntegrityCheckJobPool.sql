/*
Initialize SQL Resource Governor workload group
in order to limit Disk IO and CPU impact of
a Database Integrity job.
Author: Eitan Blumin
Date: 2021-02-14

Use the following to find the relevant Program Name based on the job name:

DECLARE
     @MaintenanceJobName   SYSNAME = N'Maintenance.IntegrityAndIndex'
   , @MaintenanceStepID    INT     = NULL -- if you know a specific step_id that you want to limit, enter it here. Otherwise set to NULL to limit the whole job.

DECLARE @JobID VARBINARY(max)
SELECT @JobID = job_id
FROM msdb..sysjobs
WHERE name = @MaintenanceJobName

SELECT JobAppName = N'SQLAgent - TSQL JobStep (Job ' + CONVERT(nvarchar(max), @JobID, 1) + ISNULL(N' : Step ' + CONVERT(nvarchar(max), @MaintenanceStepID) + N')', N' :%')


*/
USE [master]
GO
-----------------------------------------------
-- verify the resource pools, workload groups, and the classifier user-defined function --
-----------------------------------------------
USE master
GO  
--- Get the classifer function name and the name of the schema  
--- that it is bound to.  
SELECT   
      object_schema_name(classifier_function_id) AS [schema_name],  
      object_name(classifier_function_id) AS [function_name],
      *
FROM sys.dm_resource_governor_configuration    

SELECT * FROM sys.dm_resource_governor_resource_pools  
SELECT * FROM sys.dm_resource_governor_workload_groups  
GO
-----------------------------------------------
-- Step 1: Create Resource Pool
-----------------------------------------------
-- Creating Resource Pool for IntegrityCheck
CREATE RESOURCE POOL IntegrityCheckJobPool
WITH
( MIN_CPU_PERCENT=0,
MAX_CPU_PERCENT=50,
MIN_MEMORY_PERCENT=0,
MAX_MEMORY_PERCENT=30, 
MAX_IOPS_PER_VOLUME=200)
GO
-----------------------------------------------
-- Step 2: Create Workload Group
-----------------------------------------------
-- Creating Workload Group for IntegrityCheck
CREATE WORKLOAD GROUP IntegrityCheckJobGroup
USING IntegrityCheckJobPool ;
GO
-----------------------------------------------
-- Step 3: Create UDF to Route Workload Group
-----------------------------------------------
CREATE FUNCTION dbo.UDF_Workload_Classifier()
RETURNS SYSNAME
WITH SCHEMABINDING
AS
BEGIN
DECLARE @WorkloadGroup AS SYSNAME
IF(PROGRAM_NAME() LIKE 'SQLAgent - TSQL JobStep (Job 0xB7B4D113397DB642AB52C71219C3F2C4 : Step 4)')
SET @WorkloadGroup = 'IntegrityCheckJobGroup'
ELSE
SET @WorkloadGroup = 'default'
RETURN @WorkloadGroup
END
GO
-----------------------------------------------
-- Step 4: Enable Resource Governer
-- with Classifier function
-----------------------------------------------
ALTER RESOURCE GOVERNOR
WITH (CLASSIFIER_FUNCTION=dbo.UDF_Workload_Classifier);
GO
ALTER RESOURCE GOVERNOR RECONFIGURE
GO
-----------------------------------------------
-- verify the resource pools, workload groups, and the classifier user-defined function --
-----------------------------------------------
USE master
GO  
--- Get the classifer function name and the name of the schema  
--- that it is bound to.  
SELECT   
      object_schema_name(classifier_function_id) AS [schema_name],  
      object_name(classifier_function_id) AS [function_name],
      *
FROM sys.dm_resource_governor_configuration    

SELECT * FROM sys.dm_resource_governor_resource_pools  
SELECT * FROM sys.dm_resource_governor_workload_groups  
GO 
--- Find out what sessions are in each group by using the following query.
SELECT s.group_id, g.name as group_name, s.session_id, s.login_time, s.login_name, s.host_name, s.program_name
FROM sys.dm_exec_sessions s  
INNER JOIN sys.dm_resource_governor_workload_groups g  
ON g.group_id = s.group_id  
ORDER BY g.name  
GO  
-----------------------------------------------
-- Step 5: Clean Up
-- Run only if you want to clean up everything
-----------------------------------------------
/*
ALTER RESOURCE GOVERNOR WITH (CLASSIFIER_FUNCTION = NULL)
GO
ALTER RESOURCE GOVERNOR DISABLE
GO
DROP FUNCTION dbo.UDF_Workload_Classifier
GO
DROP WORKLOAD GROUP IntegrityCheckJobGroup
GO
DROP RESOURCE POOL IntegrityCheckJobPool
GO
ALTER RESOURCE GOVERNOR RECONFIGURE
*/
GO