/*========================================================================================================================

Description:	Display information about all the databases in the instance to be used for establishing a database maintenance plan
Scope:			Instance
Author:			Guy Glantser
Created:		09/09/2020
Last Updated:	15/02/2021
Notes:			Use this information to plan a maintenance plan for the databases in the instance

=========================================================================================================================*/

SELECT
	  ServerName					= SERVERPROPERTY('ServerName') 
	, DatabaseId					= database_id 
	, DatabaseName					= [name] 
	, UserAccessMode				= user_access_desc 
	, DatabaseState					= state_desc 
	, RecoveryModel					= recovery_model_desc 
	, PageVerifyOption				= page_verify_option_desc
	, CompatibilityLevel			= [compatibility_level]
	, Data_Size_MB					= dbfiles.Data_MB
	, Data_Files					= dbfiles.Data_Files
	, Log_Size_MB					= dbfiles.Log_MB
	, Log_Files						= dbfiles.Log_Files
	, IsAutoShrinkOn					= is_auto_shrink_on 
	, IsAutoCreateStatsOn				= is_auto_create_stats_on 
	, IsAutoCreateStatsIncrementalOn	= is_auto_create_stats_incremental_on 	-- Applies to: SQL Server (starting with SQL Server 2014 (12.x))
	, IsAutoUpdateStatsOn				= is_auto_update_stats_on 
	, IsAutoUpdateStatsAsyncOn		= is_auto_update_stats_async_on 
	, IsPublishedInReplication		= is_published 
	, LogReuseWait					= log_reuse_wait_desc 
	, IsCdcEnabled					= is_cdc_enabled 
	, IsPartOfAvailabilityGroup		=
		CASE
			WHEN group_database_id IS NULL	-- Applies to: SQL Server (starting with SQL Server 2012 (11.x)) and Azure SQL Database
				THEN 0
			ELSE
				1
		END 
	, IsAcceleratedDatabaseRecoveryOn	= is_accelerated_database_recovery_on	-- Applies to: SQL Server (starting with SQL Server 2019 (15.x)) and Azure SQL Database
FROM
	sys.databases
CROSS APPLY
(
	SELECT
		 Data_Files = COUNT(CASE WHEN [type] <> 1 THEN [type] END)
		,Log_Files =  COUNT(CASE WHEN [type] = 1 THEN [type] END)
		,Data_MB = CONVERT(float, SUM(CASE WHEN [type] <> 1 THEN size END) / 128.0)
		,Log_MB =  CONVERT(float, SUM(CASE WHEN [type] = 1 THEN size END) / 128.0)
	FROM sys.master_files
	WHERE master_files.database_id = databases.database_id
) AS dbfiles
WHERE
	source_database_id IS NULL
AND
	is_read_only = 0
ORDER BY
	DatabaseId ASC;
GO