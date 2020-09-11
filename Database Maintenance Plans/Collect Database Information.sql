/*========================================================================================================================

Description:	Display information about user databases to be used for establishing a database maintenance plan
Scope:			Instance
Author:			Guy Glantser
Created:		09/09/2020
Last Updated:	09/09/2020
Notes:			Use this information to plan a maintenance plan for the user databases in the instance

=========================================================================================================================*/

SELECT
	DatabaseId						= database_id ,
	DatabaseName					= [name] ,
	UserAccessMode					= user_access_desc ,
	IsAutoShrinkOn					= is_auto_shrink_on ,
	DatabaseState					= state_desc ,
	RecoveryModel					= recovery_model_desc ,
	IsAutoCreateStatsOn				= is_auto_create_stats_on ,
	IsAutoCreateStatsIncrementalOn	= is_auto_create_stats_incremental_on ,	-- Applies to: SQL Server (starting with SQL Server 2014 (12.x))
	IsAutoUpdateStatsOn				= is_auto_update_stats_on ,
	IsAutoUpdateStatsAsyncOn		= is_auto_update_stats_async_on ,
	IsPublishedInReplication		= is_published ,
	LogReuseWait					= log_reuse_wait_desc ,
	IsCdcEnabled					= is_cdc_enabled ,
	IsPartOfAvailabilityGroup		=
		CASE
			WHEN group_database_id IS NULL	-- Applies to: SQL Server (starting with SQL Server 2012 (11.x)) and Azure SQL Database
				THEN 0
			ELSE
				1
		END ,
	IsAcceleratedDatabaseRecoveryOn	= is_accelerated_database_recovery_on	-- Applies to: SQL Server (starting with SQL Server 2019 (15.x)) and Azure SQL Database
FROM
	sys.databases
WHERE
	database_id > 4
AND
	source_database_id IS NULL
AND
	is_read_only = 0
ORDER BY
	DatabaseId ASC;
GO
