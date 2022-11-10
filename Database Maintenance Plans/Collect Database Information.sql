/*========================================================================================================================

Description:	Display information about all the databases in the instance
Scope:			Instance
Author:			Guy Glantser
Created:		09/09/2020
Last Updated:	22/12/2021
Notes:			Use this information to plan a maintenance plan for the databases in the instance
				or to gather information about the databases in general

=========================================================================================================================*/

SELECT
	ServerName						= SERVERPROPERTY ('ServerName') ,
	DatabaseId						= [Databases].database_id ,
	DatabaseName					= [Databases].[name] ,
	UserAccessMode					= [Databases].user_access_desc ,
	DatabaseState					= [Databases].state_desc ,
	RecoveryModel					= [Databases].recovery_model_desc ,
	PageVerifyOption				= [Databases].page_verify_option_desc ,
	CompatibilityLevel				= [Databases].[compatibility_level] ,
	DataFilesCount					= DatabaseFiles.DataFilesCount ,
	DataSize_GB						= DatabaseFiles.DataSize_GB ,
	LogFilesCount					= DatabaseFiles.LogFilesCount ,
	LogSize_GB						= DatabaseFiles.LogSize_GB ,
	IsAutoShrinkOn					= [Databases].is_auto_shrink_on ,
	IsAutoCreateStatsOn				= [Databases].is_auto_create_stats_on ,
	IsAutoCreateStatsIncrementalOn	= [Databases].is_auto_create_stats_incremental_on ,	-- Applies to: SQL Server (starting with SQL Server 2014 (12.x))
	IsAutoUpdateStatsOn				= [Databases].is_auto_update_stats_on ,
	IsAutoUpdateStatsAsyncOn		= [Databases].is_auto_update_stats_async_on ,
	IsPublishedInReplication		= [Databases].is_published ,
	LogReuseWait					= [Databases].log_reuse_wait_desc ,
	IsCdcEnabled					= [Databases].is_cdc_enabled ,
	IsPartOfAvailabilityGroup		=
		CASE
			WHEN [Databases].group_database_id IS NULL	-- Applies to: SQL Server (starting with SQL Server 2012 (11.x)) and Azure SQL Database
				THEN 0
			ELSE
				1
		END ,
	IsAcceleratedDatabaseRecoveryOn	= [Databases].is_accelerated_database_recovery_on	-- Applies to: SQL Server (starting with SQL Server 2019 (15.x)) and Azure SQL Database
FROM
	sys.databases AS [Databases]
CROSS APPLY
(
	SELECT
		DataFilesCount	= COUNT (CASE WHEN [MasterFiles].[type] = 0 THEN [type] ELSE NULL END) ,
		DataSize_GB		= CAST (ROUND (CAST (SUM (CASE WHEN [MasterFiles].[type] = 0 THEN size ELSE 0 END) AS DECIMAL(19,2)) * 8.0 / 1024.0 / 1024.0 , 2) AS DECIMAL(19,2)) ,
		LogFilesCount	= COUNT (CASE WHEN [MasterFiles].[type] = 1 THEN [type] ELSE NULL END) ,
		LogSize_GB		= CAST (ROUND (CAST (SUM (CASE WHEN [MasterFiles].[type] = 1 THEN size ELSE 0 END) AS DECIMAL(19,2)) * 8.0 / 1024.0 / 1024.0 , 2) AS DECIMAL(19,2))
	FROM
		sys.master_files AS [MasterFiles]
	WHERE
		[MasterFiles].database_id = [Databases].database_id
)
AS
	DatabaseFiles
WHERE
	[Databases].source_database_id IS NULL	-- Not a database snapshot
AND
	[Databases].is_read_only = 0
ORDER BY
	DatabaseId ASC;
GO
