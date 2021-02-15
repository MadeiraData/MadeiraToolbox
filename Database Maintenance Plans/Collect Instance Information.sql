/*========================================================================================================================

Description:	Display general information about the SQL Server instance and the host platform
Scope:			Instance
Author:			Guy Glantser
Created:		09/09/2020
Last Updated:	15/02/2021
Notes:			N/A

=========================================================================================================================*/

SELECT
	ServerName					= SERVERPROPERTY ('ServerName') ,
	InstanceVersion				=
		CASE
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '8%'	THEN N'2000'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '9%'	THEN N'2005'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '10.0%'	THEN N'2008'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '10.5%'	THEN N'2008 R2'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '11%'	THEN N'2012'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '12%'	THEN N'2014'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '13%'	THEN N'2016'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '14%'	THEN N'2017'
			WHEN CAST (SERVERPROPERTY ('ProductVersion') AS NVARCHAR(128)) LIKE '15%'	THEN N'2019'
		END ,
	ProductLevel				= SERVERPROPERTY ('ProductLevel') ,
	ProductUpdateLevel			= SERVERPROPERTY ('ProductUpdateLevel') ,										-- Applies to: SQL Server 2012 (11.x) through current version in updates beginning in late 2015
	ProductBuildNumber			= SERVERPROPERTY ('ProductVersion') ,
	InstanceEdition				= SERVERPROPERTY ('Edition') ,
	IsClustered					= SERVERPROPERTY ('IsClustered') ,
	IsHadrEnabled				= SERVERPROPERTY ('IsHadrEnabled') ,											-- Applies to: SQL Server 2012 (11.x) and later
	HadrManagerStatus			=																				-- Applies to: SQL Server 2012 (11.x) and later
		CASE WHEN SERVERPROPERTY ('IsHadrEnabled') = 0
			THEN N'Not Applicable'
			ELSE
			CASE SERVERPROPERTY ('HadrManagerStatus')
				WHEN 0	THEN N'Not Started, Pending Communication'
				WHEN 1	THEN N'Started and Running'
				WHEN 2	THEN N'Not Started and Failed'
				ELSE N'Not Applicable'
			END
		END ,
	HostPlatform				= HostInfo.host_distribution ,													-- Applies to: SQL Server 2017 (14.x) and later
	VirtualizationType			= SystemInfo.virtual_machine_type_desc ,										-- Applies to: SQL Server 2008 R2 and later
	NumberOfCores				= SystemInfo.cpu_count ,
	PhysicalMemory_GB			= CAST (ROUND (SystemInfo.physical_memory_kb / 1024.0 / 1024.0 , 0) AS INT) ,	-- Applies to: SQL Server 2012 (11.x) and later
	LastServiceRestartDateTime	= SystemInfo.sqlserver_start_time
FROM
	sys.dm_os_host_info AS HostInfo
CROSS JOIN
	sys.dm_os_sys_info AS SystemInfo;
GO
