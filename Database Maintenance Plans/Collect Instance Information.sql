/*========================================================================================================================

Description:	Display information about the instance to be used for establishing a database maintenance plan
Scope:			Instance
Author:			Guy Glantser
Created:		09/09/2020
Last Updated:	09/09/2020
Notes:			Use this information to plan a maintenance plan for the user databases in the instance

=========================================================================================================================*/

SELECT
	ServerName			= SERVERPROPERTY ('ServerName') ,
	InstanceVersion		=
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
	ProductLevel		= SERVERPROPERTY ('ProductLevel') ,
	ProductUpdateLevel	= SERVERPROPERTY ('ProductUpdateLevel') ,	-- Applies to: SQL Server 2012 (11.x) through current version in updates beginning in late 2015
	ProductBuildNumber	= SERVERPROPERTY ('ProductVersion') ,
	InstanceEdition		= SERVERPROPERTY ('Edition') ,
	IsClustered			= SERVERPROPERTY ('IsClustered') ,
	IsHadrEnabled		= SERVERPROPERTY ('IsHadrEnabled');			-- Applies to: SQL Server 2012 (11.x) and later
GO
