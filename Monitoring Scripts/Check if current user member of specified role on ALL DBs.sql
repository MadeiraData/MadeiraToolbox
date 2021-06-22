
-- Check if current user member of specified role on ALL DBs
-- Replace db_owner by whatever rool or group you want to check

DECLARE
	@RoleToCheck	NVARCHAR(128)	= N'db_owner',
	@Command		NVARCHAR(1000)
	
	SET NOCOUNT ON;

	IF OBJECT_ID('tempdb..#allResults') IS NOT NULL
	DROP TABLE #allResults 

	CREATE TABLE #allResults
							(
								DatabaseName				NVARCHAR(128),
								IsMember					NVARCHAR(64)
							);

	SET @Command = 'USE [?] 
	SELECT
		DB_NAME(),
		CASE
			WHEN IS_MEMBER ('''+ @RoleToCheck +''') IS NOT NULL THEN CAST(IS_MEMBER ('''+ @RoleToCheck +''') AS NVARCHAR(64))
		ELSE ''Either group or role is not valid''
		END
	'

	INSERT INTO #allResults 
	EXEC master.sys.sp_MSforeachdb @Command

	SELECT
		DatabaseName,
		IsMember
	FROM
		#allResults;
