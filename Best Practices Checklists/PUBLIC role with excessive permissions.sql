/*
Excessive permissions should not be granted to PUBLIC role
===========================================================
Author: Eitan Blumin | https://madeiradata.com | https://eitanblumin.com
Date: 2021-08-02
Description:
This check is based on SQL Vulnerability Assessment rules VA1054 and VA1095
https://eitanblumin.com/sql-vulnerability-assessment-tool-rules-reference-list/#Rule_VA1054
https://eitanblumin.com/sql-vulnerability-assessment-tool-rules-reference-list/#Rule_VA1095

The rule check is evaluated against all accessible databases,
and outputs the relevant details as well as remediation commands.

Parameters:
================================================
@FilterByDatabaseName
================================================
	Optional parameter to filter by database name.
	Leave as NULL to check all accessible databases.
================================================
@EnableRevokeSimulation_To_CheckForAffectedUsers
================================================
	This enables a simulation where PUBLIC permissions are temporarily REVOKED,
	and then each database user is checked for loss of permissions.
	Any users with permissions loss will be returned in the "NegativelyAffectedUsers" columns.
	The permissions will be re-GRANT-ed after each check.

	IMPORTANT:
	Please be mindful of possible effect on production systems as this simulation
	may cause temporary loss of permissions for currently active users!

	NOTE:
	Unless you're also using the @CloneDatabaseName_ForRevokeSimulation parameter,
	this simulation is currently not supported for GRANT WITH GRANT OPTION permissions
	when there are non-dbo users who granted permissions to other users.

===============================================
@CloneDatabaseName_ForRevokeSimulation
===============================================
	Specify a non-existent database name in this parameter to use DBCC CLONEDATABASE
	for each checked database, and use that clone for the REVOKE simulation check.
	This will avoid any impact on production databases.

===============================================
@Verbose
===============================================
	If enabled (set to 1), runtime debug details will be returned in the messages output.
*/
DECLARE
	@FilterByDatabaseName sysname = NULL,
	@EnableRevokeSimulation_To_CheckForAffectedUsers bit = 0,
	@CloneDatabaseName_ForRevokeSimulation sysname = 'RevokeSimulationCloneDB',
	@Verbose bit = 0


SET NOCOUNT, XACT_ABORT, ARITHABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @EnableRevokeSimulation_ForWithGrantOption bit = 0;

IF @CloneDatabaseName_ForRevokeSimulation IS NOT NULL AND @EnableRevokeSimulation_To_CheckForAffectedUsers = 1
BEGIN
	IF SERVERPROPERTY('Edition') = 'SQL Azure' OR (CONVERT(VARCHAR, (@@microsoftversion / 0x1000000) & 0xff)) < 11
	BEGIN
		RAISERROR(N'Sorry, CLONEDATABASE is not supported by this version of SQL Server.',16,1);
		SET @EnableRevokeSimulation_To_CheckForAffectedUsers = 0;
	END
	ELSE IF DB_ID(@CloneDatabaseName_ForRevokeSimulation) IS NOT NULL
	BEGIN
		RAISERROR(N'Database "%s" already exists. Please drop it first, or enter a different name for the clone database.',16,1,@CloneDatabaseName_ForRevokeSimulation);
		SET @EnableRevokeSimulation_To_CheckForAffectedUsers = 0;
	END
	ELSE
	BEGIN
		SET @EnableRevokeSimulation_ForWithGrantOption = 1;
	END
END

IF OBJECT_ID('tempdb..#securables') IS NOT NULL DROP TABLE #securables;
CREATE TABLE #securables
(
[id] int NOT NULL IDENTITY(1,1) PRIMARY KEY,
[database_name] SYSNAME COLLATE database_default NULL,
[permission_state] NVARCHAR(256) COLLATE database_default NULL,
[permission] NVARCHAR(256) COLLATE database_default NULL,
[object_type] SYSNAME COLLATE database_default NULL,
[object_name] SYSNAME COLLATE database_default NULL,
[sub_object_name] NVARCHAR(4000) COLLATE database_default NULL
);
IF OBJECT_ID('tempdb..#dbusers') IS NOT NULL DROP TABLE #dbusers;
CREATE TABLE #dbusers
(
[id] int NOT NULL IDENTITY(1,1) PRIMARY KEY,
[database_name] SYSNAME COLLATE database_default NULL,
[user_name] sysname COLLATE database_default NULL,
[sid] varbinary(100) NULL,
[grantor_count] int NULL
);

DECLARE @CurrDB sysname, @CMD nvarchar(max), @spExecuteSQL nvarchar(1000);

SET @CMD = N'INSERT INTO #securables
SELECT DISTINCT
db_name() AS [database_name]
, perms.state_desc
, perms.permission_name
, REPLACE(perms.class_desc, ''_'', '' '') AS [object_type]
,CASE perms.class
WHEN 0 THEN DB_NAME() -- database
WHEN 1 THEN QUOTENAME(OBJECT_SCHEMA_NAME(major_id)) + N''.'' + QUOTENAME(OBJECT_NAME(major_id)) -- object
WHEN 3 THEN schema_name(major_id) -- schema
WHEN 4 THEN printarget.name -- principal
WHEN 5 THEN asm.name -- assembly
WHEN 6 THEN type_name(major_id) -- type
WHEN 10 THEN xmlsc.name -- xml schema
WHEN 15 THEN msgt.name COLLATE DATABASE_DEFAULT -- message types
WHEN 16 THEN svcc.name COLLATE DATABASE_DEFAULT -- service contracts
WHEN 17 THEN svcs.name COLLATE DATABASE_DEFAULT -- services
WHEN 18 THEN rsb.name COLLATE DATABASE_DEFAULT -- remote service bindings
WHEN 19 THEN rts.name COLLATE DATABASE_DEFAULT -- routes
WHEN 23 THEN ftc.name -- full text catalog
WHEN 24 THEN sym.name -- symmetric key
WHEN 25 THEN crt.name -- certificate
WHEN 26 THEN asym.name -- assymetric key
END AS [object_name],
(
SELECT QUOTENAME(c.name)
FROM sys.all_columns AS c
WHERE c.object_id = perms.major_id
AND c.column_id = perms.minor_id
)
FROM sys.database_permissions AS perms
LEFT JOIN sys.database_principals AS prin ON perms.grantee_principal_id = prin.principal_id
LEFT JOIN sys.assemblies AS asm ON perms.major_id = asm.assembly_id
LEFT JOIN sys.xml_schema_collections AS xmlsc ON perms.major_id = xmlsc.xml_collection_id
LEFT JOIN sys.service_message_types AS msgt ON perms.major_id = msgt.message_type_id
LEFT JOIN sys.service_contracts AS svcc ON perms.major_id = svcc.service_contract_id
LEFT JOIN sys.services AS svcs ON perms.major_id = svcs.service_id
LEFT JOIN sys.remote_service_bindings AS rsb ON perms.major_id = rsb.remote_service_binding_id
LEFT JOIN sys.routes AS rts ON perms.major_id = rts.route_id
LEFT JOIN sys.database_principals AS printarget ON perms.major_id = printarget.principal_id
LEFT JOIN sys.symmetric_keys AS sym ON perms.major_id = sym.symmetric_key_id
LEFT JOIN sys.asymmetric_keys AS asym ON perms.major_id = asym.asymmetric_key_id
LEFT JOIN sys.certificates AS crt ON perms.major_id = crt.certificate_id
LEFT JOIN sys.fulltext_catalogs AS ftc ON perms.major_id = ftc.fulltext_catalog_id
WHERE perms.grantee_principal_id = DATABASE_PRINCIPAL_ID(''public'')
AND [state] IN (''G'',''W'')
-- ignoring EXECUTE and REFERENCES permissions on user data types:
AND NOT (
perms.class = 6
AND permission_name IN (''EXECUTE'',''REFERENCES'')
)
-- ignoring Column Encryption permissions:
AND NOT (
perms.class = 0
AND prin.name = ''public''
AND perms.major_id = 0
AND perms.minor_id = 0
AND permission_name IN (
    ''VIEW ANY COLUMN ENCRYPTION KEY DEFINITION''
    ,''VIEW ANY COLUMN MASTER KEY DEFINITION''
    )
)
-- ignoring dbo.dtproperties:
AND NOT (
perms.class = 1
AND permission_name IN (''DELETE'', ''SELECT'',''INSERT'',''UPDATE'',''REFERENCES'')
AND OBJECT_SCHEMA_NAME(major_id) = ''dbo''
AND OBJECT_NAME(major_id) IN (''dtproperties'')
)
-- ignoring built-in object permissions which are too numerous to exclude individually:
AND NOT (
perms.class = 1
AND permission_name IN (''EXECUTE'', ''SELECT'')
AND (
        OBJECTPROPERTY(major_id, ''IsMSShipped'') = 1
     OR OBJECT_NAME(major_id) LIKE ''sp_DTA_%''
     OR OBJECT_NAME(major_id) LIKE ''$%$srvproperty''
     OR OBJECT_NAME(major_id) LIKE ''$%dbproperty''
     OR OBJECT_NAME(major_id) LIKE ''xp_jdbc_%''
     OR OBJECT_SCHEMA_NAME(major_id) IN (''catalog'',''internal'',''MS_PerfDashboard'')
     OR OBJECT_NAME(major_id) IN (
        ''fn_diagramobjects'',''sp_alterdiagram'',''rds_drop_database'',
	''sp_creatediagram'',''sp_dropdiagram'',''sp_renamediagram'',
	''sp_helpdiagramdefinition'',''sp_helpdiagrams''
	)
    )
)

IF @@ROWCOUNT > 0
  INSERT INTO #dbusers
  SELECT DB_NAME(), name, sid
  , GrantorCount = (SELECT COUNT(*) FROM sys.database_permissions AS perm WHERE perm.grantor_principal_id = usr.principal_id)
  FROM sys.database_principals AS usr
  WHERE is_fixed_role = 0
  AND usr.principal_id <> 1 AND [sid] > 0x01
  AND type IN (''A'', ''E'', ''S'', ''U'')';

IF @Verbose = 1 RAISERROR(N'Finding vulnerable securables...',0,1) WITH NOWAIT;

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE state = 0
AND (@FilterByDatabaseName IS NULL OR [name] = @FilterByDatabaseName)
AND HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
AND (DB_ID('rdsadmin') IS NULL OR [name] <> 'model');

OPEN DBs;

WHILE 1=1
BEGIN
  
  FETCH NEXT FROM DBs INTO @CurrDB;
  IF @@FETCH_STATUS <> 0 BREAK;
  
  SET @spExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql';
  
  IF @Verbose = 1 RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
  EXEC @spExecuteSQL @CMD WITH RECOMPILE; -- don't cache exec plans
  RAISERROR(N'Found %d vulnerable securable(s) for database "%s"',0,1,@@ROWCOUNT,@CurrDB) WITH NOWAIT;
  
END

CLOSE DBs;
DEALLOCATE DBs;

IF OBJECT_ID('tempdb..#results') IS NOT NULL DROP TABLE #results;
CREATE TABLE #results
(
	SecurableId int NOT NULL,
	UserName sysname NULL
);

IF @EnableRevokeSimulation_To_CheckForAffectedUsers = 1
BEGIN
DECLARE @CurrSecurableId int, @CurrUsername sysname, @CurrSID uniqueidentifier, @CurrRevokeCmd nvarchar(MAX), @CurrGrantCmd nvarchar(MAX), @CurrState sysname
DECLARE @CMD2 nvarchar(MAX)

SET @CMD = N'EXECUTE AS USER = @Username;

INSERT INTO #results (SecurableId, UserName)
SELECT @CurrSecurableId, @Username
FROM #securables
WHERE id = @CurrSecurableId
AND 1 NOT IN
(
 HAS_PERMS_BY_NAME([object_name], ISNULL(NULLIF([object_type],N''OBJECT OR COLUMN''), ''OBJECT''), [permission])
,HAS_PERMS_BY_NAME([object_name], ''OBJECT'', [permission], [sub_object_name], ''COLUMN'')
);

SET @RCount = @@ROWCOUNT;

REVERT;'

DECLARE Securables CURSOR
LOCAL FAST_FORWARD
FOR
SELECT id
, permission_state
, RevokeCmd = N'REVOKE ' + [permission] + ' ON '
+ ISNULL(NULLIF([object_type], N'OBJECT OR COLUMN')
+ '::' + QUOTENAME([object_name]), [object_name]) + ISNULL(N'(' + sub_object_name + N')','')
+ N' FROM [public]'
+ CASE WHEN permission_state = 'GRANT' THEN N'' ELSE N' CASCADE' END
+ N';'
, GrantCmd = N'GRANT ' + [permission] + ' ON '
+ ISNULL(NULLIF([object_type], N'OBJECT OR COLUMN')
+ '::' + QUOTENAME([object_name]), [object_name]) + ISNULL(N'(' + sub_object_name + N')','')
+ N' TO [public]'
+ CASE WHEN permission_state = 'GRANT' THEN N'' ELSE N' WITH GRANT OPTION' END
+ N';'
,[database_name]
FROM #securables;

OPEN Securables

WHILE 1=1
BEGIN
	FETCH NEXT FROM Securables INTO @CurrSecurableId, @CurrState, @CurrRevokeCmd, @CurrGrantCmd, @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	IF @CloneDatabaseName_ForRevokeSimulation IS NOT NULL
	BEGIN
		-- Clone target database without data
		IF @Verbose = 1 RAISERROR(N'DBCC CLONEDATABASE([%s], [%s]);',0,1,@CurrDB, @CloneDatabaseName_ForRevokeSimulation) WITH NOWAIT;
		DBCC CLONEDATABASE(@CurrDB, @CloneDatabaseName_ForRevokeSimulation);

		-- Enable the cloned database to be writeable
		SET @CMD2 = N'ALTER DATABASE ' + QUOTENAME(@CloneDatabaseName_ForRevokeSimulation) + N' SET READ_WRITE WITH NO_WAIT;'
		IF @Verbose = 1 RAISERROR(N'%s',0,1,@CMD2) WITH NOWAIT;
		EXEC (@CMD2);

		SET @spExecuteSQL = QUOTENAME(@CloneDatabaseName_ForRevokeSimulation) + N'..sp_executesql'
	END
	ELSE
	BEGIN
		SET @spExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql'
	END
	
	RAISERROR(N'%s -- Checking in database "%s" (%s) --', 0,1, @CurrRevokeCmd, @CurrDB, @spExecuteSQL) WITH NOWAIT;

	DECLARE UsersToCheck CURSOR
	LOCAL FAST_FORWARD
	FOR
	SELECT [user_name], [sid]
	FROM #dbusers
	WHERE [database_name] = @CurrDB
	AND ([grantor_count] = 0 OR @EnableRevokeSimulation_ForWithGrantOption = 1)
	;

	OPEN UsersToCheck;

	WHILE 1=1
	BEGIN
		FETCH NEXT FROM UsersToCheck INTO @CurrUsername, @CurrSID;
		IF @@FETCH_STATUS <> 0 BREAK;

		IF @Verbose = 1 RAISERROR(N'-- Checking for username "%s" in database "%s" (%s) --', 0,1, @CurrUsername, @CurrDB, @spExecuteSQL) WITH NOWAIT;

		IF SUSER_SNAME(@CurrSID) IS NULL
		BEGIN
			RAISERROR(N'-- No login found for username "%s" in database "%s". It is probably orphaned. --', 0,1, @CurrUsername, @CurrDB) WITH NOWAIT;
			CONTINUE;
		END

		DECLARE @RCount int;

		BEGIN TRY
			BEGIN TRAN;
			
			EXEC @spExecuteSQL @CurrRevokeCmd;

			RAISERROR(N'-- Checking revoke impact for user "%s" (securable Id %d)...',0,1,@CurrUsername, @CurrSecurableId) WITH NOWAIT;
			EXEC @spExecuteSQL @CMD, N'@Username sysname, @CurrSecurableId int, @RCount int OUTPUT', @CurrUsername, @CurrSecurableId, @RCount OUTPUT;

			IF @Verbose = 1 
			BEGIN
				IF @RCount = 0
					RAISERROR(N'-- OK --',0,1) WITH NOWAIT;
				ELSE
					RAISERROR(N'-- !!! WARNING !!! Found %d missing permission(s) --',0,1,@RCount) WITH NOWAIT;
			END

			COMMIT TRAN;
		END TRY
		BEGIN CATCH
			PRINT N'ERROR at line ' + CONVERT(nvarchar(MAX), ERROR_LINE()) + N': ' + ERROR_MESSAGE();
			WHILE @@TRANCOUNT > 0 ROLLBACK;
		END CATCH

		REVERT;
		
		IF @Verbose = 1 PRINT @CurrGrantCmd;
		EXEC @spExecuteSQL @CurrGrantCmd;

	END

	CLOSE UsersToCheck;
	DEALLOCATE UsersToCheck;
	
	IF @CloneDatabaseName_ForRevokeSimulation IS NOT NULL AND DB_ID(@CloneDatabaseName_ForRevokeSimulation) IS NOT NULL
	BEGIN
		-- Drop cloned database
		SET @CMD2 = N'DROP DATABASE ' + QUOTENAME(@CloneDatabaseName_ForRevokeSimulation)
		IF @Verbose = 1 RAISERROR(N'%s',0,1,@CMD2) WITH NOWAIT;
		EXEC(@CMD2);
	END
END

CLOSE Securables
DEALLOCATE Securables

END

SELECT @@SERVERNAME AS [server_name],
[database_name],
[permission_state],
[permission],
ISNULL([object_type],'(unknown type)') AS [object_type],
ISNULL([object_name], '(unknown object)') AS [object_name],
sub_object_name
, NegativelyAffectedUsers =
	STUFF((
		SELECT N', ' + QUOTENAME(r.UserName)
		FROM #results AS r
		WHERE r.SecurableId = s.id
		FOR XML PATH('')
	), 1, 2, N'')
, RevokeCmd = N'USE ' + QUOTENAME([database_name]) + N'; REVOKE ' + [permission] + ' ON '
+ ISNULL(NULLIF([object_type], N'OBJECT OR COLUMN')
+ '::' + QUOTENAME([object_name]), [object_name]) + ISNULL(N'(' + sub_object_name + N')','')
+ N' FROM [public]'
+ CASE WHEN permission_state = 'GRANT' THEN N'' ELSE N' CASCADE' END
+ N';'
, GrantCmd = N'USE ' + QUOTENAME([database_name]) + N'; GRANT ' + [permission] + ' ON '
+ ISNULL(NULLIF([object_type], N'OBJECT OR COLUMN')
+ '::' + QUOTENAME([object_name]), [object_name]) + ISNULL(N'(' + sub_object_name + N')','')
+ N' TO [public]'
+ CASE WHEN permission_state = 'GRANT' THEN N'' ELSE N' WITH GRANT OPTION' END
+ N';'
FROM #securables AS s
--WHERE [object_name] IS NOT NULL

IF @EnableRevokeSimulation_To_CheckForAffectedUsers = 1 AND EXISTS 
	(
	SELECT *
	FROM #securables AS s
	INNER JOIN #dbusers AS u ON s.database_name = u.database_name AND u.grantor_count > 0
	WHERE s.permission_state = 'GRANT_WITH_GRANT_OPTION'
	)
AND @CloneDatabaseName_ForRevokeSimulation IS NULL
BEGIN
	RAISERROR(N'WARNING: Detected "GRANT WITH GRANT OPTION" permissions and database users who granted permissions to other users. This is not supported for simulation without using CLONEDATABASE. Results are inconclusive.', 15, 1);
END
ELSE IF @EnableRevokeSimulation_To_CheckForAffectedUsers = 0 AND EXISTS (SELECT * FROM #results)
BEGIN
	RAISERROR(N'WARNING: Revoke Simulation was not performed! Negatively affected users are unknown!', 15, 1);
END
ELSE IF EXISTS (SELECT * FROM #results)
BEGIN
	RAISERROR(N'WARNING: Negatively affected users found! Please review results and GRANT direct permissions or role membership before revoking from PUBLIC!', 15, 1);
END