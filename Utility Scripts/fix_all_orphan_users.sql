/*
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date Created: 2018-01-02
Last Update: 2023-06-18
Description:
	Fix All Orphaned Users Within Current Database, or all databases in the instance.
	Handles 3 possible use-cases:
	1. Login with same name as user exists - generate ALTER LOGIN to map the user to the login.
	2. Login with a different name but the same sid exists - generate ALTER LOGIN to map the user to the login.
	3. Login SID is identifiable but login doesn't exist in SQL - generate CREATE LOGIN FROM WINDOWS to create a Windows authentication login.
	4. No login with same name exists - generate DROP USER to delete the orphan user.
	5. Orphan user is [dbo] - change the database owner to SA (or whatever SA was renamed to)

Remarks:
	- The script tries to detect automatically whether a user is a member of a Windows Group.
	- The script automatically detects whether the user owns schemas and objects, and generates remediation commands accordingly.
		See parameters @DropEmptyOwnedSchemas and @DropOwnedObjects for more details.

More info: https://eitanblumin.com/2018/10/31/t-sql-script-to-fix-orphaned-db-users-easily/
*/
DECLARE
	    @Database							sysname	= NULL	-- Filter by a specific database. Leave NULL for all databases.
	 ,  @WriteableDBsOnly					bit	= 0			-- Set to 1 to ignore read-only databases.
	 ,  @DropEmptyOwnedSchemas				bit	= 1			-- Set to 1 to drop schemas without objects if an orphan user owns them. Otherwise, change their owner to [dbo]. Not relevant if user is dbo.
	 ,  @DropOwnedObjects					bit	= 1			-- Set to 1 to drop objects if an orphan user owns them. Otherwise, change their schema to [dbo]. Not relevant if user is dbo.
	 ,  @CreateWindowsAccountsWhenPossible	bit = 1			-- Set to 1 to generate a CREATE LOGIN ... FROM WINDOWS command when the login SID is identifiable.

SET NOCOUNT ON;

-- Variable declaration
DECLARE @saName SYSNAME, @Cmd NVARCHAR(MAX);

SET @Cmd = N'SELECT DB_NAME(), dp.name AS user_name
, dp.[sid]
, CASE WHEN dp.name IN (SELECT name COLLATE database_default FROM sys.server_principals) THEN 1 ELSE 0 END AS LoginExists
, OwnedSchemas = (
SELECT cmd + N''; ''
FROM
(
SELECT cmd = '''
+ CASE WHEN @DropEmptyOwnedSchemas = 1 AND @DropOwnedObjects = 1 THEN N'DROP SCHEMA '' + QUOTENAME(sch.name)
FROM sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id'
ELSE N'ALTER AUTHORIZATION ON SCHEMA::'' + QUOTENAME(sch.name) + N'' TO [dbo]''
FROM sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id'
+ CASE WHEN @DropEmptyOwnedSchemas = 1 THEN N'
AND EXISTS (SELECT NULL FROM sys.objects AS obj WHERE obj.schema_id = sch.schema_id)
UNION ALL
SELECT ''DROP SCHEMA '' + QUOTENAME(sch.name)
FROM sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id
AND NOT EXISTS (SELECT NULL FROM sys.objects AS obj WHERE obj.schema_id = sch.schema_id)'
ELSE N'' END
END + N'
) AS s
FOR XML PATH ('''')
)
, OwnedObjects = ' + CASE WHEN @DropEmptyOwnedSchemas = 1 THEN N'
(SELECT cmd + N''; ''
FROM
(
SELECT cmd = '''
+ CASE WHEN @DropOwnedObjects = 1 THEN N'DROP '' 
+ CASE obj.type WHEN ''V'' THEN N''VIEW'' WHEN ''U'' THEN N''TABLE'' WHEN ''P'' THEN N''PROCEDURE'' ELSE N''FUNTION'' END'
ELSE N'ALTER SCHEMA [dbo] TRANSFER''' END + N'
+ N'' '' + QUOTENAME(sch.name) + N''.'' + QUOTENAME(obj.name)
+ ISNULL(N'' /* '' + CONVERT(nvarchar(max), SUM(p.rows)) + N'' rows */'', N'''')
FROM sys.schemas AS sch
INNER JOIN sys.objects AS obj ON obj.schema_id = sch.schema_id
LEFT JOIN sys.partitions AS p ON obj.object_id = p.object_id AND p.index_id <= 1
WHERE sch.principal_id = dp.principal_id AND sch.name <> ''dbo'' AND obj.is_ms_shipped = 0
AND obj.parent_object_id = 0
GROUP BY obj.type, sch.name, obj.name
) AS s
FOR XML PATH ('''')
)' ELSE N'NULL' END + N'
FROM sys.database_principals AS dp 
LEFT JOIN sys.server_principals AS sp ON dp.SID = sp.SID 
WHERE sp.SID IS NULL 
AND dp.type IN (''S'',''U'',''G'') AND dp.sid > 0x01
AND dp.authentication_type <> 0;'

-- Find the actual name of the "sa" login (in case it was renamed)
SET @saName = SUSER_NAME(0x01);

-- Find any Windows Group members:
DECLARE @AdminsByGroup AS TABLE (AccountName sysname NOT NULL, AccountType sysname NULL, privilege sysname NULL, MappedName sysname NULL, GroupPath sysname NULL);
DECLARE @CurrentGroup sysname;

DECLARE Groups CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.server_principals
WHERE [type] = 'G'

OPEN Groups;

WHILE 1=1
BEGIN
	FETCH NEXT FROM Groups INTO @CurrentGroup;
	IF @@FETCH_STATUS <> 0 BREAK;

	BEGIN TRY
		INSERT INTO @AdminsByGroup
		EXEC master..xp_logininfo 
			@acctname = @CurrentGroup,
			@option = 'members';
	END TRY
	BEGIN CATCH
		PRINT N'Error while retrieving members of ' + @CurrentGroup + N'; ' + ERROR_MESSAGE()
	END CATCH
END

CLOSE Groups;
DEALLOCATE Groups;

DECLARE @results AS TABLE(DBName SYSNAME NULL, UserName SYSNAME NULL, [sid] VARBINARY(128) NULL, LoginExists bit NULL, OwnedSchemas NVARCHAR(MAX) NULL, OwnedObjects NVARCHAR(MAX) NULL);
DECLARE @CurrDB sysname, @spExecuteSql nvarchar(1000);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND (@Database IS NULL OR [name] = @Database)
AND (@WriteableDBsOnly = 0 OR DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE')

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @results
	EXEC @spExecuteSql @Cmd
END

CLOSE DBs;
DEALLOCATE DBs;


SELECT ServerName = CONVERT(sysname, SERVERPROPERTY('ServerName'))
, DBName
, DBWriteable = CASE WHEN DATABASEPROPERTYEX(DBName,'Updateability') = 'READ_WRITE' THEN 1 ELSE 0 END
, UserName, LoginExists --, OwnedSchemas, OwnedObjects
, LoginName = l.LoginName
, MemberOfGroups = STUFF((
		SELECT N', ' + QUOTENAME(GroupPath)
		FROM (SELECT DISTINCT GroupPath FROM @AdminsByGroup AS g WHERE g.AccountName = l.LoginName) AS gg
		FOR XML PATH('')
		), 1, 2, N'')
, RemediationCmd =
CASE WHEN UserName = 'dbo' THEN
	N'USE ' + QUOTENAME(DBName) + N'; ALTER AUTHORIZATION ON DATABASE::' + QUOTENAME(DBName) + N' TO ' + QUOTENAME(@saName) + N'; '
	+ CASE WHEN LoginExists = 1 THEN N' CREATE USER ' + QUOTENAME(UserName) + N' WITH LOGIN = ' + QUOTENAME(UserName) + N'; ALTER ROLE [db_owner] ADD USER ' + QUOTENAME(UserName) + N';'
	ELSE N'' END
	+ N'-- assign orphaned [dbo] to [sa]'
WHEN SUSER_ID(l.LoginName) IS NOT NULL THEN
	N'USE ' + QUOTENAME(DBName) + N'; ALTER USER ' + QUOTENAME(UserName) + N' WITH LOGIN = ' + QUOTENAME(l.LoginName) + N'; -- existing login found with the same name'
WHEN @CreateWindowsAccountsWhenPossible = 1 AND LoginExists = 0 AND SUSER_SID(l.LoginName) IS NOT NULL  THEN
	N'CREATE LOGIN ' + QUOTENAME( l.LoginName ) + ' FROM WINDOWS WITH DEFAULT_DATABASE = [master]; -- trying to recreate a Windows account'
WHEN LoginExists = 0 THEN
	N'USE ' + QUOTENAME(DBName) + N'; '
		+ ISNULL(OwnedObjects, N'') + ISNULL(OwnedSchemas, N'')
		+ N' DROP USER ' + QUOTENAME(UserName) + N'; -- no existing login found'
ELSE
	N'USE ' + QUOTENAME(DBName) + N'; ALTER USER ' + QUOTENAME(UserName) + N' WITH LOGIN = ' + QUOTENAME(UserName) + N'; -- existing login found with a different sid'
END + ISNULL(N', but the login ' + QUOTENAME(l.LoginName) + N' is a member of: ' + STUFF((
		SELECT N', ' + QUOTENAME(GroupPath)
		FROM (SELECT DISTINCT GroupPath FROM @AdminsByGroup AS g WHERE g.AccountName = ISNULL(SUSER_SNAME([sid]), SUSER_SNAME(SUSER_SID(UserName)))) AS gg
		FOR XML PATH('')
		), 1, 2, N''), N'')
, CreateWindowsLoginCmd = N'CREATE LOGIN ' + QUOTENAME( l.LoginName ) + ' FROM WINDOWS WITH DEFAULT_DATABASE = [master];'
, CreateSQLLoginCmd = N'CREATE LOGIN ' + QUOTENAME( l.LoginName ) + CHAR(13) + CHAR(10) + ' WITH PASSWORD = '
	+ ISNULL(CONVERT(nvarchar(max), CAST( LOGINPROPERTY( l.LoginName, 'PasswordHash' ) AS varbinary (max)), 1) + ' HASHED', N'N''change_me''')
	+ N', SID = ' +  CONVERT(nvarchar(max), [sid], 1) + CHAR(13) + CHAR(10) + ', DEFAULT_DATABASE = ' + QUOTENAME( ISNULL(CONVERT(sysname, LOGINPROPERTY( l.LoginName, 'DefaultDatabase')), DB_NAME()) )
   + N', CHECK_POLICY = ' + CASE WHEN CAST(LOGINPROPERTY( l.LoginName, 'HistoryLength' ) AS int) <> 0 THEN N'ON' ELSE N'OFF' END
   + N', CHECK_EXPIRATION = ' + CASE WHEN LOGINPROPERTY( l.LoginName, 'DaysUntilExpiration' ) IS NOT NULL THEN N'ON' ELSE N'OFF' END
   + N';'
FROM @results
CROSS APPLY
(VALUES(COALESCE(SUSER_SNAME([sid]), SUSER_SNAME(SUSER_SID(UserName)), UserName))) AS l(LoginName)
ORDER BY DBWriteable DESC, DBName, UserName

IF @@ROWCOUNT = 0 PRINT N'No orphan users found!'