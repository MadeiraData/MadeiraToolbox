/*
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date Created: 2018-01-02
Last Update: 2022-02-22
Description:
	Fix All Orphaned Users Within Current Database, or all databases in the instance.
	Handles 3 possible use-cases:
	1. Login with same name as user exists - generate ALTER LOGIN to map the user to the login.
	2. Login with a different name but the same sid exists - generate ALTER LOGIN to map the user to the login.
	3. No login with same name exists - generate DROP USER to delete the orphan user.
	4. Orphan user is [dbo] - change the database owner to SA (or whatever SA was renamed to)

	The script also tries to detect automatically whether a user is a member of a Windows Group.

More info: https://eitanblumin.com/2018/10/31/t-sql-script-to-fix-orphaned-db-users-easily/
*/
DECLARE
	    @Database			SYSNAME	= NULL	-- Filter by a specific database. Leave NULL for all databases.
	 ,  @WriteableDBsOnly		BIT	= 0	-- Ignore read-only databases or not.
	 ,  @DropEmptyOwnedSchemas	bit	= 0	-- Drop schemas without objects if an orphan user owns them. Otherwise, change owner to [dbo].

SET NOCOUNT ON;

-- Variable declaration
DECLARE @saName SYSNAME, @Cmd NVARCHAR(MAX);

SET @Cmd = 'IF HAS_DBACCESS(''?'') = 1'
+ CASE WHEN DB_ID(@Database) IS NOT NULL THEN N'
AND DB_ID(''?'') = DB_ID(''' + @Database + ''')'
ELSE N'' END
+ CASE WHEN @WriteableDBsOnly = 1 THEN N'
AND DATABASEPROPERTYEX(''?'',''Updateability'') = ''READ_WRITE'''
ELSE N'' END
+ N'
SELECT ''?'', dp.name AS user_name
, dp.[sid]
, CASE WHEN dp.name IN (SELECT name COLLATE database_default FROM sys.server_principals) THEN 1 ELSE 0 END AS LoginExists
, OwnedSchemas = (
SELECT cmd + N''; ''
FROM
(
SELECT cmd = ''ALTER AUTHORIZATION ON SCHEMA::'' + QUOTENAME(sch.name) + N'' TO [dbo]''
FROM [?].sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id'
+ CASE WHEN @DropEmptyOwnedSchemas = 1 THEN N'
AND EXISTS (SELECT NULL FROM [?].sys.objects AS obj WHERE obj.schema_id = sch.schema_id)
UNION ALL
SELECT ''DROP SCHEMA '' + QUOTENAME(sch.name)
FROM [?].sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id
AND NOT EXISTS (SELECT NULL FROM [?].sys.objects AS obj WHERE obj.schema_id = sch.schema_id)'
ELSE N'' END + N'
) AS s
FOR XML PATH ('''')
)
FROM [?].sys.database_principals AS dp 
LEFT JOIN sys.server_principals AS sp ON dp.SID = sp.SID 
WHERE sp.SID IS NULL 
AND dp.type IN (''S'',''U'',''G'') AND dp.sid > 0x01
AND dp.authentication_type <> 0;'

-- Find the actual name of the "sa" login
SELECT @saName = SUSER_NAME(0x01);

-- Find any Windows Group members:
DECLARE @AdminsByGroup AS TABLE (AccountName sysname, AccountType sysname, privilege sysname, MappedName sysname, GroupPath sysname);
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

DECLARE @tmp AS TABLE(DBName SYSNAME NULL, UserName SYSNAME, [sid] VARBINARY(128), LoginExists BIT, OwnedSchemas NVARCHAR(MAX));
INSERT INTO @tmp
exec sp_MSforeachdb @Cmd


SELECT DBWriteable = CASE WHEN DATABASEPROPERTYEX(DBName,'Updateability') = 'READ_WRITE' THEN 1 ELSE 0 END
, DBName, UserName, LoginExists --, OwnedSchemas
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
	N'USE ' + QUOTENAME(DBName) + N'; ALTER USER ' + QUOTENAME(UserName) + N' WITH LOGIN = ' + QUOTENAME(l.LoginName) + N'; -- existing login found with the same sid'
WHEN LoginExists = 0 THEN
	N'USE ' + QUOTENAME(DBName) + N'; ' + ISNULL(OwnedSchemas, N'') + N' DROP USER ' + QUOTENAME(UserName) + N'; -- no existing login found'
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
FROM @tmp
CROSS APPLY
(VALUES(COALESCE(SUSER_SNAME([sid]), SUSER_SNAME(SUSER_SID(UserName)), UserName))) AS l(LoginName)
ORDER BY DBWriteable DESC, DBName, UserName

IF @@ROWCOUNT = 0 PRINT N'No orphan users found!'