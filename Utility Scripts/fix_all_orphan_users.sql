/*
Author: Eitan Blumin | https://eitanblumin.com
Date Created: 2018-01-02
Last Update: 2021-08-09
Description:
	Fix All Orphaned Users Within Current Database, or all databases in the instance.
	Handles 3 possible use-cases:
	1. Login with same name as user exists - generate ALTER LOGIN to map the user to the login.
	2. No login with same name exists - generate DROP USER to delete the orphan user.
	3. Orphan user is [dbo] - change the database owner to SA (or whatever SA was renamed to)

	The script also tries to detect automatically whether a user is a member of a Windows Group.

More info: https://eitanblumin.com/2018/10/31/t-sql-script-to-fix-orphaned-db-users-easily/
*/
DECLARE
	    @Database		SYSNAME		= NULL	-- Filter by a specific database. Leave NULL for all databases.
	 ,  @WriteableDBsOnly	BIT		= 1	-- Ignore read-only databases or not.

SET NOCOUNT ON;

-- Variable declaration
DECLARE @user NVARCHAR(MAX), @loginExists BIT, @saName SYSNAME, @ownedSchemas NVARCHAR(MAX);

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

DECLARE @tmp AS TABLE(DBName SYSNAME NULL, UserName NVARCHAR(MAX), [sid] VARBINARY(128), LoginExists BIT, OwnedSchemas NVARCHAR(MAX));
INSERT INTO @tmp
exec sp_MSforeachdb 'IF HAS_DBACCESS(''?'') = 1
SELECT ''?'', dp.name AS user_name
, dp.[sid]
, CASE WHEN dp.name IN (SELECT name COLLATE database_default FROM sys.server_principals) THEN 1 ELSE 0 END AS LoginExists
, OwnedSchemas = (
SELECT cmd + N''; ''
FROM
(
SELECT cmd = ''ALTER AUTHORIZATION ON SCHEMA::'' + QUOTENAME(sch.name) + N'' TO [dbo]''
FROM [?].sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id
AND EXISTS (SELECT NULL FROM [?].sys.objects AS obj WHERE obj.schema_id = sch.schema_id)
UNION ALL
SELECT ''DROP SCHEMA '' + QUOTENAME(sch.name)
FROM [?].sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id
AND NOT EXISTS (SELECT NULL FROM [?].sys.objects AS obj WHERE obj.schema_id = sch.schema_id)
) AS s
FOR XML PATH ('''')
)
FROM [?].sys.database_principals AS dp 
LEFT JOIN sys.server_principals AS sp ON dp.SID = sp.SID 
WHERE sp.SID IS NULL 
AND dp.type IN (''S'',''U'',''G'') AND dp.sid > 0x01
AND dp.authentication_type <> 0;'


SELECT DBWriteable = CASE WHEN DATABASEPROPERTYEX(DBName,'Updateability') = 'READ_WRITE' THEN 1 ELSE 0 END
, DBName, UserName, LoginExists --, OwnedSchemas
, LoginName = ISNULL(SUSER_SNAME([sid]), SUSER_SNAME(SUSER_SID(UserName)))
, MemberOfGroups = STUFF((
		SELECT N', ' + QUOTENAME(GroupPath)
		FROM (SELECT DISTINCT GroupPath FROM @AdminsByGroup AS g WHERE g.AccountName = ISNULL(SUSER_SNAME([sid]), SUSER_SNAME(SUSER_SID(UserName)))) AS gg
		FOR XML PATH('')
		), 1, 2, N'')
, RemediationCmd =
CASE WHEN UserName = 'dbo' THEN
	N'USE ' + QUOTENAME(DBName) + N'; ALTER AUTHORIZATION ON DATABASE::' + QUOTENAME(DBName) + N' TO ' + QUOTENAME(@saName) + N'; '
	+ CASE WHEN LoginExists = 1 THEN N' CREATE USER ' + QUOTENAME(UserName) + N' WITH LOGIN = ' + QUOTENAME(UserName) + N'; ALTER ROLE [db_owner] ADD USER ' + QUOTENAME(UserName) + N';'
	ELSE N'' END
	+ N'-- assign orphaned [dbo] to [sa]'
WHEN LoginExists = 0 THEN
	N'USE ' + QUOTENAME(DBName) + N'; ' + ISNULL(OwnedSchemas, N'') + N' DROP USER ' + QUOTENAME(UserName) + N'; -- no existing login found'
ELSE
	N'USE ' + QUOTENAME(DBName) + N'; ALTER USER ' + QUOTENAME(UserName) + N' WITH LOGIN = ' + QUOTENAME(UserName) + N'; -- existing login found with a different sid'
END + ISNULL(N', but the login ' + QUOTENAME(ISNULL(SUSER_SNAME([sid]), SUSER_SNAME(SUSER_SID(UserName)))) + N' is a member of: ' + STUFF((
		SELECT N', ' + QUOTENAME(GroupPath)
		FROM (SELECT DISTINCT GroupPath FROM @AdminsByGroup AS g WHERE g.AccountName = ISNULL(SUSER_SNAME([sid]), SUSER_SNAME(SUSER_SID(UserName)))) AS gg
		FOR XML PATH('')
		), 1, 2, N''), N'')
FROM @tmp
WHERE (DBName = @Database OR @Database IS NULL)
AND (@WriteableDBsOnly = 0 OR DATABASEPROPERTYEX(DBName,'Updateability') = 'READ_WRITE')
ORDER BY DBWriteable DESC, DBName, UserName

IF @@ROWCOUNT = 0 PRINT N'No orphan users found!'