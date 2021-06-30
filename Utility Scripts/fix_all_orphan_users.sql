/*
Author: Eitan Blumin | https://eitanblumin.com
Date Created: 2018-01-02
Last Update: 2021-06-30
Description:
	Fix All Orphaned Users Within Current Database, or all databases in the instance.
	Handles 3 possible use-cases:
	1. Login with same name as user exists - generate ALTER LOGIN to map the user to the login.
	2. No login with same name exists - generate DROP USER to delete the orphan user.
	3. Orphan user is [dbo] - change the database owner to SA (or whatever SA was renamed to)

More info: https://eitanblumin.com/2018/10/31/t-sql-script-to-fix-orphaned-db-users-easily/
*/
DECLARE
	 @Database	SYSNAME		= NULL	-- Filter by a specific database. Leave NULL for all databases.


SET NOCOUNT ON;

-- Variable declaration
DECLARE @user NVARCHAR(MAX), @loginExists BIT, @saName SYSNAME, @ownedSchemas NVARCHAR(MAX);

-- Find the actual name of the "sa" login
SELECT @saName = SUSER_NAME(0x01);

DECLARE @tmp AS TABLE(DBName SYSNAME NULL, UserName NVARCHAR(MAX), LoginExists BIT, OwnedSchemas NVARCHAR(MAX));
INSERT INTO @tmp
exec sp_MSforeachdb 'IF DATABASEPROPERTYEX(''?'', ''Status'') = ''ONLINE'' AND DATABASEPROPERTYEX(''?'', ''Updateability'') = ''READ_WRITE''
SELECT ''?'', dp.name AS user_name
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
AND DATABASEPROPERTYEX(''?'',''Updateability'') = ''READ_WRITE'';'

IF EXISTS (SELECT NULL FROM @tmp WHERE DBName = @Database OR @Database IS NULL)
BEGIN
	DECLARE Orphans CURSOR FOR
	SELECT DBName, UserName, LoginExists, OwnedSchemas
	FROM @tmp
	WHERE DBName = @Database OR @Database IS NULL;

	OPEN Orphans
	FETCH NEXT FROM Orphans INTO @Database, @user, @loginExists, @ownedSchemas

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 DECLARE @Command NVARCHAR(MAX)

	 IF @user = 'dbo'
		SET @Command = N'USE ' + QUOTENAME(@Database) + N'; ALTER AUTHORIZATION ON DATABASE::' + QUOTENAME(@Database) + N' TO ' + QUOTENAME(@saName) + N' -- assign orphaned [dbo] to [sa]'
	 ELSE IF @loginExists = 0
		SET @Command = N'USE ' + QUOTENAME(@Database) + N'; ' + ISNULL(@ownedSchemas, N'') + N' DROP USER ' + QUOTENAME(@user) + N' -- no existing login found'
	 ELSE
		SET @Command = N'USE ' + QUOTENAME(@Database) + N'; ALTER USER ' + QUOTENAME(@user) + N' WITH LOGIN = ' + QUOTENAME(@user) + N' -- existing login found'
 
	 PRINT @Command;
	 --EXEC (@Command);

	FETCH NEXT FROM Orphans INTO @Database, @user, @loginExists, @ownedSchemas
	END

	CLOSE Orphans
	DEALLOCATE Orphans
END
ELSE
	PRINT N'No orphan users found!'