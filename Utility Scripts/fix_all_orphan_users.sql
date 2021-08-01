/*
Author: Eitan Blumin | https://eitanblumin.com
Date Created: 2018-01-02
Last Update: 2021-07-28
Description:
	Fix All Orphaned Users Within Current Database, or all databases in the instance.
	Handles 3 possible use-cases:
	1. Login with same name as user exists - generate ALTER LOGIN to map the user to the login.
	2. No login with same name exists - generate DROP USER to delete the orphan user.
	3. Orphan user is [dbo] - change the database owner to SA (or whatever SA was renamed to)

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

DECLARE @tmp AS TABLE(DBName SYSNAME NULL, UserName NVARCHAR(MAX), LoginExists BIT, OwnedSchemas NVARCHAR(MAX));
INSERT INTO @tmp
exec sp_MSforeachdb 'IF DATABASEPROPERTYEX(''?'', ''Status'') = ''ONLINE''
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
AND dp.authentication_type <> 0;'


SELECT DBWriteable = CASE WHEN DATABASEPROPERTYEX(DBName,'Updateability') = 'READ_WRITE' THEN 1 ELSE 0 END
, DBName, UserName, LoginExists --, OwnedSchemas
, RemediationCmd =
CASE WHEN UserName = 'dbo' THEN
	N'USE ' + QUOTENAME(DBName) + N'; ALTER AUTHORIZATION ON DATABASE::' + QUOTENAME(DBName) + N' TO ' + QUOTENAME(@saName) + N' -- assign orphaned [dbo] to [sa]'
WHEN LoginExists = 0 THEN
	N'USE ' + QUOTENAME(DBName) + N'; ' + ISNULL(OwnedSchemas, N'') + N' DROP USER ' + QUOTENAME(UserName) + N' -- no existing login found'
ELSE
	N'USE ' + QUOTENAME(DBName) + N'; ALTER USER ' + QUOTENAME(UserName) + N' WITH LOGIN = ' + QUOTENAME(UserName) + N' -- existing login found'
END
FROM @tmp
WHERE (DBName = @Database OR @Database IS NULL)
AND (@WriteableDBsOnly = 0 OR DATABASEPROPERTYEX(DBName,'Updateability') = 'READ_WRITE')
ORDER BY DBWriteable DESC, DBName, UserName

IF @@ROWCOUNT = 0 PRINT N'No orphan users found!'