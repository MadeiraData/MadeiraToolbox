/*
Invalid owner for a system Role, Schema, or Database
====================================================
Author: Eitan Blumin | Madeira Data Solutions | https://www.madeiradata.com
Date: 2020-11-25
Description:

System roles and schemas must have specific owning users or roles.
 
For example, all system database roles such as db_owner, db_datawriter, db_datareader, etc. must be owned by dbo.
All system schemas such as sys, dbo, db_owner, db_datawriter, db_datareader, etc. must be owned by the system role or user of the same name.
 
It's a 3-part relationship like so:
schema X - owned by role X - owned by dbo.
 
If the database is a system database, its owner should be sa (or equivalent, if it was renamed).
 
Invalid owners for such system objects can potentially cause severe errors during version updates/upgrades, or when using certain HADR features.
Additionally, once a system object is owned by a user-created login/user, it becomes very problematic to remove or make changes to such logins/users.

This script will detect any such misconfigurations, and provide you with the proper remediation scripts to fix it.
*/
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @CMD NVARCHAR(MAX), @DBName SYSNAME, @Executor NVARCHAR(1000), @SaName SYSNAME

SET @SaName = SUSER_SNAME(0x01);

SET @CMD = N'SELECT DB_ID(), DB_NAME(), ''SCHEMA'', sch.[name], pr.[name], pr.[sid]
, ExistingMembership = IS_ROLEMEMBER(sch.[name], pr.[name])
FROM sys.schemas AS sch
LEFT JOIN sys.database_principals AS pr ON sch.principal_id = pr.principal_id
WHERE (sch.schema_id >= 16384 OR DB_NAME() = ''msdb'')
AND sch.[name] NOT IN (''SQLSentry'', ''SentryOne'')
AND (pr.principal_id IS NULL
    OR (sch.[name] NOT IN (''managed_backup'',''smart_admin'',''MS_PerfDashboard'') AND sch.[name] <> pr.[name])
    OR (sch.[name] IN (''managed_backup'',''smart_admin'',''MS_PerfDashboard'') AND sch.principal_id <> 1)
    )

UNION ALL

SELECT DB_ID(), DB_NAME(), ''ROLE'', rol.[name], pr.[name], pr.[sid]
, ExistingMembership = (SELECT COUNT(*) FROM sys.database_role_members AS rm WHERE rm.member_principal_id = pr.principal_id AND USER_NAME(rm.role_principal_id) = rol.[name])
FROM sys.database_principals AS rol
LEFT JOIN sys.database_principals AS pr ON rol.owning_principal_id = pr.principal_id
WHERE (rol.principal_id >= 16384 OR DB_NAME() = ''msdb'')
AND rol.type = ''R''
AND (pr.principal_id IS NULL OR rol.owning_principal_id <> 1)

UNION ALL

SELECT DB_ID(), DB_NAME(), ''DATABASE'', DB_NAME(), sp.[name] COLLATE database_default, dp.[sid]
, ExistingMembership = 0
FROM sys.database_principals AS dp
LEFT JOIN sys.server_principals AS sp ON dp.sid = sp.sid
WHERE dp.principal_id = 1
AND DB_ID() <= 4
AND (sp.sid IS NULL OR sp.sid <> 0x01)'

DECLARE @Result AS TABLE
(
[DBId] INT NULL, DBName SYSNAME NULL, ObjType SYSNAME NULL, ObjectName SYSNAME NULL, CurrentOwnerName SYSNAME NULL
, OwnerSID VARBINARY(MAX) NULL
, IsExistingMembership INT NULL
, DefaultOwner AS (CASE WHEN ObjType = 'SCHEMA' THEN ObjectName WHEN ObjType = 'ROLE' THEN 'dbo' END)
);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE state = 0 AND is_read_only = 0
AND HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @DBName;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @Executor = QUOTENAME(@DBName) + N'..sp_executesql';

	INSERT INTO @Result
	EXEC @Executor @CMD WITH RECOMPILE;
END

CLOSE DBs;
DEALLOCATE DBs;

SELECT ServerName = SERVERPROPERTY('ServerName')
, DatabaseName = DBName
, ObjectName = ObjType + N'::' + QUOTENAME(ObjectName)
, OwnerName = CurrentOwnerName
, LoginName = SUSER_SNAME(OwnerSID)
, OwnerShouldBe = ISNULL(DefaultOwner, @SaName)
, HasExistingMembership = CASE WHEN IsExistingMembership > 0 THEN 'YES' ELSE N'NO' END
, RemediationCmd = N'USE ' + QUOTENAME(DBName) + N'; ALTER AUTHORIZATION ON ' + UPPER(ObjType) + N'::' + QUOTENAME(ObjectName) + N' TO ' + QUOTENAME(ISNULL(DefaultOwner, @SaName)) + N';'
+ ISNULL(
  CASE WHEN ISNULL(IsExistingMembership,0) = 0 AND IS_SRVROLEMEMBER('sysadmin', SUSER_SNAME(OwnerSID)) = 0 THEN
	CASE ObjType
	WHEN N'DATABASE' THEN N' CREATE USER ' + QUOTENAME(SUSER_SNAME(OwnerSID)) + N' FOR LOGIN ' + QUOTENAME(SUSER_SNAME(OwnerSID)) + N'; ALTER ROLE [db_owner] ADD MEMBER ' + QUOTENAME(SUSER_SNAME(OwnerSID)) + N';'
	ELSE N' ALTER ROLE ' + QUOTENAME(ObjectName) + N' ADD MEMBER ' + QUOTENAME(SUSER_SNAME(OwnerSID)) + N';'
	END
  ELSE N'' END
  , N' -- Existing membership not found. Please fix with caution!')
FROM @Result
ORDER BY [DBId] ASC