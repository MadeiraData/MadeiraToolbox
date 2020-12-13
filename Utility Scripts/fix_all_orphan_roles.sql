/*
Author: Eitan Blumin | https://eitanblumin.com
Date Created: 2020-12-13
Description:
	Drop all orphan roles within all databases in the instance.
	If the role owns schemas, and these schemas contain objects, then their owner will be changed to dbo.
	If the role owns schemas, and these schemas don't contain objects, then these schemas will be dropped.

More info: https://eitanblumin.com/sql-vulnerability-assessment-tool-rules-reference-list/#Rule_VA1282
*/
SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @results AS TABLE
(
[database_name] SYSNAME COLLATE database_default,
[role_name] SYSNAME COLLATE database_default,
modify_date DATETIME NULL,
OwnedSchemas NVARCHAR(MAX) COLLATE database_default
);

INSERT INTO @results
exec master.sys.sp_MSforeachdb 'IF EXISTS (SELECT * FROM sys.databases WHERE [name] = ''?'' AND database_id > 4 AND state_desc = ''ONLINE'' AND is_read_only = 0)
BEGIN
USE [?];
SELECT DB_NAME(), [name], modify_date
, OwnedSchemas = (
SELECT cmd + N''; ''
FROM
(
SELECT cmd = ''ALTER AUTHORIZATION ON SCHEMA::'' + QUOTENAME(sch.name) + N'' TO [dbo]''
FROM sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id
AND EXISTS (SELECT NULL FROM sys.objects AS obj WHERE obj.schema_id = sch.schema_id)
UNION ALL
SELECT ''DROP SCHEMA '' + QUOTENAME(sch.name)
FROM sys.schemas AS sch
WHERE sch.principal_id = dp.principal_id
AND NOT EXISTS (SELECT NULL FROM sys.objects AS obj WHERE obj.schema_id = sch.schema_id)
) AS s
FOR XML PATH ('''')
)
FROM sys.database_principals AS dp
WHERE type = ''R''
AND principal_id not in (0,16384,16385,16386,16387,16389,16390,16391,16392,16393)
AND principal_id not in ( SELECT distinct role_principal_id FROM sys.database_role_members )
AND is_fixed_role = 0
AND modify_date < DATEADD(dd, -7, GETDATE()) -- ignore recently created roles
END
'

SELECT 'In server: ' + @@SERVERNAME + ', in database: ' + QUOTENAME([database_name]) + ' found unused database role: ' + QUOTENAME([role_name])
+ ' (last modified: ' + CONVERT(varchar(19), modify_date, 121) + ')',
DropCmd = N'USE ' + QUOTENAME([database_name]) + '; ' + ISNULL(OwnedSchemas, N'') + N' DROP ROLE ' + QUOTENAME([role_name]) + N';'
FROM @results
WHERE [database_name] NOT IN ('msdb','SentryOne','ReportServer','rdsadmin','SSISDB','distribution')
