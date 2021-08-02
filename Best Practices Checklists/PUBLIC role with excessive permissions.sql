/*
Excessive permissions should not be granted to PUBLIC role
===========================================================
Author: Eitan Blumin | https://madeiradata.com | https://eitanblumin.com
Date: 2021-08-02
Description:
This check is based on SQL Vulnerability Assessment rule VA1095
https://eitanblumin.com/sql-vulnerability-assessment-tool-rules-reference-list/#Rule_VA1095

The rule check is evaluated against all accessible databases,
and outputs the relevant details as well as remediation commands.
*/
SET NOCOUNT, XACT_ABORT, ARITHABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
DECLARE @temp AS TABLE
(
[database_name] SYSNAME COLLATE database_default NULL,
[permission_state] NVARCHAR(256) COLLATE database_default NULL,
[permission] NVARCHAR(256) COLLATE database_default NULL,
[object_type] SYSNAME COLLATE database_default NULL,
[object_name] SYSNAME COLLATE database_default NULL
);

DECLARE @CurrDB sysname, @CMD nvarchar(max), @Executor nvarchar(1000);

SET @CMD = N'
SELECT DISTINCT
db_name() AS [database_name]
, perms.state_desc
, perms.permission_name
, REPLACE(perms.class_desc, ''_'', '' '') AS [object_type]
,CASE perms.class
WHEN 0 THEN db_name() -- database
WHEN 3 THEN schema_name(major_id) -- schema
WHEN 4 THEN printarget.NAME -- principal
WHEN 5 THEN asm.NAME -- assembly
WHEN 6 THEN type_name(major_id) -- type
WHEN 10 THEN xmlsc.NAME -- xml schema
WHEN 15 THEN msgt.NAME COLLATE DATABASE_DEFAULT -- message types
WHEN 16 THEN svcc.NAME COLLATE DATABASE_DEFAULT -- service contracts
WHEN 17 THEN svcs.NAME COLLATE DATABASE_DEFAULT -- services
WHEN 18 THEN rsb.NAME COLLATE DATABASE_DEFAULT -- remote service bindings
WHEN 19 THEN rts.NAME COLLATE DATABASE_DEFAULT -- routes
WHEN 23 THEN ftc.NAME -- full text catalog
WHEN 24 THEN sym.NAME -- symmetric key
WHEN 25 THEN crt.NAME -- certificate
WHEN 26 THEN asym.NAME -- assymetric key
ELSE N''(unknown object)''
END AS [object_name]
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
AND class != 1 -- Object or Columns (class = 1) are handled by VA1054 and have different remediation syntax
AND [state] IN (''G'',''W'')
AND NOT (
perms.class = 6
AND permission_name IN (''EXECUTE'',''REFERENCES'')
)
AND NOT (
perms.class = 0
AND prin.NAME = ''public''
AND perms.major_id = 0
AND perms.minor_id = 0
AND permission_name IN (
    ''VIEW ANY COLUMN ENCRYPTION KEY DEFINITION''
    ,''VIEW ANY COLUMN MASTER KEY DEFINITION''
    )
)';

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE state = 0
AND is_read_only = 0
AND source_database_id IS NULL
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE';

OPEN DBs;
WHILE 1=1
BEGIN
FETCH NEXT FROM DBs INTO @CurrDB
IF @@FETCH_STATUS <> 0 BREAK;
SET @Executor = QUOTENAME(@CurrDB) + N'..sp_executesql'
INSERT INTO @temp
EXEC @Executor @CMD
END
CLOSE DBs;
DEALLOCATE DBs;

SELECT @@SERVERNAME AS [server_name],
[database_name],
[permission_state],
[permission],
ISNULL([object_type],'(unknown type)') AS [object_type],
ISNULL([object_name], '(unknown object)') AS [object_name],
RemediationCmd = N'USE ' + QUOTENAME([database_name]) + N'; REVOKE '
+ [permission] + ' ON ' + [object_type] + '::' +QUOTENAME([object_name])
+ N' FROM [public];'
FROM @temp
--WHERE [object_name] IS NOT NULL
