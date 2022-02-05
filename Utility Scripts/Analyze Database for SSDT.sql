SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
CREATE TABLE #Results
(
	[database_name] sysname NULL,
	[finding_level] sysname NOT NULL,
	[finding_type] sysname NOT NULL,
	[finding_detail] nvarchar(MAX) NULL,
	INDEX IX CLUSTERED ([database_name], [finding_level], [finding_type])
);

INSERT INTO #Results
SELECT NULL, 'Instance', 'ServerName', CONVERT(nvarchar(MAX), SERVERPROPERTY('ServerName'))
UNION ALL
SELECT NULL, 'Instance', 'EngineEdition', CONVERT(nvarchar(MAX), SERVERPROPERTY('EngineEdition'))
UNION ALL
SELECT NULL, 'Instance', 'Edition', CONVERT(nvarchar(MAX), SERVERPROPERTY('Edition'))
UNION ALL
SELECT NULL, 'Instance', '@@VERSION', @@VERSION

INSERT INTO #Results
SELECT NULL, 'Instance', 'Linked Server', srv.srvname
FROM sys.sysservers AS srv
WHERE srv.srvid > 0
AND 1 IN (srv.dataaccess, srv.rpc, srv.rpcout)

INSERT INTO #Results
SELECT NULL, 'Instance', 'Server Trigger', trg.[name]
FROM sys.server_triggers AS trg
WHERE trg.is_disabled = 0


INSERT INTO #Results
SELECT DISTINCT js.[database_name], 'Instance', 'Job', j.[name]
FROM msdb..sysjobs AS j
INNER JOIN msdb..sysjobsteps AS js ON j.job_id = js.job_id
WHERE ISNULL(js.database_name, DB_NAME()) = DB_NAME()
AND j.enabled = 1

INSERT INTO #Results
SELECT [name], 'Database', 'Compatibility Level', CONVERT(nvarchar(MAX),db.compatibility_level)
FROM sys.databases AS db
WHERE (db.database_id = DB_ID() OR db.[name] = 'master')

INSERT INTO #Results
SELECT [name], 'Database', d.finding_type, d.finding_value
FROM sys.databases AS db
CROSS APPLY
(VALUES
	('CDC Enabled', db.is_cdc_enabled),
	('Trustworthy', db.is_trustworthy_on),
	('DB Chaining', db.is_db_chaining_on),
	('Published', db.is_published),
	('Subscribed', db.is_subscribed),
	('Merge Published', db.is_merge_published),
	('Broker Enabled', db.is_broker_enabled)
) AS d(finding_type, finding_value)
WHERE db.database_id = DB_ID()
AND d.finding_value = 1

INSERT INTO #Results
SELECT DB_NAME(), REPLACE(RTRIM(ob.type_desc), '_', ' '), d.finding_type, QUOTENAME(SCHEMA_NAME(ob.schema_id)) + N'.' + QUOTENAME(ob.name)
FROM sys.sql_modules AS m
INNER JOIN sys.objects AS ob ON m.object_id = ob.object_id
CROSS APPLY
(VALUES
	('Encrypted Module', CASE WHEN m.definition IS NULL THEN 1 ELSE 0 END),
	('Natively Compiled', m.uses_native_compilation)
) AS d(finding_type, finding_value)
WHERE d.finding_value = 1

INSERT INTO #Results
SELECT DB_NAME(), 'Assembly', asm.permission_set_desc COLLATE DATABASE_DEFAULT, asm.name
FROM sys.assemblies AS asm

INSERT INTO #Results
SELECT DB_NAME(), N'Table', SUBSTRING(REPLACE(p.xx.value('local-name(.)','sysname'), '_', ' '), 4, 400), QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
FROM sys.tables AS t
CROSS APPLY ( SELECT p.* FROM sys.tables AS p WHERE t.object_id = p.object_id FOR XML AUTO, ELEMENTS, TYPE ) AS d(x)
CROSS APPLY d.x.nodes('p/*') AS p(xx)
WHERE p.xx.value('local-name(.)','sysname') LIKE 'is[_]%'
AND p.xx.value('(text())[1]','nvarchar(max)') = '1'
AND t.is_ms_shipped = 0

INSERT INTO #Results
SELECT DB_NAME(), N'Synonym', N'Cross Database Reference', s.name COLLATE DATABASE_DEFAULT + N' := ' + s.base_object_name COLLATE DATABASE_DEFAULT
FROM sys.synonyms AS s
WHERE LEN(REPLACE(s.base_object_name, '.', '')) <= LEN(s.base_object_name) - 2

SELECT *
FROM #Results
