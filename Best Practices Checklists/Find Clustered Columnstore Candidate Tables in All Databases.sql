/*
Find Clustered Columnstore Candidate Tables in All Databases
============================================================
Author: Eitan Blumin | https://www.madeiradata.com
Date: 2022-05-04
Description:
	Based on Azure SQL Tip #1290:
	https://github.com/microsoft/azure-sql-tips/wiki/Azure-SQL-Database-tips#tip_id-1290
Supported versions:
	SQL Server 2017 (14.x) and newer | Azure SQL Database | Azure SQL Managed Instance
*/
DECLARE @CCICandidateMinSizeGB int = 10, @DaysBack int = 30;


SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @results table (dbName sysname NULL, cci_candidate_count int NULL, details nvarchar(max) NULL);
DECLARE @CMD nvarchar(MAX);

SET @CMD = N'WITH any_partition AS
(
SELECT p.object_id, p.index_id, p.partition_number, p.rows, p.data_compression_desc, ps.used_page_count * 8 / 1024. AS partition_size_mb,
MAX(IIF(p.data_compression_desc IN (''COLUMNSTORE'',''COLUMNSTORE_ARCHIVE''), 1, 0)) OVER (PARTITION BY p.object_id) AS object_has_columnstore_indexes,
MAX(IIF(p.rows >= 102400, 1, 0)) OVER (PARTITION BY p.object_id) AS object_has_columnstore_compressible_partitions
FROM sys.partitions AS p
INNER JOIN sys.dm_db_partition_stats AS ps
ON p.partition_id = ps.partition_id
AND p.object_id = ps.object_id
AND p.index_id = ps.index_id
WHERE -- restrict to objects that do not have column data types not supported for CCI
NOT EXISTS (
        SELECT 1
        FROM sys.columns AS c
        INNER JOIN sys.types AS t
        ON c.system_type_id = t.system_type_id
        WHERE c.object_id = p.object_id
        AND t.name IN (''text'',''ntext'',''image'',''timestamp'',''sql_variant'',''hierarchyid'',''geometry'',''geography'',''xml'')
        )
),
candidate_partition AS
(
SELECT object_id, index_id, partition_number, rows, partition_size_mb
FROM any_partition
WHERE data_compression_desc IN (''NONE'',''ROW'',''PAGE'')
AND object_has_columnstore_indexes = 0 -- an object with any kind of columnstore is not a candidate
AND object_has_columnstore_compressible_partitions = 1
),
table_operational_stats AS -- summarize operational stats for heap, CI, and NCI
(
SELECT cp.object_id,
       SUM(IIF(cp.index_id IN (0,1), partition_size_mb, 0)) AS table_size_mb, -- exclude NCI size
       SUM(IIF(cp.index_id IN (0,1), 1, 0)) AS partition_count,
       SUM(ios.leaf_insert_count) AS lead_insert_count,
       SUM(ios.leaf_update_count) AS leaf_update_count,
       SUM(ios.leaf_delete_count + ios.leaf_ghost_count) AS leaf_delete_count,
       SUM(ios.range_scan_count) AS range_scan_count,
       SUM(ios.singleton_lookup_count) AS singleton_lookup_count
FROM candidate_partition AS cp
CROSS APPLY sys.dm_db_index_operational_stats(DB_ID(), cp.object_id, cp.index_id, cp.partition_number) AS ios -- assumption: a representative workload has populated index operational stats for relevant tables
GROUP BY cp.object_id
),
cci_candidate_table AS
(
SELECT QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) COLLATE DATABASE_DEFAULT AS schema_name,
       QUOTENAME(t.name) COLLATE DATABASE_DEFAULT AS table_name,
       columns_count = (SELECT COUNT(*) FROM sys.columns AS c WHERE c.object_id = t.object_id AND c.is_computed = 0),
       tos.table_size_mb,
       tos.partition_count,
       tos.lead_insert_count AS insert_count,
       tos.leaf_update_count AS update_count,
       tos.leaf_delete_count AS delete_count,
       tos.singleton_lookup_count AS singleton_lookup_count,
       tos.range_scan_count AS range_scan_count,
       ISNULL(ius.user_seeks, 0) AS seek_count,
       ISNULL(ius.user_scans, 0) AS full_scan_count,
       ISNULL(ius.user_lookups, 0) AS lookup_count
FROM sys.tables AS t
INNER JOIN sys.indexes AS i ON t.object_id = i.object_id
INNER JOIN table_operational_stats AS tos ON t.object_id = tos.object_id
LEFT JOIN sys.dm_db_index_usage_stats AS ius ON t.object_id = ius.object_id AND i.index_id = ius.index_id
WHERE i.index_id <= 1 -- clustered index or heap
AND tos.table_size_mb > @CCICandidateMinSizeGB * 1024. -- consider sufficiently large tables only
AND t.is_ms_shipped = 0
AND tos.leaf_update_count = 0 -- conservatively require a CCI candidate to have no updates, seeks, or lookups
AND tos.singleton_lookup_count = 0
AND (
    i.index_id = 0 OR 
	(ius.user_lookups = 0
	AND ius.user_seeks = 0
	AND ius.user_scans > 0) -- require a CCI candidate to have some full scans
    )
),
cci_candidate_details AS
(
SELECT STRING_AGG(
        CAST(CONCAT(
                ''schema: '', schema_name, '', '',
                ''table: '', table_name, '', '',
                ''table size (MB): '', FORMAT(table_size_mb, ''#,0.00''), '', '',
                ''columns count: '', FORMAT(columns_count, ''#,0''), '', '',
                ''partition count: '', FORMAT(partition_count, ''#,0''), '', '',
                ''inserts: '', FORMAT(insert_count, ''#,0''), '', '',
                ''updates: '', FORMAT(update_count, ''#,0''), '', '',
                ''deletes: '', FORMAT(delete_count, ''#,0''), '', '',
                ''singleton lookups: '', FORMAT(singleton_lookup_count, ''#,0''), '', '',
                ''range scans: '', FORMAT(range_scan_count, ''#,0''), '', '',
                ''seeks: '', FORMAT(seek_count, ''#,0''), '', '',
                ''full scans: '', FORMAT(full_scan_count, ''#,0''), '', '',
                ''lookups: '', FORMAT(lookup_count, ''#,0'')
                ) AS nvarchar(max)), CHAR(10)
        ) WITHIN GROUP (ORDER BY schema_name, table_name)
       AS details,
       COUNT(1) AS cci_candidate_count
FROM cci_candidate_table
)
SELECT DB_NAME(), cci_candidate_count, ccd.details
FROM cci_candidate_details AS ccd
WHERE ccd.details IS NOT NULL'

DECLARE @CurrDB sysname, @spExecuteSql nvarchar(1000);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @results
	EXEC @spExecuteSql @CMD, N'@CCICandidateMinSizeGB int', @CCICandidateMinSizeGB;
END

CLOSE DBs;
DEALLOCATE DBs;


SELECT
	  dbName
	, cci_candidate_count
	, v.[value]
FROM @results AS r
CROSS APPLY string_split(r.details, CHAR(10)) AS v

UNION ALL

SELECT NULL, 0, N'Server ' + @@SERVERNAME + N' uptime is since ' + CONVERT(nvarchar(MAX), sqlserver_start_time, 121)
FROM sys.dm_os_sys_info
WHERE sqlserver_start_time > DATEADD(DAY, -@DaysBack, GETDATE())

UNION ALL

SELECT NULL, 0, N'Server ' + @@SERVERNAME + N' SQL major version is ' + CONVERT(nvarchar(MAX), CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff))
	+ N', ' + CONVERT(nvarchar(max), SERVERPROPERTY('Edition'))
WHERE CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) < 14
AND CONVERT(varchar(256), SERVERPROPERTY('Edition')) <> 'SQL Azure'