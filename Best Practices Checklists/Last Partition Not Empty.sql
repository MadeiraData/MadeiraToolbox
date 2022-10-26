SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Results AS TABLE
(database_id int, object_id int, rows bigint, partition_number int, partition_scheme sysname, partition_function sysname, filegroup_name sysname, last_boundary_range sql_variant
, total_table_rows bigint);

IF (CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 9 AND CONVERT(int, SERVERPROPERTY('EngineEdition')) IN (3,5,6,8)) -- Enterprise equivalent of SQL 2005+
OR (CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) > 13) -- SQL 2017+
OR (CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) = 13 AND CONVERT(int, @@microsoftversion & 0xffff) >= 4001) -- SQL 2016 SP1+
BEGIN
    DECLARE @CurrDB sysname, @spExecuteSql nvarchar(1000);

    DECLARE DBs CURSOR
    LOCAL FAST_FORWARD
    FOR
    SELECT [name]
    FROM sys.databases
    WHERE source_database_id IS NULL
    AND state = 0
    AND HAS_DBACCESS([name]) = 1
    AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'

    OPEN DBs;

    WHILE 1=1
    BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @Results
	EXEC @spExecuteSql N'SELECT DB_ID(), t.object_id, p.rows, p.partition_number, p.partition_scheme, p.partition_function, p.filegroup_name, p.last_boundary_range
	, total_rows = (select SUM(p2.rows) FROM sys.partitions AS p2 WHERE p2.index_id <= 1 AND p2.object_id = t.object_id)
	FROM sys.tables AS t
	CROSS APPLY
	(
		SELECT TOP 1 p.rows, p.partition_number, ps.name AS partition_scheme, pf.name AS partition_function, fg.name AS filegroup_name, last_range.value AS last_boundary_range
		FROM sys.partitions AS p
		INNER HASH JOIN sys.indexes AS ix ON p.object_id = ix.object_id AND p.index_id = ix.index_id
		INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
		INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
		INNER JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id AND ps.data_space_id = dds.partition_scheme_id
		INNER JOIN sys.filegroups AS fg ON dds.data_space_id = fg.data_space_id
		CROSS APPLY
		(
			SELECT TOP 1 *
			FROM sys.partition_range_values AS pr
			WHERE pr.function_id = ps.function_id
			ORDER BY pr.boundary_id DESC
		) AS last_range
		WHERE p.partition_number > 1 -- non-first partition
		AND p.index_id <= 1 -- clustered or heap only
		AND p.object_id = t.object_id
		ORDER BY partition_number DESC
	) AS p
	WHERE t.is_ms_shipped = 0
	AND p.rows > 0'
    END

    CLOSE DBs;
    DEALLOCATE DBs;
END
ELSE
    PRINT N'Table partitioning not supported on this instance. Check skipped.';

SELECT
    MessageText = N'In Server: ' + @@SERVERNAME + N', Database: ' + QUOTENAME(DB_NAME(database_id))
      + N', Table: ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id, database_id)) + '.' + QUOTENAME(OBJECT_NAME(object_id, database_id))
      + N' last partition "' + CONVERT(nvarchar(4000), partition_number) + N'" is not empty'
      + ISNULL(N' (boundary range "' + CONVERT(nvarchar(4000), last_boundary_range, 21) + N'" in partition scheme ' + QUOTENAME(partition_scheme) + N')', N'')
    , ServerName = SERVERPROPERTY('ServerName')
    , DatabaseName = DB_NAME(database_id)
    , SchemaName = OBJECT_SCHEMA_NAME(object_id, database_id)
    , TableName = OBJECT_NAME(object_id, database_id)
    , PartitionScheme = partition_scheme
    , PartitionFunction = partition_function
    , LastPartition = partition_number
    , LastBoundryRange = last_boundary_range
    , FileGroupName = filegroup_name
    , NumberOfRowsInLastPartition = [rows]
    , TotalNumberOfRows = total_table_rows
FROM @Results
