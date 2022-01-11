SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Results AS TABLE
(database_id int, object_id int, rows int, partition_number int, partition_scheme sysname, partition_function sysname, last_boundary_range sql_variant);

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
	EXEC @spExecuteSql N'SELECT DB_ID(), t.object_id, p.rows, p.partition_number, p.partition_scheme, p.partition_function, p.last_boundary_range
	FROM sys.tables AS t
	CROSS APPLY
	(
		SELECT TOP 1 p.rows, p.partition_number, ps.name AS partition_scheme, pf.name AS partition_function, last_range.value AS last_boundary_range
		FROM sys.partitions AS p
		INNER JOIN sys.indexes AS ix ON p.object_id = ix.object_id AND p.index_id = ix.index_id
		INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
		INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
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
    , ServerName = @@SERVERNAME
    , DatabaseName = DB_NAME(database_id)
    , SchemaName = OBJECT_SCHEMA_NAME(object_id, database_id)
    , TableName = OBJECT_NAME(object_id, database_id)
    , PartitionScheme = partition_scheme
    , PartitionFunction = partition_function
    , LastPartition = partition_number
    , LastBoundryRange = last_boundary_range
    , NumberOfRows = [rows]
FROM @Results