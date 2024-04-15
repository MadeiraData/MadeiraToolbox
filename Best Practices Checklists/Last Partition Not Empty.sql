SET NOCOUNT, XACT_ABORT, ARITHABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @MinGapFromEnd INT = 2

IF SERVERPROPERTY('EngineEdition') IN (3,5,6,8) -- Enterprise equivalent
OR (CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) > 13) -- SQL 2017+
OR (CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) = 13 AND CONVERT(int, @@microsoftversion & 0xffff) >= 4001) -- SQL 2016 SP1+
BEGIN
    SET NOCOUNT ON;
    DECLARE @Results AS TABLE
    (database_id INT, object_id INT, rows INT, partition_number INT, fanout INT, partition_function SYSNAME, partition_scheme SYSNAME, last_boundary_range SQL_VARIANT);

 DECLARE @CurrDB sysname, @spExecuteSql sysname;

 DECLARE DBs CURSOR
 LOCAL FAST_FORWARD
 FOR
 SELECT [name]
 FROM sys.databases
 WHERE HAS_DBACCESS([name]) = 1
 AND state = 0
 AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'

 OPEN DBs;

 WHILE 1=1
 BEGIN
  FETCH NEXT FROM DBs INTO @CurrDB;

  IF @@FETCH_STATUS <> 0 BREAK;

  SET @spExecuteSql = QUOTENAME(@CurrDB) + N'..sp_executesql'

  INSERT INTO @Results
  EXEC @spExecuteSql N'
SELECT DB_ID(), so.object_id, p.rows, p.partition_number, pf.fanout, pf.[name] AS partition_function, ps.[name] AS partition_scheme, prv.[value] AS last_boundary_range
FROM sys.partition_functions AS pf
INNER JOIN sys.partition_schemes as ps on ps.function_id=pf.function_id
INNER JOIN sys.indexes as si on si.data_space_id=ps.data_space_id
INNER JOIN sys.objects as so on si.object_id = so.object_id
INNER JOIN sys.partitions as p on si.object_id=p.object_id and si.index_id=p.index_id
LEFT JOIN sys.partition_range_values as prv on prv.function_id=pf.function_id AND p.partition_number= 
  CASE pf.boundary_value_on_right WHEN 1
   THEN prv.boundary_id + 1
  ELSE prv.boundary_id
  END
WHERE so.is_ms_shipped = 0
AND p.rows > 0
AND p.partition_number > 1 -- non-first partition
AND p.index_id <= 1 -- clustered or heap only
AND pf.fanout - p.partition_number <= @MinGapFromEnd', N'@MinGapFromEnd INT', @MinGapFromEnd

 END

 CLOSE DBs;
 DEALLOCATE DBs;

END
ELSE
    SELECT N'Table partitioning not supported on this instance. Check skipped.', 0

SELECT
    MessageText = N'In Server: ' + CONVERT(sysname, SERVERPROPERTY('ServerName')) + N', Database: ' + QUOTENAME(DB_NAME(database_id))
    + N', Table: ' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id, database_id)) + '.' + QUOTENAME(OBJECT_NAME(object_id, database_id))
    + N' partition "' + CONVERT(nvarchar(4000), partition_number) + N'" out of "' + CONVERT(nvarchar(4000), fanout) + N'" is not empty'
	+ ISNULL(N' (boundary range "' + CONVERT(nvarchar(4000), last_boundary_range, 21) + N'" in partition scheme ' + QUOTENAME(partition_scheme) + N', partition function ' + QUOTENAME(partition_function) + N')'
		, N'')
    , ServerName = SERVERPROPERTY('ServerName')
    , DatabaseName = DB_NAME(database_id)
    , SchemaName = OBJECT_SCHEMA_NAME(object_id, database_id)
    , TableName = OBJECT_NAME(object_id, database_id)
    , PartitionScheme = partition_scheme
    , PartitionFunction = partition_function
    , LastPartition = partition_number
    , LastBoundryRange = last_boundary_range
    , NumberOfRowsInLastPartition = [rows]
FROM @Results
ORDER BY NumberOfRowsInLastPartition DESC