/*
-- Example usage by min value:
DECLARE @MinDateValueToKeep datetime = DATEADD(year, -1, GETDATE())

EXEC dbo.[PurgePartitions]
	  @PartitionFunctionName = 'PRT_FN_MyFunction'
	, @MaxIntervals = NULL
	, @MinValueToKeep = @MinDateValueToKeep
	, @TruncateOldPartitions = 1
	, @DebugOnly = 0
GO
-- Example usage by max intervals:

EXEC dbo.[PurgePartitions]
	  @PartitionFunctionName = 'PRT_FN_MyFunction'
	, @MaxIntervals = 200
	, @MinValueToKeep = NULL
	, @TruncateOldPartitions = 1
	, @DebugOnly = 0
*/
CREATE OR ALTER PROCEDURE dbo.[PurgePartitions]
  @PartitionFunctionName sysname
, @MaxIntervals int = NULL
, @MinValueToKeep sql_variant = NULL
, @TruncateOldPartitions bit = 1
, @DebugOnly bit = 0
AS
BEGIN

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @PartitionFunctionId int, @Msg nvarchar(max), @CMD nvarchar(max), @PartitionKeyDataType sysname;


SELECT TOP (1)
  @PartitionFunctionId = pf.function_id
, @PartitionKeyDataType = QUOTENAME(tp.[name])
+ CASE
	WHEN tp.name LIKE '%char%' OR tp.name LIKE '%binary%' THEN N'(' + ISNULL(CONVERT(nvarchar(MAX), NULLIF(c.max_length,-1)),'max') + N')'
	WHEN tp.name IN ('decimal', 'numeric') THEN N'(' + CONVERT(nvarchar(MAX), c.precision) + N',' + CONVERT(nvarchar(MAX), c.scale) + N')'
	WHEN tp.name IN ('datetime2') THEN N'(' + CONVERT(nvarchar(MAX), c.scale) + N')'
	ELSE N''
  END
FROM sys.partitions AS p
INNER JOIN sys.indexes AS ix ON p.object_id = ix.object_id AND p.index_id = ix.index_id
INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.index_columns AS ic ON ic.object_id = p.object_id AND ic.index_id = p.index_id AND ic.partition_ordinal > 0
INNER JOIN sys.columns AS c ON c.object_id = p.object_id AND c.column_id = ic.column_id
INNER JOIN sys.types AS tp ON c.system_type_id = tp.system_type_id AND c.user_type_id = tp.user_type_id
WHERE pf.name = @PartitionFunctionName
ORDER BY CASE WHEN p.rows > 0 THEN 0 ELSE 1 END ASC, p.partition_number DESC


IF @MaxIntervals IS NULL AND @MinValueToKeep IS NULL
BEGIN
	RAISERROR(N'At least one of @MaxIntervals or @MinValueToKeep must be specified',16,1);
	RETURN -1;
END

-- Truncate and merge old partitions
WHILE 1=1
BEGIN
	DECLARE @CurrObjectId int, @MinPartitionRangeValue sql_variant, @IsMinRangeValueBiggerThanMinValueToKeep bit

	SET @CMD = N'SELECT @MinPartitionRangeValue = MIN(CONVERT(' + @PartitionKeyDataType + N', value))
	FROM sys.partition_range_values
	WHERE function_id = @PartitionFunctionId
	
	IF CONVERT(' + @PartitionKeyDataType + N', @MinPartitionRangeValue) >= CONVERT(' + @PartitionKeyDataType + N', @MinValueToKeep)
		SET @IsMinRangeValueBiggerThanMinValueToKeep = 1
	ELSE
		SET @IsMinRangeValueBiggerThanMinValueToKeep = 0'

	EXEC sp_executesql @CMD
		, N'@PartitionFunctionId int, @MinPartitionRangeValue sql_variant OUTPUT, @MinValueToKeep sql_variant, @IsMinRangeValueBiggerThanMinValueToKeep bit OUTPUT'
		, @PartitionFunctionId, @MinPartitionRangeValue OUTPUT, @MinValueToKeep, @IsMinRangeValueBiggerThanMinValueToKeep OUTPUT

	IF @IsMinRangeValueBiggerThanMinValueToKeep = 1
	OR @MaxIntervals > (select fanout from sys.partition_functions where function_id = @PartitionFunctionId)
		BREAK;

	-- Truncate old partitions
	IF @TruncateOldPartitions = 1
	BEGIN
		DECLARE PartitionedTables CURSOR
		LOCAL FAST_FORWARD
		FOR
		SELECT p.object_id
		FROM sys.partitions AS p
		INNER JOIN sys.indexes AS ix ON ix.object_id = p.object_id AND ix.index_id = p.index_id
		INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
		WHERE p.partition_number = 1 -- first partition
		AND p.index_id <= 1 -- clustered or heap only
		AND p.[rows] > 0 -- not empty
		AND ps.function_id = @PartitionFunctionId
		GROUP BY p.object_id

		OPEN PartitionedTables;

		WHILE 1=1
		BEGIN
			FETCH NEXT FROM PartitionedTables INTO @CurrObjectId;
			IF @@FETCH_STATUS <> 0 BREAK;

			SET @CMD = N'TRUNCATE TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId)) + N' WITH (PARTITIONS (1));'
			RAISERROR(@CMD,0,1) WITH NOWAIT;
			IF @DebugOnly = 0 EXEC(@CMD);
		END
	
		CLOSE PartitionedTables;
		DEALLOCATE PartitionedTables;
	END

	SET @CMD = 'SET QUOTED_IDENTIFIER ON;
ALTER PARTITION FUNCTION ' + QUOTENAME(@PartitionFunctionName) + '() MERGE RANGE (CONVERT(' + @PartitionKeyDataType + N', @MinPartitionRangeValue));'
	
	PRINT CONCAT(N'Merging @MinPartitionRangeValue: ', CONVERT(nvarchar(MAX), @MinPartitionRangeValue))
	RAISERROR(@CMD,0,1) WITH NOWAIT;
	IF @DebugOnly = 0 EXEC sp_executesql @CMD, N'@MinPartitionRangeValue sql_variant', @MinPartitionRangeValue;

	IF @DebugOnly = 1 BREAK;
END

SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Done.')
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

END
GO