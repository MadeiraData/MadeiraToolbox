:setvar PartitionKeyDataType bigint
GO
/*
-- Example usage:

EXEC dbo.[PurgePartitions_$(PartitionKeyDataType)]
	  @FirstFileGroup = 'FG_Partitions_1'
	, @SecondFileGroup = 'FG_Partitions_2'
	, @PartitionFunctionName = 'PRT_FN_MyFunction'
	, @MaxValueFromTable = '[dbo].[MyTable]'
	, @MaxValueFromColumn = '[BigIntColumn]'
	, @BufferIntervals = 200
	, @DebugOnly = 0
*/
CREATE OR ALTER PROCEDURE dbo.[PurgePartitions_$(PartitionKeyDataType)]
  @PartitionFunctionName sysname
, @MinValueToKeep $(PartitionKeyDataType)
, @TruncateOldPartitions bit = 1
, @DebugOnly bit = 0
AS
BEGIN

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @PartitionFunctionId int, @CurrentRangeCount int, @LastPartitionNumber int;
DECLARE @Msg nvarchar(max), @CMD nvarchar(max)

select @PartitionFunctionId = function_id
FROM sys.partition_functions
WHERE name = @PartitionFunctionName

-- Truncate and merge old partitions
WHILE 1 = 1
BEGIN
	DECLARE @CurrObjectId int, @MinPartitionRangeValue $(PartitionKeyDataType)

	SELECT @MinPartitionRangeValue = MIN(CONVERT($(PartitionKeyDataType), value))
	FROM sys.partition_range_values
	WHERE function_id = @PartitionFunctionId
	
	IF @MinPartitionRangeValue >= @MinValueToKeep
		BREAK;

	-- Truncate old partitions
	IF @TruncateOldPartitions = 1
	BEGIN
		DECLARE Tabs CURSOR
		LOCAL FAST_FORWARD
		FOR
		SELECT p.object_id
		FROM sys.partitions AS p
		INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
		WHERE p.partition_number = 1 -- first partition
		AND p.index_id <= 1 -- clustered or heap only
		AND p.[rows] > 0 -- not empty
		AND ps.function_id = @PartitionFunctionId

		OPEN Tabs;

		WHILE 1=1
		BEGIN
			FETCH NEXT FROM Tabs INTO @CurrObjectId;
			IF @@FETCH_STATUS <> 0 BREAK;

			SET @CMD = N'TRUNCATE TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId)) + N' WITH (PARTITIONS (1));'
			RAISERROR(@CMD,0,1) WITH NOWAIT;
			EXEC(@CMD);
		END
	
		CLOSE Tabs;
		DEALLOCATE Tabs;
	END

	-- Merge the old range
	SET @CMD = 'SET QUOTED_IDENTIFIER ON;
ALTER PARTITION FUNCTION ' + QUOTENAME(@PartitionFunctionName) + '() MERGE RANGE (@MinPartitionRangeValue);'
	
	RAISERROR(@CMD,0,1) WITH NOWAIT;
	EXEC sp_executesql @CMD, N'@MinPartitionRangeValue $(PartitionKeyDataType)', @MinPartitionRangeValue;
END

SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Done.')
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

END
GO