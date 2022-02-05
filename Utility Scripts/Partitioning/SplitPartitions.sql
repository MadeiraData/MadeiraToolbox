:setvar PartitionKeyDataType bigint
GO
/*
-- Example usage:

EXEC dbo.[SplitPartitions_$(PartitionKeyDataType)]
	  @FirstFileGroup = 'FG_Partitions_1'
	, @SecondFileGroup = 'FG_Partitions_2'
	, @PartitionFunctionName = 'PRT_FN_MyFunction'
	, @MaxValueFromTable = '[dbo].[MyTable]'
	, @MaxValueFromColumn = '[BigIntColumn]'
	, @BufferIntervals = 200
	, @DebugOnly = 0
*/
CREATE OR ALTER PROCEDURE dbo.[SplitPartitions_$(PartitionKeyDataType)]
  @FirstFileGroup sysname = 'PRIMARY'
, @SecondFileGroup sysname = 'PRIMARY'
, @PartitionFunctionName sysname
, @MaxValueFromTable sysname
, @MaxValueFromColumn sysname
, @MaxValueOverride $(PartitionKeyDataType) = NULL
, @BufferIntervals int = 200
, @DebugOnly bit = 0
AS
BEGIN

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @PartitionRangeInterval $(PartitionKeyDataType), @MaxPartitionRangeValue $(PartitionKeyDataType), @CurrentRangeCount int, @LastPartitionNumber int;
DECLARE @Msg nvarchar(max), @CMD nvarchar(max)

SELECT @PartitionRangeInterval = interval, @MaxPartitionRangeValue = MAX([value]), @CurrentRangeCount = MAX(boundary_count) + 1, @LastPartitionNumber = MAX(boundary_id)
FROM
(
	SELECT rv.boundary_id, value =  CONVERT($(PartitionKeyDataType), rv.value)
	, CONVERT($(PartitionKeyDataType), rv.value) - LAG(CONVERT($(PartitionKeyDataType), rv.value), 1) OVER(ORDER BY rv.boundary_id ASC) AS interval
	, COUNT(*) OVER () AS boundary_count
	FROM sys.partition_range_values AS rv
	INNER JOIN sys.partition_functions AS f ON rv.function_id = f.function_id
	WHERE f.name = @PartitionFunctionName -- new
) AS q
WHERE interval IS NOT NULL
GROUP BY interval

DECLARE @ActualMaxValue $(PartitionKeyDataType), @MissingIntervals int;

IF @MaxValueOverride IS NULL
BEGIN
	SET @CMD = N'SELECT @ActualMaxValue = MAX(' + @MaxValueFromColumn + N') FROM ' + @MaxValueFromTable;
	EXEC sp_executesql @CMD, N'@ActualMaxValue $(PartitionKeyDataType) OUTPUT', @ActualMaxValue OUTPUT;
END
ELSE
BEGIN
	SET @ActualMaxValue = @MaxValueOverride;
END

SET @CMD = N'SET @LastPartitionNumber = $PARTITION.' + QUOTENAME(@PartitionFunctionName) + N'(@ActualMaxValue)'
EXEC sp_executesql @CMD, N'@LastPartitionNumber int OUTPUT, @ActualMaxValue $(PartitionKeyDataType)', @LastPartitionNumber OUTPUT, @ActualMaxValue

SET @MissingIntervals = CEILING((@ActualMaxValue - @MaxPartitionRangeValue) / @PartitionRangeInterval) + @BufferIntervals

SET @Msg = CONCAT(
  N'-- @PartitionRangeInterval: ', @PartitionRangeInterval
, N'. @MaxPartitionRangeValue: ', @MaxPartitionRangeValue
, N'. @CurrentRangeCount: ', @CurrentRangeCount
, N'. @LastPartitionNumber: ', @LastPartitionNumber
, N'. @ActualMaxValue: ', @ActualMaxValue
, N'. @ActualMaxValue - @MaxPartitionRangeValue: ', @ActualMaxValue - @MaxPartitionRangeValue
, N'. @MissingIntervals: ', @MissingIntervals
)
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

IF @MissingIntervals > 0
BEGIN
	DECLARE @CurrentRangeValue $(PartitionKeyDataType) = @MaxPartitionRangeValue, @TargetRangeValue $(PartitionKeyDataType) = @MaxPartitionRangeValue + (@PartitionRangeInterval * @MissingIntervals);

	SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Current range: ', @CurrentRangeValue, N'. Target range: ', @TargetRangeValue)
	RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

	WHILE @CurrentRangeValue < @TargetRangeValue
	BEGIN
		SET @CurrentRangeValue = @CurrentRangeValue + @PartitionRangeInterval;
		
		SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Splitting range: ', @CurrentRangeValue)
		RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;
		
		-- Execute NEXT USED for all dependent partition schemes
		DECLARE @CurrPS sysname, @CurrFG sysname

		DECLARE PSFG CURSOR
		LOCAL FAST_FORWARD
		FOR
		select ps.name, dst.name
		from sys.partition_schemes AS ps
		inner join sys.partition_functions AS f ON ps.function_id = f.function_id
		cross apply
		(
			select top (1) dds.data_space_id, fg.name
			from sys.destination_data_spaces AS dds
			inner join sys.data_spaces as fg on dds.data_space_id = fg.data_space_id
			where dds.partition_scheme_id = ps.data_space_id
			order by dds.destination_id desc
		) as dst
		where f.name = @PartitionFunctionName;

		OPEN PSFG;

		WHILE 1=1
		BEGIN
			FETCH NEXT FROM PSFG INTO @CurrPS, @CurrFG;
			IF @@FETCH_STATUS <> 0 BREAK;

			SET @CMD = N'ALTER PARTITION SCHEME ' + QUOTENAME(@CurrPS) + ' NEXT USED ';
			IF @CurrFG = @FirstFileGroup
				SET @CMD = @CMD + QUOTENAME(@SecondFileGroup);
			ELSE
				SET @CMD = @CMD + QUOTENAME(@FirstFileGroup);

			RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
			IF @DebugOnly = 0 EXEC (@CMD);
		END

		CLOSE PSFG;
		DEALLOCATE PSFG;

		-- Execute SPLIT on the partition function
		SET @CMD = N'ALTER PARTITION FUNCTION ' + QUOTENAME(@PartitionFunctionName) + N'() SPLIT RANGE(@CurrentRangeValue); -- ' + CONVERT(nvarchar(MAX), @CurrentRangeValue)
		RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
		IF @DebugOnly = 0 EXEC sp_executesql @CMD, N'@CurrentRangeValue $(PartitionKeyDataType)', @CurrentRangeValue;
	
	END
END

SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Done.')
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

END
GO