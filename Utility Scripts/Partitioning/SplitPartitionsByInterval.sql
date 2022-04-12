:setvar PartitionKeyDataType bigint
/*
This SplitPartitions stored procedure supports float-compatible data types:
tinyint, smallint, int, bigint, float, real, decimal, numeric, datetime, time, datetime2

It also supports up to 2 interchanging file groups

DECLARE @PartitionFunctionName sysname = 'PRT_FN_MyPartitionFunction'

SELECT f.name, tp.name, prm.max_length, prm.precision, prm.scale, prm.collation_name
FROM sys.partition_functions AS f
INNER JOIN sys.partition_parameters AS prm ON prm.function_id = f.function_id
INNER JOIN sys.types AS tp ON prm.system_type_id = tp.system_type_id AND prm.user_type_id = tp.user_type_id
WHERE f.name = @PartitionFunctionName
*/
GO
/*
-- Example usage:

EXEC dbo.[SplitPartitionsByInterval_$(PartitionKeyDataType)]
	  @FirstFileGroup = 'FG_Partitions_1'
	, @SecondFileGroup = 'FG_Partitions_2'
	, @PartitionFunctionName = 'PRT_FN_MyFunction'
	, @MaxValueFromTable = '[dbo].[MyTable]'
	, @MaxValueFromColumn = '[BigIntColumn]'
	, @BufferIntervals = 200
	, @DebugOnly = 0
*/
CREATE OR ALTER PROCEDURE dbo.[SplitPartitionsByInterval_$(PartitionKeyDataType)]
  @FirstFileGroup sysname = 'PRIMARY'
, @SecondFileGroup sysname = 'PRIMARY'
, @PartitionFunctionName sysname
, @MaxValueFromTable sysname
, @MaxValueFromColumn sysname
, @ActualMaxValue $(PartitionKeyDataType) = NULL
, @PartitionRangeInterval $(PartitionKeyDataType) = NULL
, @PartitionIncrementExpression nvarchar(4000) = NULL -- 'DATEADD(MM, 1, @CurrentRangeValue)'
, @TargetRangeValue $(PartitionKeyDataType) = NULL
, @BufferIntervals int = 100
, @DebugOnly bit = 0
AS
BEGIN

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @MaxPartitionRangeValue $(PartitionKeyDataType), @CurrentRangeCount int, @LastPartitionNumber int;
DECLARE @Msg nvarchar(max), @CMD nvarchar(max)

SELECT
	  @LastPartitionNumber = MAX(rv.boundary_id)
	, @MaxPartitionRangeValue =  MAX(CONVERT($(PartitionKeyDataType), rv.value))
	, @CurrentRangeCount = COUNT(*) + 1
FROM sys.partition_range_values AS rv
INNER JOIN sys.partition_functions AS f ON rv.function_id = f.function_id
WHERE f.name = @PartitionFunctionName

IF @PartitionRangeInterval IS NULL AND @PartitionIncrementExpression IS NULL
BEGIN
	SELECT TOP (1)
		@PartitionRangeInterval = @MaxPartitionRangeValue - CONVERT($(PartitionKeyDataType), rv.value)
	FROM sys.partition_range_values AS rv
	INNER JOIN sys.partition_functions AS f ON rv.function_id = f.function_id
	WHERE f.name = @PartitionFunctionName
	AND rv.boundary_id < @LastPartitionNumber
	ORDER BY
		rv.boundary_id DESC
END

SET @PartitionIncrementExpression = ISNULL(@PartitionIncrementExpression, N'@CurrentRangeValue + @PartitionRangeInterval')

DECLARE @MissingIntervals float;

IF @ActualMaxValue IS NULL
BEGIN
	SET @CMD = N'SELECT @ActualMaxValue = MAX(' + @MaxValueFromColumn + N') FROM ' + @MaxValueFromTable;
	EXEC sp_executesql @CMD, N'@ActualMaxValue $(PartitionKeyDataType) OUTPUT', @ActualMaxValue OUTPUT;
END

SET @CMD = N'SET @LastPartitionNumber = $PARTITION.' + QUOTENAME(@PartitionFunctionName) + N'(@ActualMaxValue)'
EXEC sp_executesql @CMD, N'@LastPartitionNumber int OUTPUT, @ActualMaxValue $(PartitionKeyDataType)', @LastPartitionNumber OUTPUT, @ActualMaxValue

SET @MissingIntervals = CEILING(CONVERT(float, ISNULL(@TargetRangeValue, @ActualMaxValue) - @MaxPartitionRangeValue) / CONVERT(float, @PartitionRangeInterval)) + @BufferIntervals
SET @TargetRangeValue = ISNULL(@TargetRangeValue, @MaxPartitionRangeValue + (@PartitionRangeInterval * @MissingIntervals))

SET @Msg = CONCAT(
  N'-- @PartitionRangeInterval: ', @PartitionRangeInterval
, N'. @MaxPartitionRangeValue: ', @MaxPartitionRangeValue
, N'. @CurrentRangeCount: ', @CurrentRangeCount
, N'. @LastPartitionNumber: ', @LastPartitionNumber
, N'. @ActualMaxValue: ', @ActualMaxValue
, N'. @ActualMaxValue - @MaxPartitionRangeValue: ', @ActualMaxValue - @MaxPartitionRangeValue
, N'. @MissingIntervals: ', @MissingIntervals
, N'. @TargetRangeValue: ', @TargetRangeValue
)
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

IF @MissingIntervals > 0
BEGIN
	DECLARE @CurrentRangeValue $(PartitionKeyDataType) = @MaxPartitionRangeValue;

	WHILE @CurrentRangeValue < @TargetRangeValue
	BEGIN
		SET @CMD = N'SET @CurrentRangeValue = ' + @PartitionIncrementExpression
		EXEC sp_executesql @CMD, N'@CurrentRangeValue $(PartitionKeyDataType) OUTPUT', @CurrentRangeValue OUTPUT;
		
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