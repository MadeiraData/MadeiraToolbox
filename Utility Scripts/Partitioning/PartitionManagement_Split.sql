/*
===============================================================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2022-01-11
Minimum Version: SQL Server 2016 (13.x) and later
===============================================================

-- Example 1: Automatically detect current max value, and create 200 buffer partitions beyond it, using the last interval as the increment:

EXEC dbo.[PartitionManagement_Split]
	  @PartitionFunctionName = 'PF_MyPartitionFunction'
	, @RoundRobinFileGroups = 'FG_Partitions_1,FG_Partitions_2'
	, @TargetRangeValue = NULL
	--, @PartitionRangeInterval = 100000
	, @BufferIntervals = 200
	, @DebugOnly = 1
	, @Verbose = 1

GO

-- Example 2: Create monthly partitions one year forward:

DECLARE @FutureValue datetime = DATEADD(year,1, CONVERT(date, GETDATE()))
, @PartitionRangeInterval datetime = DATEADD(dd,1,0)

EXEC dbo.[PartitionManagement_Split]
	  @PartitionFunctionName = 'PF_MyPartitionFunction'
	, @RoundRobinFileGroups = 'PRIMARY'
	, @TargetRangeValue = @FutureValue
	, @PartitionIncrementExpression = 'DATEADD(month, 1, CONVERT(datetime, @CurrentRangeValue))'
	, @PartitionRangeInterval = @PartitionRangeInterval
	, @DebugOnly = 0
	, @AllowDataMovementForColumnstore = 1 -- this will enable data movement for non-empty partitions with columnstore
*/
CREATE OR ALTER PROCEDURE dbo.[PartitionManagement_Split]
  @PartitionFunctionName sysname
, @RoundRobinFileGroups nvarchar(MAX) = N'PRIMARY'
, @TargetRangeValue sql_variant = NULL
, @PartitionIncrementExpression nvarchar(4000) = N'CONVERT(float, @CurrentRangeValue) + CONVERT(float, @PartitionRangeInterval)' -- 'DATEADD(month, 1, CONVERT(datetime, @CurrentRangeValue))'
, @BufferIntervals int = 200
, @PartitionRangeInterval sql_variant = NULL
, @DebugOnly bit = 0
, @Verbose bit = 0
, @AllowDataMovementForColumnstore bit = 0 /* SPLIT is not supported for non-empty partitions with columnstore indexes (error 35346). This will MOVE such partitions to a staging table and then re-insert them back. */
AS
EXECUTE AS USER = 'dbo'
BEGIN

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @FileGroups AS table (FGID int NOT NULL IDENTITY(1,1), FGName sysname NOT NULL);
DECLARE @Msg nvarchar(max);

INSERT INTO @FileGroups (FGName)
SELECT RTRIM(LTRIM([value]))
FROM STRING_SPLIT(@RoundRobinFileGroups, N',')

IF @@ROWCOUNT = 0
BEGIN
	RAISERROR(N'At least one filegroup must be specified in @RoundRobinFileGroups',16,1);
	RETURN -1;
END

IF EXISTS (SELECT FGName FROM @FileGroups GROUP BY FGName HAVING COUNT(*) > 1)
BEGIN
	RAISERROR(N'Each filegroup in @RoundRobinFileGroups cannot be specified more than once.',16,1);
	RETURN -1;
END

SELECT @Msg = ISNULL(@Msg + N', ', N'') + FGName
FROM @FileGroups
WHERE FGName NOT IN (SELECT ds.name FROM sys.data_spaces AS ds WHERE ds.type = 'FG')

IF @Msg IS NOT NULL
BEGIN
	RAISERROR(N'Invalid filegroup(s) specified: %s', 16, 1, @Msg);
	RETURN -1;
END

DECLARE
  @MaxPartitionRangeValue sql_variant
, @CurrentTotalPartitionCount int
, @LastPartitionNumber int
, @LastPartitionHasData bit
, @PartitionFunctionId int
, @CMD nvarchar(max)
, @MaxValueFromTable sysname
, @MaxValueFromColumn sysname
, @PartitionKeyDataType sysname
, @ActualMaxValue sql_variant
, @ExistingBuffer int

SELECT TOP (1)
  @PartitionFunctionId = pf.function_id
, @CurrentTotalPartitionCount = pf.fanout
, @LastPartitionNumber = rv.boundary_id
, @LastPartitionHasData = CASE WHEN p.rows > 0 THEN 1 ELSE 0 END
, @MaxPartitionRangeValue = rv.value
, @MaxValueFromColumn = c.name
, @MaxValueFromTable = QUOTENAME(OBJECT_SCHEMA_NAME(p.object_id)) + N'.' + QUOTENAME(OBJECT_NAME(p.object_id))
, @PartitionKeyDataType = ISNULL(@PartitionKeyDataType, QUOTENAME(tp.[name])
+ CASE
	WHEN tp.name LIKE '%char' OR tp.name LIKE '%binary' THEN N'(' + ISNULL(CONVERT(nvarchar(MAX), NULLIF(params.max_length,-1)),'max') + N')'
	WHEN tp.name IN ('decimal', 'numeric') THEN N'(' + CONVERT(nvarchar(MAX), params.precision) + N',' + CONVERT(nvarchar(MAX), params.scale) + N')'
	WHEN tp.name IN ('datetime2','time') THEN N'(' + CONVERT(nvarchar(MAX), params.scale) + N')'
	ELSE N''
  END)
, @ExistingBuffer = ISNULL(p.partition_number - pdata.partition_number + 1, 0)
FROM sys.partition_schemes AS ps
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.partition_range_values AS rv ON rv.function_id = pf.function_id
INNER JOIN sys.partition_parameters AS params ON params.function_id = pf.function_id
INNER JOIN sys.types AS tp ON params.system_type_id = tp.system_type_id AND params.user_type_id = tp.user_type_id
LEFT JOIN sys.indexes AS ix ON ix.data_space_id = ps.data_space_id
LEFT JOIN sys.partitions AS p ON rv.boundary_id = p.partition_number AND p.object_id = ix.object_id AND p.index_id = ix.index_id
LEFT JOIN sys.index_columns AS ic ON ic.object_id = p.object_id AND ic.index_id = p.index_id AND ic.partition_ordinal > 0
LEFT JOIN sys.columns AS c ON c.object_id = p.object_id AND c.column_id = ic.column_id
OUTER APPLY
(
	SELECT TOP(1) *
	FROM sys.partitions AS p2
	WHERE p2.object_id = p.object_id
	AND p2.index_id = p.index_id
	AND p2.rows > 0
	ORDER BY p2.partition_number DESC
) AS pdata
WHERE pf.name = @PartitionFunctionName
ORDER BY rv.boundary_id DESC, p.rows DESC, ix.index_id ASC

IF @PartitionFunctionId IS NULL
BEGIN
	RAISERROR(N'Partition function "%s" is invalid',16,1,@PartitionFunctionName);
	RETURN -1;
END

IF @PartitionRangeInterval IS NULL AND @MaxPartitionRangeValue IS NOT NULL
BEGIN
	SET @CMD = N'
	SELECT TOP (1)
		@PartitionRangeInterval = CONVERT(sql_variant, CONVERT(' + @PartitionKeyDataType + N', @MaxPartitionRangeValue) - CONVERT(' + @PartitionKeyDataType + N', rv.value))
	FROM sys.partition_range_values AS rv
	WHERE rv.function_id = @PartitionFunctionId
	AND rv.boundary_id <= @LastPartitionNumber
	ORDER BY rv.boundary_id DESC'

	IF @Verbose = 1 PRINT @CMD;
	EXEC sp_executesql @CMD
		, N'@PartitionRangeInterval sql_variant OUTPUT, @MaxPartitionRangeValue sql_variant, @PartitionFunctionId int, @LastPartitionNumber int'
		, @PartitionRangeInterval OUTPUT, @MaxPartitionRangeValue, @PartitionFunctionId, @LastPartitionNumber
END

SET @PartitionIncrementExpression = ISNULL(@PartitionIncrementExpression, N'CONVERT(float, @CurrentRangeValue) + CONVERT(float, @PartitionRangeInterval)')

DECLARE @MissingIntervals int = 0, @CurrentRangeValue sql_variant, @IsCurrentRangeValueSmallerThanTargetValue bit;

IF @ActualMaxValue IS NULL AND @MaxValueFromColumn IS NOT NULL AND @MaxValueFromTable IS NOT NULL
BEGIN
	SET @CMD = N'SELECT @ActualMaxValue = CONVERT(sql_variant, MAX(' + @MaxValueFromColumn + N')) FROM ' + @MaxValueFromTable;
	IF @Verbose = 1 PRINT @CMD;
	EXEC sp_executesql @CMD, N'@ActualMaxValue sql_variant OUTPUT', @ActualMaxValue OUTPUT;
END

IF @LastPartitionNumber IS NULL AND @ActualMaxValue IS NOT NULL
BEGIN
	SET @CMD = N'SET @LastPartitionNumber = $PARTITION.' + QUOTENAME(@PartitionFunctionName) + N'(CONVERT(' + @PartitionKeyDataType + N', @ActualMaxValue))'
	IF @Verbose = 1 PRINT @CMD;
	EXEC sp_executesql @CMD, N'@LastPartitionNumber int OUTPUT, @ActualMaxValue sql_variant', @LastPartitionNumber OUTPUT, @ActualMaxValue
END

SET @CMD = N'SET @IsSmaller = CASE WHEN CONVERT(' + @PartitionKeyDataType + N', @MaxPartitionRangeValue) < CONVERT(' + @PartitionKeyDataType + N', @TargetRangeValue) THEN 1 ELSE 0 END'
IF @Verbose = 1 PRINT @CMD;
EXEC sp_executesql @CMD
	, N'@IsSmaller bit OUTPUT, @MaxPartitionRangeValue sql_variant, @TargetRangeValue sql_variant'
	, @IsCurrentRangeValueSmallerThanTargetValue OUTPUT, @MaxPartitionRangeValue, @TargetRangeValue;

--IF @TargetRangeValue IS NULL AND @LastPartitionHasData = 1
BEGIN
	DECLARE @i int = @ExistingBuffer;
	SET @CurrentRangeValue = @MaxPartitionRangeValue;

	WHILE (@TargetRangeValue IS NULL AND @i <= @BufferIntervals) OR (@TargetRangeValue IS NOT NULL AND @IsCurrentRangeValueSmallerThanTargetValue = 1)
	BEGIN
		SET @IsCurrentRangeValueSmallerThanTargetValue = 0;
		SET @CMD = N'SET @CurrentRangeValue = ' + @PartitionIncrementExpression
		SET @CMD = @CMD + CHAR(13) + CHAR(10)
			+ N'; SET @IsSmaller = CASE WHEN CONVERT(' + @PartitionKeyDataType + N', @CurrentRangeValue) < CONVERT(' + @PartitionKeyDataType + N', @TargetRangeValue) THEN 1 ELSE 0 END'
		IF @Verbose = 1 PRINT @CMD;
		EXEC sp_executesql @CMD
			, N'@IsSmaller bit OUTPUT, @CurrentRangeValue sql_variant OUTPUT, @TargetRangeValue sql_variant, @PartitionRangeInterval sql_variant'
			, @IsCurrentRangeValueSmallerThanTargetValue OUTPUT, @CurrentRangeValue OUTPUT, @TargetRangeValue, @PartitionRangeInterval;
		
		SET @i = @i + 1;
	END

	SET @MissingIntervals = @LastPartitionNumber + @i - @CurrentTotalPartitionCount - ISNULL(@ExistingBuffer,0);
	IF @TargetRangeValue IS NULL SET @TargetRangeValue = @CurrentRangeValue;
END

SET @CMD = N'SET @IsSmaller = CASE WHEN CONVERT(' + @PartitionKeyDataType + N', @MaxPartitionRangeValue) < CONVERT(' + @PartitionKeyDataType + N', @TargetRangeValue) THEN 1 ELSE 0 END'
IF @Verbose = 1 PRINT @CMD;
EXEC sp_executesql @CMD
	, N'@IsSmaller bit OUTPUT, @MaxPartitionRangeValue sql_variant, @TargetRangeValue sql_variant'
	, @IsCurrentRangeValueSmallerThanTargetValue OUTPUT, @MaxPartitionRangeValue, @TargetRangeValue;
	
SET @Msg = CONCAT(
  N'@PartitionRangeInterval: ', CONVERT(nvarchar(MAX), @PartitionRangeInterval)
, N'. @MaxPartitionRangeValue: ', CONVERT(nvarchar(MAX), @MaxPartitionRangeValue)
, N'. @CurrentTotalPartitionCount: ', @CurrentTotalPartitionCount
, N'. @LastPartitionNumber: ', @LastPartitionNumber
, N'. @ActualMaxValue: ', CONVERT(nvarchar(MAX), @ActualMaxValue)
, N'. @TargetRangeValue: ', ISNULL(CONVERT(nvarchar(MAX), @TargetRangeValue), N'(null)')
, N'. @MissingIntervals: ', ISNULL(CONVERT(nvarchar(MAX), @MissingIntervals), N'(null)')
, N'. @ExistingBuffer: ', ISNULL(CONVERT(nvarchar(MAX), @ExistingBuffer), N'(null)')
, N'. @IsCurrentRangeValueSmallerThanTargetValue: ', ISNULL(CONVERT(nvarchar(MAX), @IsCurrentRangeValueSmallerThanTargetValue), N'(null)')
)
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

DECLARE @StagingTablesToRecover AS TABLE(StagingTable sysname, RecoverCommand nvarchar(max));

BEGIN TRAN;

IF @MissingIntervals > 0 OR @IsCurrentRangeValueSmallerThanTargetValue = 1
BEGIN
	SET @CurrentRangeValue = @MaxPartitionRangeValue;
	SET @IsCurrentRangeValueSmallerThanTargetValue = 1;

	WHILE @IsCurrentRangeValueSmallerThanTargetValue = 1
	BEGIN
		SET @IsCurrentRangeValueSmallerThanTargetValue = 0;
		SET @CMD = N'SET @CurrentRangeValue = ' + @PartitionIncrementExpression
		SET @CMD = @CMD 
			+ CHAR(13) + CHAR(10) + N'; SET @LastPartitionNumber = $PARTITION.' + QUOTENAME(@PartitionFunctionName) + N'(CONVERT(' + @PartitionKeyDataType + N', @CurrentRangeValue))'
			+ CHAR(13) + CHAR(10) + N'; SET @IsSmaller = CASE WHEN CONVERT(' + @PartitionKeyDataType + N', @CurrentRangeValue) < CONVERT(' + @PartitionKeyDataType + N', @TargetRangeValue) THEN 1 ELSE 0 END'
		IF @Verbose = 1 PRINT @CMD;
		EXEC sp_executesql @CMD
			, N'@IsSmaller bit OUTPUT, @CurrentRangeValue sql_variant OUTPUT, @TargetRangeValue sql_variant, @LastPartitionNumber int OUTPUT, @PartitionRangeInterval sql_variant'
			, @IsCurrentRangeValueSmallerThanTargetValue OUTPUT, @CurrentRangeValue OUTPUT, @TargetRangeValue, @LastPartitionNumber OUTPUT, @PartitionRangeInterval;
		
		SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Splitting range: ', CONVERT(nvarchar(max), CONVERT(bigint, @CurrentRangeValue)))
		RAISERROR(N'%s (partition %d)', 0,1, @Msg, @LastPartitionNumber) WITH NOWAIT;
		
		-- Execute NEXT USED for all dependent partition schemes:
		DECLARE @CurrPS sysname, @CurrFG sysname, @NextFG sysname

		DECLARE PSFG CURSOR
		LOCAL FAST_FORWARD
		FOR
		select ps.name, dst.name
		from sys.partition_schemes AS ps
		inner join sys.partition_functions as pf on ps.function_id = pf.function_id
		cross apply
		(
			select top (1) dds.data_space_id, fg.name
			from sys.destination_data_spaces AS dds
			inner join sys.data_spaces as fg on dds.data_space_id = fg.data_space_id
			where dds.partition_scheme_id = ps.data_space_id
			AND dds.destination_id < pf.fanout
			order by dds.destination_id desc
		) as dst
		where ps.function_id = @PartitionFunctionId;

		OPEN PSFG;

		WHILE 1=1
		BEGIN
			FETCH NEXT FROM PSFG INTO @CurrPS, @CurrFG;
			IF @@FETCH_STATUS <> 0 BREAK;

			SET @CMD = N'ALTER PARTITION SCHEME ' + QUOTENAME(@CurrPS) + ' NEXT USED ';

			-- Find next FG based on round-robin:
			SET @NextFG = NULL;
			SELECT TOP(1) @NextFG = FGName
			FROM @FileGroups AS fg1
			WHERE FGID > (SELECT TOP (1) fg2.FGID FROM @FileGroups AS fg2 WHERE fg2.FGName = @CurrFG ORDER BY fg2.FGID)
			ORDER BY FGID

			IF @NextFG IS NULL
				SELECT @NextFG = FGName
				FROM @FileGroups
				WHERE FGID = 1;

			SET @CMD = @CMD + QUOTENAME(@NextFG) + N' -- prev: ' + QUOTENAME(@CurrFG);

			PRINT @CMD;
			IF @DebugOnly = 0 EXEC (@CMD);
		END

		CLOSE PSFG;
		DEALLOCATE PSFG;
		
		DECLARE @CurrObjectId int;

		DECLARE PartitionedTables CURSOR
		LOCAL FAST_FORWARD
		FOR
		SELECT p.object_id
		FROM sys.partitions AS p
		INNER JOIN sys.indexes AS ix ON ix.object_id = p.object_id AND ix.index_id = p.index_id
		INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
		WHERE p.partition_number = @LastPartitionNumber -- current partition
		AND ix.[type_desc] LIKE N'%COLUMNSTORE%' -- columnstore indexes only
		AND p.[rows] > 0 -- not empty
		AND ps.function_id = @PartitionFunctionId
		GROUP BY p.object_id

		OPEN PartitionedTables;

		WHILE 1=1
		BEGIN
			FETCH NEXT FROM PartitionedTables INTO @CurrObjectId;
			IF @@FETCH_STATUS <> 0 BREAK;
				
			SET @CMD = QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId))
			RAISERROR(N'Columnstore index found on table %s with non-empty partition %d',0,1,@CMD,@LastPartitionNumber) WITH NOWAIT;
			
			-- Truncate non-empty partitions on tables with a columnstore index
			IF @AllowDataMovementForColumnstore = 1
			BEGIN
				DECLARE @ColumnsList nvarchar(max), @PartitioningKeyColumn sysname, @StagingTableName sysname;
				SET @ColumnsList = NULL;
				SET @PartitioningKeyColumn = NULL;

				SELECT @ColumnsList = ISNULL(@ColumnsList + N', ', N'') + QUOTENAME([name])
				FROM sys.columns
				WHERE object_id = @CurrObjectId
				AND is_computed = 0

				SELECT @PartitioningKeyColumn = c.name
				FROM sys.index_columns AS ic
				INNER JOIN sys.indexes AS ix ON ic.object_id = ix.object_id AND ic.index_id = ix.index_id
				INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
				WHERE ic.partition_ordinal > 0
				AND ix.type_desc LIKE N'%COLUMNSTORE%'
				AND ic.object_id = @CurrObjectId

				WHILE 1=1
				BEGIN
					SET @StagingTableName = QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId) + N'_STAGING_' + REPLACE(NEWID(),N'-',N''))
					IF OBJECT_ID(@StagingTableName) IS NULL BREAK;
				END

				SET @CMD = N'
				SELECT ' + @ColumnsList + N'
				INTO ' + @StagingTableName + N'
				FROM ' + QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId)) + N'
				WHERE $PARTITION.' + QUOTENAME(@PartitionFunctionName) + N'(' + QUOTENAME(@PartitioningKeyColumn) + N') = ' + CONVERT(nvarchar(max), @LastPartitionNumber) + N';

				RAISERROR(N''Saved %d rows in ' + @StagingTableName + N''',0,1,@@ROWCOUNT) WITH NOWAIT;

				TRUNCATE TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId)) + N' WITH (PARTITIONS (' + CONVERT(nvarchar(max), @LastPartitionNumber) + N'));'
				
				INSERT INTO @StagingTablesToRecover (StagingTable, RecoverCommand)
				VALUES (@StagingTableName,
				CASE WHEN CONVERT(bit, OBJECTPROPERTY(@CurrObjectId, 'TableHasIdentity')) = 1 THEN N'
				SET IDENTITY_INSERT ' + QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId)) + N' ON;'
				ELSE N'' END + N'

				INSERT INTO ' + QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId)) + N'
				(' + @ColumnsList + N')
				SELECT
				 ' + @ColumnsList + N'
				FROM ' + @StagingTableName + N'
				
				RAISERROR(N''Recovered %d rows from ' + @StagingTableName + N''',0,1,@@ROWCOUNT) WITH NOWAIT;

				' + CASE WHEN CONVERT(bit, OBJECTPROPERTY(@CurrObjectId, 'TableHasIdentity')) = 1 THEN N'
				SET IDENTITY_INSERT ' + QUOTENAME(OBJECT_SCHEMA_NAME(@CurrObjectId)) + N'.' + QUOTENAME(OBJECT_NAME(@CurrObjectId)) + N' OFF;'
				ELSE N'' END + N'

				DROP TABLE ' + @StagingTableName + N';')

				IF @Verbose = 1 PRINT @CMD;
				IF @DebugOnly = 0 EXEC sp_executesql @CMD;
			END
		END
	
		CLOSE PartitionedTables;
		DEALLOCATE PartitionedTables;

		-- Execute SPLIT on the partition function
		SET @CMD = N'ALTER PARTITION FUNCTION ' + QUOTENAME(@PartitionFunctionName) + N'() SPLIT RANGE(CONVERT(' + @PartitionKeyDataType + N', @CurrentRangeValue)); -- ' + CONVERT(nvarchar(MAX), @CurrentRangeValue)
		IF @Verbose = 1 RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
		IF @DebugOnly = 0 EXEC sp_executesql @CMD, N'@CurrentRangeValue sql_variant', @CurrentRangeValue;
	
	END
END
ELSE
	PRINT N'No new partition ranges required.'
	
-- Recover any data saved in staging tables due to columnstore indexes
DECLARE StagingTables CURSOR
LOCAL FAST_FORWARD
FOR
SELECT StagingTable, RecoverCommand
FROM @StagingTablesToRecover

OPEN StagingTables;

WHILE 1=1
BEGIN
	FETCH NEXT FROM StagingTables INTO @StagingTableName, @CMD;
	IF @@FETCH_STATUS <> 0 BREAK;
	IF @Verbose = 1 PRINT @CMD;
	IF @DebugOnly = 0 EXEC sp_executesql @CMD
END

CLOSE StagingTables;
DEALLOCATE StagingTables;

COMMIT TRAN;

SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Done.')
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

END
GO