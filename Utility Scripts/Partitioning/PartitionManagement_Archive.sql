/*
===============================================================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2023-07-09
===============================================================

-- Example 1: Archive based on minimum value to keep:

DECLARE @MinDateValueToKeep datetime = DATEADD(year, -1, GETDATE())

EXEC dbo.[PartitionManagement_Archive]
	  @PartitionFunctionName = 'PF_MyPartitionFunction'
	, @MinValueToKeep = @MinDateValueToKeep
	, @TruncateOldPartitions = 1
	, @DebugOnly = 0

GO

-- Example 2: Archive based on minimum value to keep and enforce a minimal number of partitions

DECLARE @MinDateValueToKeep datetime = DATEADD(year, -1, GETDATE())

EXEC dbo.[PartitionManagement_Archive]
	  @PartitionFunctionName = 'PF_MyPartitionFunction'
	, @MinValueToKeep = @MinDateValueToKeep
	, @MinPartitionsToKeep = 1000
	, @TruncateOldPartitions = 1
	, @DebugOnly = 0

*/
CREATE OR ALTER PROCEDURE dbo.[PartitionManagement_Archive]
  @PartitionFunctionName sysname
, @MinValueToKeep sql_variant
, @MinPartitionsToKeep int = 3
, @TruncateOldPartitions bit = 1
, @HistoricalTablePrefix sysname = NULL				-- Historical table prefix to add to the name of source table
, @HistoricalTablePostfix sysname = '_Historical'	-- Historical table postfix to add to the name of source table
, @HistoricalTableSchema sysname = NULL				-- The schema name of each historical table (leave NULL for same as source table)
, @AllowCreateIfNotExists bit = 0
, @AllowMergeOldPartitions bit = 1
, @DebugOnly bit = 0
AS
EXECUTE AS USER = 'dbo'
BEGIN

SET NOCOUNT, ARITHABORT, XACT_ABORT, QUOTED_IDENTIFIER ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @PartitionFunctionId int, @Msg nvarchar(max), @CMD nvarchar(max), @PartitionKeyDataType sysname, @CurrentPartitionsCount int;

-- Validations
IF @HistoricalTablePrefix IS NULL AND @HistoricalTablePostfix IS NULL AND @HistoricalTableSchema IS NULL
BEGIN
	RAISERROR(N'At least one of the following parameters must be specified: @HistoricalTablePrefix, @HistoricalTablePostfix, @HistoricalTableSchema',16,1);
	RETURN -1;
END

IF @MinValueToKeep IS NULL
BEGIN
	RAISERROR(N'Value for @MinValueToKeep must be specified',16,1);
	RETURN -1;
END

IF @HistoricalTableSchema IS NOT NULL AND SCHEMA_ID(@HistoricalTableSchema) IS NULL
BEGIN
	IF @AllowCreateIfNotExists = 1
	BEGIN
		SET @CMD = N'CREATE SCHEMA ' + QUOTENAME(@HistoricalTableSchema);
		RAISERROR(@CMD,0,1) WITH NOWAIT;
		IF @DebugOnly = 0 EXEC sp_executesql @CMD;
	END
	ELSE
	BEGIN
		RAISERROR(N'Schema "%s" was not found',16,1,@HistoricalTableSchema);
		RETURN -1;
	END
END

-- Get partition function ID and column data type
SELECT TOP (1)
  @PartitionFunctionId = pf.function_id
, @PartitionKeyDataType = QUOTENAME(tp.[name])
+ CASE
	WHEN tp.name LIKE '%char' OR tp.name LIKE '%binary' THEN N'(' + ISNULL(CONVERT(nvarchar(MAX), NULLIF(params.max_length,-1)),'max') + N')'
	WHEN tp.name IN ('decimal', 'numeric') THEN N'(' + CONVERT(nvarchar(MAX), params.precision) + N',' + CONVERT(nvarchar(MAX), params.scale) + N')'
	WHEN tp.name IN ('datetime2', 'time') THEN N'(' + CONVERT(nvarchar(MAX), params.scale) + N')'
	ELSE N''
  END
FROM sys.partition_schemes AS ps
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.partition_range_values AS rv ON rv.function_id = pf.function_id
INNER JOIN sys.partition_parameters AS params ON params.function_id = pf.function_id
INNER JOIN sys.types AS tp ON params.system_type_id = tp.system_type_id AND params.user_type_id = tp.user_type_id
LEFT JOIN sys.indexes AS ix ON ix.data_space_id = ps.data_space_id
LEFT JOIN sys.partitions AS p ON rv.boundary_id = p.partition_number AND p.object_id = ix.object_id AND p.index_id = ix.index_id
WHERE pf.name = @PartitionFunctionName
ORDER BY CASE WHEN p.rows > 0 THEN 0 ELSE 1 END ASC, rv.boundary_id DESC

-- Archive and merge old partitions
WHILE @PartitionFunctionId IS NOT NULL
BEGIN
	DECLARE @CurrObjectId int, @CurrPartitionToArchive int, @CurrPartitionRangeValue sql_variant, @MinPartitionNumberToKeep int;
	SET @CMD = N'SET @MinPartitionNumberToKeep = $PARTITION.' + QUOTENAME(@PartitionFunctionName) + N'(CONVERT(' + @PartitionKeyDataType + N', @MinValueToKeep))'

	EXEC sp_executesql @CMD
		, N'@MinPartitionNumberToKeep int OUTPUT, @MinValueToKeep sql_variant'
		, @MinPartitionNumberToKeep OUTPUT, @MinValueToKeep
	
	SELECT @CurrentPartitionsCount = fanout
	FROM sys.partition_functions
	WHERE function_id = @PartitionFunctionId

	-- Stop condition:
	IF @MinPartitionNumberToKeep <= 1
	OR @CurrentPartitionsCount <= @MinPartitionsToKeep
		BREAK;
		
	-- Archive old partitions
	IF @TruncateOldPartitions = 1
	BEGIN
		DECLARE PartitionedTables CURSOR
		LOCAL FAST_FORWARD
		FOR
		SELECT ix.object_id, pr.partition_number, pr.[value]
		FROM sys.partition_schemes AS ps 
		INNER JOIN sys.indexes AS ix ON ix.data_space_id = ps.data_space_id
		CROSS APPLY
		(
			SELECT TOP(1) p.partition_number, rv.[value]
			FROM sys.partitions AS p 
			INNER JOIN sys.partition_range_values AS rv ON rv.boundary_id = p.partition_number
			WHERE ix.object_id = p.object_id
			AND ix.index_id = p.index_id
			AND rv.function_id = ps.function_id
			AND p.partition_number <= @MinPartitionNumberToKeep
			AND p.[rows] > 0 -- not empty
			ORDER BY p.partition_number ASC
		) AS pr
		WHERE ps.function_id = @PartitionFunctionId
		AND ix.index_id <= 1 -- clustered or heap only

		OPEN PartitionedTables;

		WHILE 1=1
		BEGIN
			FETCH NEXT FROM PartitionedTables INTO @CurrObjectId, @CurrPartitionToArchive, @CurrPartitionRangeValue;
			IF @@FETCH_STATUS <> 0 BREAK;

			DECLARE @TableName sysname, @TableSchema sysname, @ArchiveTableName sysname, @ArchiveTableSchema sysname;

			-- Get source table schema+name
			SELECT @TableName = OBJECT_ID(@CurrObjectId), @TableSchema = OBJECT_SCHEMA_NAME(@CurrObjectId);

			-- Get target table schema+name
			SELECT @ArchiveTableName = ISNULL(@HistoricalTablePrefix, N'') + @TableName + ISNULL(@HistoricalTablePostfix, N'')
			, @ArchiveTableSchema = ISNULL(@HistoricalTableSchema, @TableSchema);

			-- Validation: Check if target table exists
			IF OBJECT_ID(QUOTENAME(@ArchiveTableSchema) + N'.' + QUOTENAME(@ArchiveTableName)) IS NULL
			BEGIN
				-- Create if not exists
				IF @AllowCreateIfNotExists = 1
				BEGIN
					SET @CMD = N' SELECT * INTO ' + QUOTENAME(@ArchiveTableSchema) + N'.' + QUOTENAME(@ArchiveTableName)
					+ N' FROM ' + QUOTENAME(@TableSchema) + N'.' + QUOTENAME(@TableName) + N' WHERE 1=0'
					+ N' UNION ALL SELECT TOP(1) * FROM ' + QUOTENAME(@TableSchema) + N'.' + QUOTENAME(@TableName) + N' WHERE 1=0' -- this is added to prevent transferrance of IDENTITY
					RAISERROR(@CMD,0,1) WITH NOWAIT;
					IF @DebugOnly = 0 EXEC sp_executesql @CMD;
				END
				ELSE
				BEGIN
					RAISERROR(N'Historical table "%s.%s" does not exist. Please create it first or run this procedure with @AllowCreateIfNotExists = 1.',16,1,@ArchiveTableSchema,@ArchiveTableName);
					RETURN -1;
				END
			END

			-- Validation: Source and target tables cannot be identical
			IF OBJECT_ID(QUOTENAME(@ArchiveTableSchema) + N'.' + QUOTENAME(@ArchiveTableName)) = OBJECT_ID(QUOTENAME(@TableSchema) + N'.' + QUOTENAME(@TableName))
			BEGIN
				RAISERROR(N'Source and Historical tables cannot be the one and the same. Please adjust postfix/prefix/schema parameters accordingly.',16,1);
				RETURN -1;
			END
			
			-- Validation: Historical table must be partitioned
			DECLARE @ArchiveTablePartitionFunctionName int, @ArchiveTablePartitionNumber int;
			
			SELECT @ArchiveTablePartitionFunctionName = pf.name
			FROM sys.partitions AS p
			INNER JOIN sys.indexes AS ix ON ix.object_id = p.object_id AND ix.index_id = p.index_id
			INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
			INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
			WHERE p.partition_number = 1 -- first (any) partition
			AND p.index_id <= 1 -- clustered or heap only
			AND p.object_id = OBJECT_ID(QUOTENAME(@ArchiveTableSchema) + N'.' + QUOTENAME(@ArchiveTableName))
			
			IF @ArchiveTablePartitionFunctionId IS NOT NULL
			BEGIN
				RAISERROR(N'Historical table "%s.%s" is using partition function "%s". This is an informational message.'
					,0,1,@ArchiveTableSchema,@ArchiveTableName,@ArchiveTablePartitionFunctionName) WITH NOWAIT;

				-- Get the equivalent partition number of the historical table
				SET @CMD = N'SET @ArchiveTablePartitionNumber = $PARTITION.' + QUOTENAME(@ArchiveTablePartitionFunctionName) + N'(CONVERT(' + @PartitionKeyDataType + N', @CurrPartitionRangeValue))'

				EXEC sp_executesql @CMD
					, N'@ArchiveTablePartitionNumber int OUTPUT, @CurrPartitionRangeValue sql_variant'
					, @ArchiveTablePartitionNumber OUTPUT, @CurrPartitionRangeValue
			END
			ELSE
			BEGIN
				RAISERROR(N'Historical table "%s.%s" must be partitioned.',16,1,@ArchiveTableSchema,@ArchiveTableName) WITH NOWAIT;
				RETURN -1;
			END

			-- Validation: Source and target tables must not use the same partition function
			IF @ArchiveTablePartitionFunctionName = @PartitionFunctionName
			BEGIN
				RAISERROR(N'Source and Historical tables "%s.%s" and "%s.%s" must not use the same partition function "%s".'
					,16,1,@TableSchema,@TableName,@ArchiveTableSchema,@ArchiveTableName,@PartitionFunctionName);
				RETURN -1;
			END

			-- Validation: Target partition must be empty
			IF NOT EXISTS
			(
				SELECT *
				FROM sys.partitions AS p
				WHERE p.object_id = OBJECT_ID(QUOTENAME(@ArchiveTableSchema) + N'.' + QUOTENAME(@ArchiveTableName))
				AND p.index_id <= 1 -- clustered or heap only
				AND p.partition_number = @ArchiveTablePartitionNumber
				AND p.[rows] = 0
			)
			BEGIN
				RAISERROR(N'The target partition "%d" in Historical table "%s.%s" must be empty.',16,1,@ArchiveTablePartitionNumber,@ArchiveTableSchema,@ArchiveTableName) WITH NOWAIT;
				RETURN -1;
			END

			-- Perform switch
			SET @CMD = N'ALTER TABLE ' + QUOTENAME(@TableSchema) + N'.' + QUOTENAME(@TableName) + N' SWITCH PARTITION ' + CONVERT(nvarchar(MAX), @CurrPartitionToArchive)
				+ N' TO ' + QUOTENAME(@ArchiveTableSchema) + N'.' + QUOTENAME(@ArchiveTableName) + N' PARTITION ' + CONVERT(nvarchar(MAX), @ArchiveTablePartitionNumber) + N';'
			RAISERROR(@CMD,0,1) WITH NOWAIT;
			IF @DebugOnly = 0 EXEC sp_executesql @CMD;
		END
	
		CLOSE PartitionedTables;
		DEALLOCATE PartitionedTables;
	END
	
	WHILE @AllowMergeOldPartitions = 1
	BEGIN
		DECLARE @MinPartitionRangeValue sql_variant;

		SELECT TOP(1) @MinPartitionRangeValue = rv.[value]
		FROM sys.partition_range_values AS rv
		INNER JOIN sys.partitions AS p ON rv.boundary_id = p.partition_number
		INNER JOIN sys.indexes AS ix ON ix.object_id = p.object_id AND ix.index_id = p.index_id
		INNER JOIN sys.partition_schemes AS ps ON ix.data_space_id = ps.data_space_id
		WHERE ps.function_id = @PartitionFunctionId
		AND p.partition_number <= @MinPartitionNumberToKeep
		AND p.[rows] = 0
		ORDER BY rv.boundary_id ASC

		IF @@ROWCOUNT = 0 BREAK;

		SET @CMD = 'SET QUOTED_IDENTIFIER ON;
	ALTER PARTITION FUNCTION ' + QUOTENAME(@PartitionFunctionName) + '() MERGE RANGE (CONVERT(' + @PartitionKeyDataType + N', @MinPartitionRangeValue));'
	
		PRINT CONCAT(N'Merging @MinPartitionRangeValue: ', CONVERT(nvarchar(MAX), @MinPartitionRangeValue))
		RAISERROR(@CMD,0,1) WITH NOWAIT;
		IF @DebugOnly = 0 EXEC sp_executesql @CMD, N'@MinPartitionRangeValue sql_variant', @MinPartitionRangeValue;
		IF @DebugOnly = 1 BREAK;
	END

	IF @DebugOnly = 1 BREAK;
END

SET @Msg = CONCAT(CONVERT(nvarchar(24), GETDATE(), 121), N' - Done.')
RAISERROR(N'%s', 0,1, @Msg) WITH NOWAIT;

END
GO