/*
Check that the configured value for MAXDOP is in the recommended range, as described in this KB article: 
https://support.microsoft.com/en-us/help/2806535/recommendations-and-guidelines-for-the-max-degree-of-parallelism-confi

If @WhatIf = 0 then MAXDOP will automatically be changed to the recommended setting.
*/
-- change this to 1 to only display findings without actually changing the config:
DECLARE @WhatIf BIT = 1;

--------------------------------------
DECLARE @ProductVersion NVARCHAR(50);
DECLARE @Major INT;
DECLARE @NumaNodeCount INT;
DECLARE @LogicalProcessorPerNumaNodeCount INT;
DECLARE @EffectiveMaxDOP INT;
DECLARE @LogicalProcessorThreshold INT;
DECLARE @ResultMessage NVARCHAR(200);
DECLARE @RecommendedMaxDOP INT = 0;

SET @ProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(50));
SET @Major = CAST(LEFT(@ProductVersion, CHARINDEX('.', @ProductVersion)-1) AS INT);

IF @Major < 10
RAISERROR('This script is intended for SQL Server 2008 or higher. It will not work on version [%s].', 16, 1, @ProductVersion);

IF CAST(SERVERPROPERTY('Edition') AS NVARCHAR(50)) = 'Azure SQL'
RAISERROR('This script is not intended for Azure SQL DB.', 16, 1);

-- Get the MaxDOP setting
SELECT @EffectiveMaxDOP = CAST(value_in_use AS INT)
FROM sys.configurations
WHERE [name] = N'max degree of parallelism';

IF @EffectiveMaxDOP = 0
SELECT @EffectiveMaxDOP = COUNT(*)
FROM sys.dm_os_schedulers
WHERE scheduler_id <= 1048575 AND is_online = 1;

-- Get the NUMA node count
-- Get the logical processors per numa node
SELECT
  @NumaNodeCount = COUNT(DISTINCT memory_node_id)
, @LogicalProcessorPerNumaNodeCount = MAX(online_scheduler_count)
FROM
(
SELECT memory_node_id, SUM(online_scheduler_count) AS online_scheduler_count
FROM sys.dm_os_nodes
WHERE memory_node_id <> 64 AND node_id <> 64 --Excluded DAC node
GROUP BY memory_node_id
) AS m

IF @NumaNodeCount < 1 OR @LogicalProcessorPerNumaNodeCount < 1
RAISERROR('Could not capture NUMA node or logical processor count. Reported NUMA: [%d], Logical Processor: [%d]',
11,1, @NumaNodeCount, @LogicalProcessorPerNumaNodeCount);

SET @LogicalProcessorThreshold = CASE WHEN @NumaNodeCount = 1 THEN 8 ELSE 16 END;

--If NUMA = 1 and LogiProcs <= 8 THEN ASSERT(MaxDOP <= LogiProcs)
--If NUMA > 1 and LogiProcs <= 16 THEN ASSERT(MaxDOP <= LogiProcs)
IF @EffectiveMaxDOP = 1
BEGIN
SET @RecommendedMaxDOP = CASE WHEN @LogicalProcessorPerNumaNodeCount <= @LogicalProcessorThreshold
THEN @LogicalProcessorPerNumaNodeCount
WHEN @LogicalProcessorPerNumaNodeCount > @LogicalProcessorThreshold
AND (@LogicalProcessorPerNumaNodeCount / 2) <= @LogicalProcessorThreshold
THEN (@LogicalProcessorPerNumaNodeCount / 2)
WHEN @LogicalProcessorPerNumaNodeCount > @LogicalProcessorThreshold
THEN @LogicalProcessorThreshold
END

RAISERROR('MaxDOP is set to 1, which suppresses parallel plan generation.', 0, 1);
END
ELSE
IF @LogicalProcessorPerNumaNodeCount <= @LogicalProcessorThreshold
AND @EffectiveMaxDOP > @LogicalProcessorPerNumaNodeCount
BEGIN
SET @ResultMessage = N'MaxDOP should be less than or equal to the Logical Processor count per NUMA node.';
SET @RecommendedMaxDOP = @LogicalProcessorPerNumaNodeCount;
END
ELSE
BEGIN
-- If NUMA = 1 and LogiProcs > 8 THEN ASSERT(MaxDOP == 8)
IF @NumaNodeCount = 1
AND @LogicalProcessorPerNumaNodeCount > @LogicalProcessorThreshold
--AND @EffectiveMaxDOP > @LogicalProcessorThreshold
BEGIN
SET @ResultMessage = N'MaxDOP should be equal to 8.';
SET @RecommendedMaxDOP = 8;
END
-- If NUMA > 1 and LogiProcs > 16 THEN ASSERT(MaxDOP <= 16 & MaxDOP <= (LogiProcs / 2))
ELSE
BEGIN
IF @LogicalProcessorPerNumaNodeCount > @LogicalProcessorThreshold
--AND @EffectiveMaxDOP > @LogicalProcessorThreshold
BEGIN
SET @ResultMessage = N'MaxDOP should not exceed a value of 16.';
SET @RecommendedMaxDOP = 16;
END
ELSE IF @LogicalProcessorPerNumaNodeCount > @LogicalProcessorThreshold
AND (@LogicalProcessorPerNumaNodeCount / 2) <= @LogicalProcessorThreshold
--AND @EffectiveMaxDOP > (@LogicalProcessorPerNumaNodeCount / 2)
BEGIN
SET @ResultMessage = N'MaxDOP should be set at half the number of logical processors per NUMA node with a MAX value of 16.';
SET @RecommendedMaxDOP = (@LogicalProcessorPerNumaNodeCount / 2);
END
END
END

PRINT CONCAT(N'@@SERVERNAME: ', @@SERVERNAME, N'
@EffectiveMaxDOP: ', @EffectiveMaxDOP, N'
@NumaNodeCount: ', @NumaNodeCount, N'
@LogicalProcessorPerNumaNodeCount: ', @LogicalProcessorPerNumaNodeCount, N'
@LogicalProcessorThreshold: ', @LogicalProcessorThreshold, N'
@RecommendedMaxDOP: ', @RecommendedMaxDOP, N'
================================================')

IF @ResultMessage IS NOT NULL AND @EffectiveMaxDOP > @RecommendedMaxDOP
BEGIN
PRINT @ResultMessage + ' Changing MaxDOP ' + CONVERT(varchar(10), @EffectiveMaxDOP) + ' to ' + CONVERT(varchar(10), @RecommendedMaxDOP);


DECLARE @AdvancedOptionsWasOn BIT

SELECT @AdvancedOptionsWasOn = CAST([value] AS BIT) FROM sys.configurations WHERE name = 'show advanced options';

IF @AdvancedOptionsWasOn = 0
BEGIN
	IF @WhatIf = 0
	BEGIN
	  EXEC sp_configure 'show advanced options', 1;
	  RECONFIGURE WITH OVERRIDE;
	END
	ELSE
	BEGIN
	  PRINT N'EXEC sp_configure ''show advanced options'', 1; RECONFIGURE WITH OVERRIDE;'
	END
END

IF @WhatIf = 0
BEGIN
  EXEC sp_configure 'max degree of parallelism', @RecommendedMaxDOP;
  RECONFIGURE WITH OVERRIDE;
END
ELSE
BEGIN
  PRINT N'EXEC sp_configure ''max degree of parallelism'', ' + CONVERT(NVARCHAR(MAX), @RecommendedMaxDOP) + N'; RECONFIGURE WITH OVERRIDE;'
END

IF @AdvancedOptionsWasOn = 0
BEGIN
	IF @WhatIf = 0
	BEGIN
	  EXEC sp_configure 'show advanced options', 0;
	  RECONFIGURE WITH OVERRIDE;
	END
	ELSE
	BEGIN
	  PRINT N'EXEC sp_configure ''show advanced options'', 0; RECONFIGURE WITH OVERRIDE;'
	END
END
END
ELSE
PRINT N'MaxDOP is already within recommended range: ' + CONVERT(varchar(10), @RecommendedMaxDOP)