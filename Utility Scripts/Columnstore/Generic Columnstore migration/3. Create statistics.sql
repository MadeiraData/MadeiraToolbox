/*
Description:
	Creates statictics on all columns and updates them. Requires Auto Create Statistics on the target DB. 
Notes:
	1) I've set monthly partitions on MyTargetPartitionColumn column, if that's not optimal, please alter the script accordingly
*/

USE [master]
GO

DECLARE  @SourceDBName sysname = 'MySourceDB'
		,@SourceSchemaName sysname = 'MySourceShema'
		,@SourceTableName sysname = 'MySourceTable'
		,@TargetDBName sysname = 'MyTargetDB'
		,@TargetSchemaName sysname = 'MyTargetShema'
		,@TargetTableName sysname = 'MyTargetTable'
		,@TargetPartitionColumn sysname = 'MyTargetPartitionColumn'
		,@StartDate datetime = CAST(GETDATE()-4 AS DATE) -- a few days worth of data should be enough, if the stats weren't created, increase it
		,@TopRows int = 10000 -- should be enough, if the stats weren't created, increase it
		,@ExecuteCommands bit = 1

--------------------------------------------------------------------------------------

-- Declare internal variables: 

DECLARE  @SourceTableNameFull NVARCHAR(200) = '[' + @SourceDBName + '].[' + @SourceSchemaName + '].[' +  @SourceTableName + ']'
		,@TargetTableNameFull nvarchar(200) = '[' + @TargetSchemaName + '].[' +  @TargetTableName + ']'
		,@GetTargetColumnList nvarchar(max) = ''
		,@TargetColumnList nvarchar(max) = ''
		,@CreateStatsCommand nvarchar(max) = ''
		,@GetConvertStatsCommand nvarchar(max) = ''
		,@ConvertStatsCommand nvarchar(max) = ''
		,@GetUpdateStatsCommand nvarchar(max) = ''
		,@UpdateStatsCommand nvarchar(max) = ''
		,@ErrorMsg NVARCHAR(2048) = ''
		,@CheckVariables NVARCHAR(MAX) = ''

-- Checks:

IF NOT EXISTS	
	(
		SELECT 1
		FROM sys.databases db
		WHERE db.name = @SourceDBName
	)
	SET @ErrorMsg +=  NCHAR(13) + '  Source DB doesn''t exist.';

IF NOT EXISTS	
	(
		SELECT 1
		FROM sys.databases db
		WHERE db.name = @TargetDBName
	)
	SET @ErrorMsg +=  NCHAR(13) + '  Target DB doesn''t exist.';

IF @ErrorMsg <> ''
	THROW 51000, @ErrorMsg, 1;  

SELECT @CheckVariables += NCHAR(13) + 'USE [' + @SourceDBName + ']' + NCHAR(13) 
						+ 'IF OBJECT_ID(''' + @SourceTableNameFull + ''') IS NULL' + NCHAR(13) 
						+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Source table doesn''''t exist.'';' + NCHAR(13);

SELECT @CheckVariables += NCHAR(13) + 'USE [' + @TargetDBName + ']' + NCHAR(13) 
						+ 'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NULL' + NCHAR(13) 
						+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Target table doesn''''t exist.'';' + NCHAR(13)
						+ CASE WHEN @TargetPartitionColumn IS NOT NULL 
							THEN 'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
								+ ' AND NOT EXISTS ( SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(''' + @TargetTableNameFull + ''') AND name = ''' + @TargetPartitionColumn + ''')' + NCHAR(13) 
								+ '		SET @ErrorMsg +=  NCHAR(13) + ''  Target partition column doesn''''t exist.'';' + NCHAR(13) 
								+ 'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
								+ ' AND NOT EXISTS ( SELECT 1 FROM sys.indexes i JOIN sys.partition_schemes s ON i.data_space_id = s.data_space_id WHERE i.object_id = OBJECT_ID(''' + @TargetTableNameFull + ''') AND i. name = ''CCI_' + @TargetTableName + ''')' + NCHAR(13) 
								+ '		SET @ErrorMsg +=  NCHAR(13) + ''  Partition column is specified, but the target table is not partitioned.'';' + NCHAR(13) 
							END	;
exec sp_executesql @CheckVariables, N'@ErrorMsg NVARCHAR(2048) OUTPUT', @ErrorMsg OUTPUT

IF @TargetPartitionColumn IS NULL AND @StartDate IS NOT NULL
	SET @ErrorMsg +=  NCHAR(13) + '  Partition column isn''t specified, while start value is.';
	ELSE 
		IF @TargetPartitionColumn IS NOT NULL AND @StartDate IS NULL
			SET @ErrorMsg +=  NCHAR(13) + '  Start value isn''t specified, while partition column is.';

IF @ErrorMsg <> ''
	THROW 51000, @ErrorMsg, 1;  

-- Get target column list:

SELECT @GetTargetColumnList = N'USE [' + @TargetDBName + '];' + NCHAR(13) 
							+ 'SELECT @TargetColumnList = STRING_AGG(''['' + c.name + '']'', '','') WITHIN GROUP (ORDER BY c.column_id)' + NCHAR(13) 
							+ 'FROM sys.columns c' + NCHAR(13) 
							+ 'WHERE 1=1' + NCHAR(13) 
							+ '	AND c.object_id = OBJECT_ID(''' + @TargetTableNameFull + ''')' + NCHAR(13) 
							+ '	AND c.is_computed = 0' + NCHAR(13) 
							+ 'GROUP BY c.object_id;'

EXEC sp_executesql @GetTargetColumnList, N'@TargetColumnList NVARCHAR(MAX) OUTPUT', @TargetColumnList OUTPUT

-- Create statistics

IF OBJECT_ID('tempdb..#create_stats') IS NOT NULL
DROP TABLE #create_stats;

SELECT @CreateStatsCommand = N'USE [' + @TargetDBName + '];' + NCHAR(13)
							+ 'SELECT TOP ' + CAST(@TopRows AS NVARCHAR(10)) + ' ' + @TargetColumnList + NCHAR(13)
							+ 'INTO #create_stats'  + NCHAR(13)
							+ 'FROM ' + CASE 
											WHEN @SourceDBName = @TargetDBName 
												THEN '[' + @SourceSchemaName + '].[' +  @SourceTableName + ']'
											ELSE @SourceTableNameFull
									    END
							+ CASE 
								WHEN @TargetPartitionColumn IS NULL
									THEN NCHAR(13)
								WHEN @TargetPartitionColumn IS NOT NULL  
									THEN NCHAR(13) + 'WHERE [' + @TargetPartitionColumn + '] >= ''' + CONVERT(nvarchar(8), @StartDate, 112) + '''' + NCHAR(13)
							  END
							+ 'EXCEPT' + NCHAR(13)
							+ 'SELECT TOP ' + CAST(@TopRows AS NVARCHAR(10)) + ' ' + @TargetColumnList + NCHAR(13)
							+ 'FROM ' + @TargetTableNameFull
							+ CASE 
								WHEN @TargetPartitionColumn IS NULL
									THEN ';' + NCHAR(13)
								WHEN @TargetPartitionColumn IS NOT NULL  
									THEN NCHAR(13) + 'WHERE [' + @TargetPartitionColumn + '] >= ''' + CONVERT(nvarchar(8), @StartDate, 112) + '''' + NCHAR(13)
										+ 'OPTION(RECOMPILE);'
							  END

IF @ExecuteCommands = 0
	PRINT CAST(@CreateStatsCommand AS NTEXT)
ELSE 
	EXEC sp_executesql @CreateStatsCommand;  

-- Convert statistics:

IF @TargetPartitionColumn IS NOT NULL
BEGIN
	SELECT @ConvertStatsCommand = N'USE [' + @TargetDBName + '];' + NCHAR(13);
	SELECT @GetConvertStatsCommand = N'USE [' + @TargetDBName + '];' + NCHAR(13)
								+ 'SELECT @ConvertStatsCommand += N''UPDATE STATISTICS [' + @TargetSchemaName + '].['' + OBJECT_NAME(s.object_id) + '']('' + s.name + '') WITH INCREMENTAL = ON;'' + NCHAR(13)' + NCHAR(13)
								+ 'FROM sys.stats s' + NCHAR(13)
								+ 'INNER JOIN sys.stats_columns sc' + NCHAR(13)
								+ '	ON s.OBJECT_ID = sc.OBJECT_ID' + NCHAR(13)
								+ '		AND s.stats_id = sc.stats_id' + NCHAR(13)
								+ 'INNER JOIN sys.columns c' + NCHAR(13)
								+ '	ON s.OBJECT_ID = c.OBJECT_ID' + NCHAR(13)
								+ '		AND sc.column_id = c.column_id' + NCHAR(13)
								+ 'WHERE s.OBJECT_ID = OBJECT_ID(''' + @TargetTableNameFull + ''')' + NCHAR(13)
								+ '	AND s.auto_created = 1' + NCHAR(13)
								+ '	AND s.is_incremental = 0;'
	EXEC sp_executesql @GetConvertStatsCommand, N'@ConvertStatsCommand NVARCHAR(MAX) OUTPUT', @ConvertStatsCommand OUTPUT
	IF @ExecuteCommands = 0
	BEGIN
		PRINT @GetConvertStatsCommand;
		PRINT CAST(@ConvertStatsCommand AS NTEXT);
	END
	ELSE 
		EXEC sp_executesql @ConvertStatsCommand;
END

-- Update Statistics:

SELECT @UpdateStatsCommand = N'USE [' + @TargetDBName + '];' + NCHAR(13);

SELECT @GetUpdateStatsCommand = N'USE [' + @TargetDBName + '];' + NCHAR(13)
							+ CASE 
								WHEN @TargetPartitionColumn IS NULL
									THEN 'SELECT @UpdateStatsCommand += N''UPDATE STATISTICS [' + @TargetSchemaName + '].['' + OBJECT_NAME(s.object_id) + ''](['' + s.name + '']);'' + NCHAR(13)' + NCHAR(13)
								WHEN @TargetPartitionColumn IS NOT NULL   
									THEN 'SELECT @UpdateStatsCommand += N''UPDATE STATISTICS [' + @TargetSchemaName + '].['' + OBJECT_NAME(s.object_id) + ''](['' + s.name + '']) WITH RESAMPLE ON PARTITIONS('' + CAST(sp.partition_number AS nvarchar(10)) + '');'' + NCHAR(13)' + NCHAR(13)
							  END
							+ 'FROM sys.stats s'
							+ CASE 
								WHEN @TargetPartitionColumn IS NULL
									THEN NCHAR(13)
								WHEN @TargetPartitionColumn IS NOT NULL  
									THEN  + NCHAR(13) + 'CROSS APPLY sys.dm_db_incremental_stats_properties(s.object_id, s.stats_id) sp' + NCHAR(13)
							  END
							+ 'WHERE s.object_id = OBJECT_ID(''' + @TargetTableNameFull + ''')' + NCHAR(13)
							+ '	AND s.auto_created = 1' + NCHAR(13)
							+ CASE 
								WHEN @TargetPartitionColumn IS NULL
									THEN '	AND ISNULL(STATS_DATE(s.object_id,s.stats_id), ''19000101'') < CAST(GETDATE() AS DATE)' + NCHAR(13)
										+ 'ORDER BY s.stats_id;'
								WHEN @TargetPartitionColumn IS NOT NULL  
									THEN '	AND ISNULL(sp.last_updated, ''19000101'') < CAST(GETDATE() AS DATE)' + NCHAR(13)
										+ 'ORDER BY s.stats_id, sp.partition_number;'
							  END	

EXEC sp_executesql @GetUpdateStatsCommand, N'@UpdateStatsCommand NVARCHAR(MAX) OUTPUT', @UpdateStatsCommand OUTPUT

IF @ExecuteCommands = 0
BEGIN
	print @GetUpdateStatsCommand
	PRINT CAST(@UpdateStatsCommand AS NTEXT)
END
ELSE 
BEGIN
	EXEC sp_executesql @UpdateStatsCommand;  
END
GO 
