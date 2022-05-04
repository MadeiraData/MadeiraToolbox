/*
Description:
The following script generates the DELETE/INSERT commands that load latest data (modified since the main migration) from source to target table.
Notes:
	1) In DELETE/INSERT commands, PK columns should be used as JOIN predicates. If there's no PK, choose a set of columns that 
		uniquely identify rows and are rarely updated.
*/

USE [master]
GO

DECLARE  @SourceDBName sysname = 'MySourceDB'
		,@SourceSchemaName sysname = 'MySourceShema'
		,@SourceTableName sysname = 'MySourceTable'
		,@TargetDBName sysname = 'MyTargetDB'
		,@TargetSchemaName sysname = 'MyTargetShema'
		,@TargetTableName sysname = 'MyTargetTable'
		,@TargetPKColumns varchar(MAX) = 'PKColumn1,PKColumn2'
		,@TargetPartitionColumn sysname = NULL--'MyTargetPartitionColumn'
		,@TargetUpdateIndicator sysname = 'MyUpdateIndicatorColumn'
		,@StartDate datetime = CAST(GETDATE()-1 AS date) -- set it one day before the main migration (script #2) start date 
		,@IncludeIdentity bit = 1
		,@ExecuteCommands bit = 0

-----------------------------------------------------------------------------------------------------

-- Declare Internal variables: 

DECLARE  @SourceTableNameFull nvarchar(200) = '[' + @SourceDBName + '].[' + @SourceSchemaName + '].[' +  @SourceTableName + ']'
		,@TargetTableNameFull nvarchar(200) = '[' + @TargetSchemaName + '].[' +  @TargetTableName + ']'
		,@PartitionDate datetime = '19000101'
		,@GetTargetColumnList nvarchar(MAX) = ''
		,@TargetColumnList nvarchar(MAX) = ''
		,@GetDeleteColumnList nvarchar(MAX) = ''
		,@DeleteColumnList nvarchar(MAX) = ''
		,@InsertCommand nvarchar(MAX) = ''
		,@JoinPredicate nvarchar(MAX) = ''
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
							END	
						+ CASE WHEN @TargetUpdateIndicator IS NOT NULL 
							THEN 'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
								+ ' AND NOT EXISTS ( SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(''' + @TargetTableNameFull + ''') AND name = ''' + @TargetUpdateIndicator + ''')' + NCHAR(13) 
								+ '		SET @ErrorMsg +=  NCHAR(13) + ''  Target update indicator column doesn''''t exist.'';' + NCHAR(13) 
							END;
exec sp_executesql @CheckVariables, N'@ErrorMsg NVARCHAR(2048) OUTPUT', @ErrorMsg OUTPUT

IF @TargetUpdateIndicator IS NULL AND @StartDate IS NOT NULL
	SET @ErrorMsg +=  NCHAR(13) + '  Target update indicator column column isn''t specified, while start value is.';
	ELSE 
		IF @TargetUpdateIndicator IS NOT NULL AND @StartDate IS NULL
			SET @ErrorMsg +=  NCHAR(13) + '  Start value isn''t specified, while target update indicator column is.';

IF @ErrorMsg <> ''
	THROW 51000, @ErrorMsg, 1;  

-- Get target column list:

SELECT @GetTargetColumnList = 'USE [' + @TargetDBName + '];' + NCHAR(13) 
					+ 'SELECT @TargetColumnList = STRING_AGG(''['' + c.name + '']'', '','') WITHIN GROUP (ORDER BY c.column_id) 
					FROM sys.columns c 
					WHERE 1=1
						AND c.object_id = OBJECT_ID(''' + @TargetTableNameFull + ''')' + NCHAR(13) +
				'		AND c.is_computed = 0' + NCHAR(13) +		
	CASE WHEN @IncludeIdentity = 0 THEN '		AND c.is_identity = 0' ELSE '' END + NCHAR(13) +
					'GROUP BY c.object_id;'

EXEC sp_executesql @GetTargetColumnList, N'@TargetColumnList NVARCHAR(MAX) OUTPUT', @TargetColumnList OUTPUT

--Get join predicate:

SELECT @JoinPredicate += '		AND S.' + value + ' = D.' + value + NCHAR(13) 
FROM STRING_SPLIT(@TargetPKColumns, ',')  
WHERE RTRIM(value) <> '';

SELECT @JoinPredicate = RIGHT(@JoinPredicate, LEN(@JoinPredicate)-6);

-- Get column list for DELETE statement:

SELECT @DeleteColumnList = @TargetColumnList;

SELECT @GetDeleteColumnList += 'SELECT @DeleteColumnList = REPLACE(@DeleteColumnList, ''[' + value + '],'', '''');' + NCHAR(13)
FROM STRING_SPLIT(@TargetPKColumns, ',')
WHERE RTRIM(value) <> ''; 

exec sp_executesql @GetDeleteColumnList, N'@DeleteColumnList NVARCHAR(MAX) OUTPUT', @DeleteColumnList OUTPUT

-- Get @PartitionDate:

SELECT @InsertCommand = N'USE [' + @TargetDBName + '];' + NCHAR(13) 

IF @TargetUpdateIndicator <> @TargetPartitionColumn AND @TargetUpdateIndicator IS NOT NULL
	SELECT @InsertCommand = N'DECLARE  @StartDate datetime = ''' + CONVERT(nvarchar(8), @StartDate, 112) + '''' + NCHAR(13)
						+ '		,@PartitionDate datetime' + NCHAR(13)
						+ 'SELECT @PartitionDate = MIN([' + @TargetPartitionColumn + '])' + NCHAR(13)
						+ 'FROM ' + @TargetTableNameFull + NCHAR(13)
						+ 'WHERE [' + @TargetUpdateIndicator + '] >= ''' + CONVERT(nvarchar(8), @StartDate, 112) + '''' + NCHAR(13)
						+ 'OPTION(RECOMPILE);' + NCHAR(13)
ELSE 
	IF @TargetPartitionColumn IS NULL AND @TargetUpdateIndicator IS NOT NULL
		SELECT @InsertCommand = 'SELECT @StartDate = ''' + CONVERT(nvarchar(8), @StartDate, 112) + ''';' + NCHAR(13)

-- DELETE/INSERT changed rows:

IF @IncludeIdentity = 1 
	SELECT @InsertCommand += 'SET IDENTITY_INSERT [' + @TargetTableName + '] ON;' + NCHAR(13)

SELECT @InsertCommand += 'DELETE D' + NCHAR(13)
					+ 'FROM ' + @SourceTableNameFull + ' S' + NCHAR(13)
					+ 'INNER JOIN ' + @TargetTableNameFull + ' D' + NCHAR(13)
					+ '	ON ' + @JoinPredicate
					+ 'WHERE 1=1' + NCHAR(13)
					+ '	AND EXISTS' + NCHAR(13)
					+ '	(' + NCHAR(13)
					+ '		SELECT ' + REPLACE(@DeleteColumnList, '[', 'S.[') + NCHAR(13)
					+ '		EXCEPT' + NCHAR(13)
					+ '		SELECT ' + REPLACE(@DeleteColumnList, '[', 'D.[') + NCHAR(13)
					+ '	)' + NCHAR(13)
					+ CASE 
						WHEN @TargetPartitionColumn IS NOT NULL 
							THEN '	AND D.[' + @TargetPartitionColumn + '] >= @PartitionDate' + NCHAR(13)
						ELSE ''
					  END
					+ CASE						
						WHEN @TargetUpdateIndicator IS NOT NULL
							THEN '	AND D.[' + @TargetUpdateIndicator + '] >= @StartDate' + NCHAR(13)
						ELSE ''
					  END
					+ 'OPTION(RECOMPILE);' + NCHAR(13)
					+ 'INSERT INTO ' + @TargetTableNameFull + ' (' + @TargetColumnList + ')' + NCHAR(13)
					+ 'SELECT ' + REPLACE(@TargetColumnList, '[', 'S.[') + NCHAR(13)
					+ 'FROM ' + @SourceTableNameFull + ' S' + NCHAR(13)
					+ 'LEFT OUTER JOIN ' + @TargetTableNameFull + ' D' + NCHAR(13)
					+ '	ON ' + @JoinPredicate
					+ 'WHERE 1=1' + NCHAR(13)
					+ '	AND D.' + (SELECT TOP 1 value FROM STRING_SPLIT(@TargetPKColumns, ',')) + ' IS NULL' + NCHAR(13)
					+ CASE 
						WHEN @TargetPartitionColumn IS NOT NULL 
							THEN '	AND D.[' + @TargetPartitionColumn + '] >= @PartitionDate' + NCHAR(13)
						ELSE ''
					  END
					+ CASE						
						WHEN @TargetUpdateIndicator IS NOT NULL
							THEN '	AND D.[' + @TargetUpdateIndicator + '] >= @StartDate' + NCHAR(13)
						ELSE ''
					  END
					+ 'OPTION(RECOMPILE);' + NCHAR(13)
IF @IncludeIdentity = 1 
	SELECT @InsertCommand += 'SET IDENTITY_INSERT [' + @TargetTableName + '] OFF;' + NCHAR(13)
SELECT @InsertCommand += 'ALTER INDEX [CCI_' + @TargetTableName + '] ON [' + @TargetTableName + '] REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);'

IF @ExecuteCommands = 0
	PRINT CAST(@InsertCommand AS NTEXT)
ELSE 
	EXEC sp_executesql @InsertCommand;  
GO
