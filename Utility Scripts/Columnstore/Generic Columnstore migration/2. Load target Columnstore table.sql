/*
Description:
The following script generates a command that migrates data from the source to the target table.
Notes:
	1) Choose @IncrementColumn according to source partition/CI column to avoid full table scans each chunk
		a) if your @IncrementColumn is a date, define @IncrementValue as 'MM, 1' or 'DD, 1', depending on the desired chunk size
		b) if your @IncrementColumn is a number, define @IncrementValue as '1045678' or more, depending on the desired chunk size 
		

*/

USE [master]
GO

DECLARE  @SourceDBName sysname = 'MySourceDB'
		,@SourceSchemaName sysname = 'MySourceShema'
		,@SourceTableName sysname = 'MySourceTable'
		,@TargetDBName sysname = 'MyTargetDB'
		,@TargetSchemaName sysname = 'MyTargetShema'
		,@TargetTableName sysname = 'MyTargetTable'
		,@IncrementColumn sysname = NULL--'MyTargetPartitionColumn'
		,@IncrementValue varchar(10) = NULL--'MM, 1'
		,@EndDate nvarchar(8) = CONVERT(nvarchar(8), GETDATE(), 112) -- desired last day in the target table
		,@IncludeIdentity bit = 1
		,@ExecuteInsert bit = 0

-----------------------------------------------------------------------------------

-- Declare internal variables: 

DECLARE  @InsertCommand NVARCHAR(MAX) = ''
		,@SourceTableNameFull NVARCHAR(200) = '[' + @SourceSchemaName + '].[' +  @SourceTableName + ']'
		,@GetSourceColumnList NVARCHAR(MAX) = ''
		,@SourceColumnList NVARCHAR(MAX) = ''
		,@TargetTableNameFull NVARCHAR(200) = '[' + @TargetSchemaName + '].[' +  @TargetTableName + ']'
		,@GetTargetColumnList NVARCHAR(MAX) = ''
		,@TargetColumnList NVARCHAR(MAX) = ''
		,@IncrementColumnDatatype sysname = ''
		,@GetIncrementColumnDatatype NVARCHAR(MAX) = ''
		,@ErrorMsg NVARCHAR(2048) = ''
		,@CheckVariables NVARCHAR(MAX) = ''

-- Checks:

IF NOT EXISTS	
	(
		SELECT 1
		FROM sys.databases db
		WHERE db.name IN (@SourceDBName)
	)
	SET @ErrorMsg +=  NCHAR(13) + '  Source DB doesn''t exist.';

IF NOT EXISTS	
	(
		SELECT 1
		FROM sys.databases db
		WHERE db.name IN (@TargetDBName)
	)
	SET @ErrorMsg +=  NCHAR(13) + '  Target DB doesn''t exist.';

IF @ErrorMsg <> ''
THROW 51000, @ErrorMsg, 1;  

SELECT @CheckVariables += NCHAR(13) + 'USE [' + @SourceDBName + ']' + NCHAR(13) 
					+ 'IF OBJECT_ID(''' + @SourceTableNameFull + ''') IS NULL' + NCHAR(13) 
					+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Source table doesn''''t exist.''' + NCHAR(13);

SELECT @CheckVariables += NCHAR(13) + 'USE [' + @TargetDBName + ']' + NCHAR(13) 
					+ 'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NULL' + NCHAR(13) 
					+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Target table doesn''''t exist.''' + NCHAR(13)
					+ CASE WHEN @IncrementColumn IS NOT NULL 
						THEN 'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
							+ ' AND NOT EXISTS ( SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(''' + @TargetTableNameFull + ''') AND name = ''' + @IncrementColumn + ''')' + NCHAR(13) 
							+ '		SET @ErrorMsg +=  NCHAR(13) + ''  Target partition column doesn''''t exist.''' + NCHAR(13) 
						ELSE ''
						END	;

exec sp_executesql @CheckVariables, N'@ErrorMsg NVARCHAR(2048) OUTPUT', @ErrorMsg OUTPUT

IF @IncrementColumn IS NULL AND @IncrementValue IS NOT NULL
	SET @ErrorMsg +=  NCHAR(13) + '  Increment column isn''t specified, while increment value is.';
	ELSE 
		IF @IncrementColumn IS NOT NULL AND @IncrementValue IS NULL
			SET @ErrorMsg +=  NCHAR(13) + '  Increment value isn''t specified, while increment column is.';

IF @ErrorMsg <> ''
THROW 51000, @ErrorMsg, 1;  

-- Get source column list:

SELECT @GetSourceColumnList = 'USE [' + @SourceDBName + '];' + NCHAR(13) 
					+ 'SELECT @SourceColumnList = STRING_AGG(''['' + c.name + '']'', '','') WITHIN GROUP (ORDER BY c.column_id) FROM sys.columns c WHERE c.object_id = OBJECT_ID(@SourceTableNameFull) GROUP BY c.object_id;'

EXEC sp_executesql @GetSourceColumnList, N'@SourceColumnList NVARCHAR(MAX) OUTPUT, @SourceTableNameFull NVARCHAR(200)', @SourceColumnList OUTPUT, @SourceTableNameFull;

-- Get target column list:

SELECT @GetTargetColumnList = 'USE [' + @TargetDBName + '];' + NCHAR(13) 
					+ 'SELECT @TargetColumnList = STRING_AGG(''['' + c.name + '']'', '','') WITHIN GROUP (ORDER BY c.column_id) 
					FROM sys.columns c 
					WHERE 1=1
						AND c.object_id = OBJECT_ID(''' + @TargetTableNameFull + ''')' + NCHAR(13) +
				'		AND c.is_computed = 0' + NCHAR(13) +		
	CASE WHEN @IncludeIdentity = 0 THEN '		AND c.is_identity = 0' ELSE '' END + NCHAR(13) +
					'GROUP BY c.object_id;'

EXEC sp_executesql @GetTargetColumnList, N'@TargetColumnList NVARCHAR(MAX) OUTPUT', @TargetColumnList OUTPUT;

-- Get @IncrementColumn datatype

IF @IncrementColumn IS NOT NULL
BEGIN
	SELECT @GetIncrementColumnDatatype = 'USE [' + @SourceDBName + ']' + NCHAR(13) 
										+ 'SELECT @IncrementColumnDatatype = dt.name' + NCHAR(13) 
										+ 'FROM sys.columns c' + NCHAR(13)  
										+ 'INNER JOIN sys.types dt' + NCHAR(13) 
										+ '	ON c.system_type_id = dt.system_Type_id' + NCHAR(13) 
										+ 'WHERE object_id = OBJECT_ID(''' + @SourceTableNameFull + ''')' + NCHAR(13)  
										+ '	AND c.name = ''' + @IncrementColumn + '''' + NCHAR(13) 
	EXEC sp_executesql @GetIncrementColumnDatatype, N'@IncrementColumnDatatype sysname OUTPUT', @IncrementColumnDatatype OUTPUT;
END

-- Get @StartDate:

SELECT @InsertCommand = 'USE [' + @TargetDBName + ']' + NCHAR(13) 
					+ CASE 
						WHEN @IncrementColumn IS NULL
							THEN ''
						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%date%'
							THEN	+ 'DECLARE  @StartDate ' + @IncrementColumnDatatype + NCHAR(13) 
									+ '		,@EndDate datetime = ''' + @EndDate + '''' + NCHAR(13) 
									+ 'SELECT @StartDate = DATEADD(DD, 1, MAX([' + @IncrementColumn + ']))' + NCHAR(13) 
									+ 'FROM ' + @TargetTableNameFull + ';' + NCHAR(13) 
									+ 'IF @StartDate IS NULL' + NCHAR(13) 
									+ '	SELECT @StartDate = DATEADD(month, DATEDIFF(month, 0, MIN([' + @IncrementColumn + '])), 0)' + NCHAR(13) 
									+ CASE WHEN @SourceDBName = @TargetDBName
										THEN '	FROM [' + @SourceSchemaName + '].[' + @SourceTableName + '];' + NCHAR(13) 
										ELSE '	FROM [' + @SourceDBName + '].[' + @SourceSchemaName + '].[' + @SourceTableName + '];' + NCHAR(13) 
									 END

						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%int%'
							THEN	+ 'DECLARE  @StartID ' + @IncrementColumnDatatype + NCHAR(13) 
									+ '		,@EndID ' + @IncrementColumnDatatype + NCHAR(13) 
									+ 'SELECT @StartID = MAX([' + @IncrementColumn + '])+1' + NCHAR(13) 
									+ 'FROM ' + @TargetTableNameFull + ';' + NCHAR(13) 
									+ 'SELECT @EndID = MAX([' + @IncrementColumn + '])' + NCHAR(13) 
									+ 'FROM ' + @SourceTableNameFull + ';' + NCHAR(13) 
									+ 'IF @StartID IS NULL' + NCHAR(13) 
									+ '	SELECT @StartDate = MIN([' + @IncrementColumn + '])' + NCHAR(13) 
									+ '	FROM [' + @SourceDBName + '].[' + @SourceSchemaName + '].[' + @SourceTableName + '];' + NCHAR(13) 
						END
-- Include/skip IDENTITY_INSERT ON:
					+ CASE WHEN @IncludeIdentity = 1 THEN 'SET IDENTITY_INSERT [' + @TargetTableName + '] ON;' + NCHAR(13) ELSE '' END
-- Generate the insert statement:
					+ CASE 
						WHEN @IncrementColumn IS NULL
							THEN ''
						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%date%'
							THEN	+ 'WHILE @StartDate <= @EndDate' + NCHAR(13) 
									+ 'BEGIN' + NCHAR(13)
						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%int%'
							THEN	+ 'WHILE @StartID <= @EndID' + NCHAR(13) 
									+ 'BEGIN' + NCHAR(13)	
						ELSE ''
						END 
					+ '	INSERT INTO ' + @TargetTableNameFull + ' (' + @TargetColumnList + ')' + NCHAR(13)
					+ '	SELECT ' + @SourceColumnList + NCHAR(13)
					+ '	FROM [' + @SourceDBName + '].[' + @SourceSchemaName + '].[' + @SourceTableName + ']'
					+ CASE
						WHEN @IncrementColumn IS NULL
							THEN ';' + NCHAR(13)
						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%date%'
							THEN	+ NCHAR(13) + '	WHERE [' + @IncrementColumn + '] >= @StartDate' + NCHAR(13)
									+ '		AND [' + @IncrementColumn + '] < DATEADD(' + @IncrementValue + ', @StartDate)' + NCHAR(13)
									+ '	OPTION(RECOMPILE);' + NCHAR(13)
						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%int%'
							THEN	+ NCHAR(13) + '	WHERE [' + @IncrementColumn + '] >= @StartID' + NCHAR(13)
									+ '		AND [' + @IncrementColumn + '] < @StartID + ' + @IncrementValue + NCHAR(13) 
									+ '	OPTION(RECOMPILE);' + NCHAR(13)
						ELSE  ';' + NCHAR(13)
						END 
-- Advance @StartDate/StartID:
					+ CASE
						WHEN @IncrementColumn IS NULL
							THEN ''
						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%date%'
							THEN	+ '	SET @StartDate = DATEADD(' + @IncrementValue + ', @StartDate);' + NCHAR(13)
									+ 'END' + NCHAR(13)
						WHEN @IncrementColumn IS NOT NULL AND @IncrementColumnDatatype LIKE '%int%'
							THEN	+ '	SET @StartID += ' + @IncrementValue + ';' + NCHAR(13)
									+ 'END' + NCHAR(13)
						ELSE ''
						END 
-- Include/skip IDENTITY_INSERT OFF:
					+ CASE WHEN @IncludeIdentity = 1 THEN 'SET IDENTITY_INSERT [' + @TargetTableName + '] OFF;' + NCHAR(13) ELSE '' END
-- Reorganize CS:
					+ 'ALTER INDEX [CCI_' + @TargetTableName + '] ON ' + @TargetTableNameFull + ' REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);'

-- PRINT/EXEC @CreateTableCommand, according to @ExecuteCreation:

IF @ExecuteInsert = 0
	PRINT @InsertCommand;
ELSE 
	EXEC sp_executesql  @InsertCommand;
GO
