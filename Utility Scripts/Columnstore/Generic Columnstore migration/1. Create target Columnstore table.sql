/*
Description:
The following script generates a command that creates the target Columnstore table.
Notes:
	1) This script requires sp_GenerateTableDDLScript (https://github.com/EitanBlumin/sp_GenerateTableDDLScript) to exist in [master] DB.
*/

USE [master]
GO

DECLARE  @SourceDBName sysname = 'MySourceDB'
		,@SourceSchemaName sysname = 'MySourceShema'
		,@SourceTableName sysname = 'MySourceTable'
		,@TargetDBName sysname = 'MyTargetDB'
		,@TargetSchemaName sysname = 'MyTargetShema'
		,@TargetTableName sysname = 'MyTargetTable'
		,@TargetFG sysname = 'ARCHIVE'
		,@TargetPartitionScheme sysname = NULL--'PS_MySourceTable'
		,@TargetPartitionColumn sysname = NULL--'MyTargetPartitionColumn'
		,@CompressionMode NVARCHAR(20) = 'COLUMNSTORE'
		,@IncludeIdentity bit = 1
		,@DropTargetTable bit = 1
		,@ExecuteCreation bit = 0

-----------------------------------------------------------------------------------

-- Declare Internal variables: 

DECLARE  @CreateTableCommand NVARCHAR(MAX) = ''
		,@sp_GenerateTableDDLScript NVARCHAR(MAX) = ''
		,@SourceTableDefinition NVARCHAR(MAX) = ''
		,@SourceTableNameFull NVARCHAR(200) = '[' + @SourceSchemaName + '].[' +  @SourceTableName + ']'
		,@TargetTableNameFull NVARCHAR(200) = '[' + @TargetSchemaName + '].[' +  @TargetTableName + ']'
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
					+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Source table doesn''''t exist.''' + NCHAR(13)
					+ CASE WHEN @TargetPartitionColumn IS NOT NULL 
						THEN 'IF OBJECT_ID(''' + @SourceTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
							+ ' AND NOT EXISTS ( SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(''' + @SourceTableNameFull + ''') AND name = ''' + @TargetPartitionColumn + ''')' + NCHAR(13) 
							+ '		SET @ErrorMsg +=  NCHAR(13) + ''  Target partition column doesn''''t exist.''' + NCHAR(13) 
						END;

SELECT @CheckVariables += NCHAR(13) + 'USE [' + @TargetDBName + ']' + NCHAR(13) 
					+ 'IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N''' + @TargetSchemaName + ''')' + NCHAR(13)
					+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Target schema doesn''''t exist.''' + NCHAR(13) 
					+ CASE WHEN @TargetFG IS NOT NULL 
						THEN 'IF NOT EXISTS (SELECT * FROM sys.filegroups WHERE name = N''' + @TargetFG + ''')' + NCHAR(13) 
							+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Target FG doesn''''t exist.''' + NCHAR(13) 
						END 
					+ CASE WHEN @TargetPartitionScheme IS NOT NULL 
						THEN 'IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = N''' + @TargetPartitionScheme + ''')' + NCHAR(13) 
							+ '	SET @ErrorMsg +=  NCHAR(13) + ''  Target PS doesn''''t exist.''' + NCHAR(13) 
						END;

exec sp_executesql @CheckVariables, N'@ErrorMsg NVARCHAR(2048) OUTPUT', @ErrorMsg OUTPUT

IF @TargetFG IS NOT NULL AND @TargetPartitionScheme IS NOT NULL
	SELECT @ErrorMsg += NCHAR(13) + '	Please specify either @TargetFG or @TargetPartitionScheme, not both.'

IF @ErrorMsg <> ''
THROW 51000, @ErrorMsg, 1;  

-- Generate source table definition:

SELECT @sp_GenerateTableDDLScript = 'USE [' + @SourceDBName + '];' + NCHAR(13) 
								+ 'EXEC sp_GenerateTableDDLScript @SourceTableNameFull, @TargetTableNameFull, @SourceTableDefinition OUTPUT, @IncludeForeignKeys = 0, @IncludeIdentity = ' + CAST(@IncludeIdentity AS NCHAR(1)) + ', @IncludeIndexes = 0'

EXEC sp_executesql @sp_GenerateTableDDLScript, N'@SourceTableNameFull NVARCHAR(200), @TargetTableNameFull NVARCHAR(200), @SourceTableDefinition NVARCHAR(MAX) OUTPUT', @SourceTableNameFull, @TargetTableNameFull, @SourceTableDefinition OUTPUT

-- Add DROP/IF EXISTS check to @CreateTableCommand, according to @DropTargetTable:
IF @DropTargetTable = 1	
	SET @CreateTableCommand = 'USE [' + @TargetDBName + ']' + NCHAR(13) 
							+ 'IF OBJECT_ID(''[' + @TargetSchemaName + '].[' + @TargetTableName + ']'') IS NOT NULL' + NCHAR(13) 
							+ '	DROP TABLE [' + @TargetSchemaName + '].[' + @TargetTableName + '];' + NCHAR(13) 
							+ 'BEGIN'
ELSE 
	SET @CreateTableCommand = 'USE [' + @TargetDBName + '];' + NCHAR(13) 
							+ 'IF OBJECT_ID(''[' + @TargetSchemaName + '].[' + @TargetTableName + ']'') IS NULL' + NCHAR(13) 
							+ 'BEGIN'
-- Add source table definition and FG/PS to @CreateTableCommand:
SELECT @CreateTableCommand += @SourceTableDefinition 
							+ CASE WHEN @TargetFG IS NOT NULL
								THEN ' ON [' + @TargetFG + '];' + NCHAR(13)
								ELSE ' ON [' + @TargetPartitionScheme + ']([' + @TargetPartitionColumn + ']);' + NCHAR(13) 
							END +
-- Add Clustered Columnstore index definition to @CreateTableCommand:
							+ 'CREATE CLUSTERED COLUMNSTORE INDEX CCI_' + @TargetTableName + ' ON [' + @TargetDBName + '].[' + @TargetSchemaName + '].[' + @TargetTableName + ']' + NCHAR(13) 
							+ '	WITH ( DATA_COMPRESSION = ' + @CompressionMode + ')' + NCHAR(13) 
							+ CASE WHEN @TargetFG IS NOT NULL
								THEN ' ON [' + @TargetFG + '];' + NCHAR(13)
								ELSE ' ON [' + @TargetPartitionScheme + ']([' + @TargetPartitionColumn + ']);' + NCHAR(13) 
							END + NCHAR(13)
							+'END'

-- PRINT/EXEC @CreateTableCommand, according to @ExecuteCreation:

IF @ExecuteCreation = 0
	PRINT CAST(@CreateTableCommand AS NTEXT)
ELSE 
	EXEC sp_executesql @CreateTableCommand;
GO
