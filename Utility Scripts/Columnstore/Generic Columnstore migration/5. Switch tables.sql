/*
Description:
The following script switches target table to production, by renaming the source and the target tables accordingly.
Notes:

*/

USE [master]
GO

DECLARE  @SourceDBName sysname = 'MySourceDB'
		,@SourceSchemaName sysname = 'MySourceShema'
		,@SourceTableName sysname = 'MySourceTable'
		,@SourceTableSuffix nvarchar(10) = 'old_RS'
		,@TargetDBName sysname = 'MyTargetDB'
		,@TargetSchemaName sysname = 'MyTargetShema'
		,@TargetTableName sysname = 'MyTargetTable'
		,@ExecuteCommands bit = 0

-----------------------------------------------------------------------------------------------------

-- Declare Internal variables: 

DECLARE  @SourceTableNameFull NVARCHAR(200) = N'[' + @SourceSchemaName + '].[' +  @SourceTableName + ']'
		,@TargetTableNameFull nvarchar(200) = N'[' + @TargetSchemaName + '].[' +  @TargetTableName + ']'
		,@RenameCommands nvarchar(max) = N''

SELECT @RenameCommands = N'USE [' + @SourceDBName + '];' + NCHAR(13) 

IF @SourceDBName = @TargetDBName
	SELECT @RenameCommands += N'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
					+ 'BEGIN' + NCHAR(13) 
					+ '	EXEC sp_rename ''' + @SourceTableNameFull + ''', ''' + @SourceTableName + '_' + @SourceTableSuffix + ''';' + NCHAR(13) 
					+ '	EXEC sp_rename ''' + @TargetTableNameFull + ''', ''' + @SourceTableName +''';' + NCHAR(13) 
					+ 'END'
ELSE
	SELECT @RenameCommands += N'IF OBJECT_ID(''' + @SourceTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
					+ '	EXEC sp_rename ''' + @SourceTableNameFull + ''', ''' + @SourceTableName + '_' + @SourceTableSuffix + ''';' + NCHAR(13) 
					+ 'USE [' + @TargetDBName + '];' + NCHAR(13) 
					+ 'IF OBJECT_ID(''' + @TargetTableNameFull + ''') IS NOT NULL' + NCHAR(13) 
					+ '	EXEC sp_rename ''' + @TargetTableNameFull + ''', ''' + @SourceTableName +''';' + NCHAR(13) 

IF @ExecuteCommands = 0
	PRINT @RenameCommands
ELSE 
	EXEC sp_executesql @RenameCommands;  
GO
