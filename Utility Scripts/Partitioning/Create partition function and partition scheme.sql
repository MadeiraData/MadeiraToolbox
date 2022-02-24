/*
Description:
The following script generates a command that creates a dedicated PF/PS for a desired table.
Notes:

*/

USE [master]
GO

--User parameters:

DECLARE  @DBName sysname = 'CorpSms_Reports_Archive'
		,@SchemaName sysname = 'dbo'
		,@TableName sysname = 'MSG_Messages_Archive'
		,@PartitionFunctionType nvarchar(50) = 'datetime' 
		,@PartitionFunctionRange nvarchar(10) = 'RIGHT' 
		,@StartTime datetime = '20211101'
		,@EndTime datetime = '20221101'
		,@FG nvarchar(50) = 'ARCHIVE'
		,@ExecuteCommands BIT = 0

--------------------------------------------------------------------

DECLARE  @PartitionFunctionName nvarchar(100) = 'PF_' + @TableName
		,@PartitionSchemeName nvarchar(100) = 'PS_' + @TableName
		,@CreatePFPS nvarchar(max) = ''

SET @CreatePFPS = N'USE [' + @DBName + '];' + NCHAR(13) 
				+ 'IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = N''' + @PartitionFunctionName + ''')' + NCHAR(13) 
				+ '	CREATE PARTITION FUNCTION ' + @PartitionFunctionName + ' (' + @PartitionFunctionType + ')' + NCHAR(13)  
				+ '		AS RANGE ' + @PartitionFunctionRange + ' FOR VALUES (' 

WHILE @StartTime < @EndTime
BEGIN  
IF @PartitionFunctionType IN ('datetime', 'datetime2')
	SET @CreatePFPS += '''' + CONVERT(nvarchar(8), @StartTime, 112) + '''' + N', ';  
SET @StartTime = DATEADD(MM, 1, @StartTime);  
END  
IF @PartitionFunctionType IN ('datetime', 'datetime2')
	SET @CreatePFPS += '''' + CONVERT(nvarchar(8), @StartTime, 112) + '''' + N');' + NCHAR(13);   

SET @CreatePFPS += 'IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = N'''  + @PartitionSchemeName + ''')' + NCHAR(13)
				+ '	CREATE PARTITION SCHEME ' + @PartitionSchemeName + NCHAR(13)
				+ '		AS PARTITION ' + @PartitionFunctionName + NCHAR(13)
				+ '		ALL TO ([' + @FG + ']);'

IF @ExecuteCommands = 0
BEGIN
	PRINT CAST(@CreatePFPS AS NTEXT)
END
ELSE 
BEGIN
	EXEC sp_executesql @CreatePFPS;   
END
GO 