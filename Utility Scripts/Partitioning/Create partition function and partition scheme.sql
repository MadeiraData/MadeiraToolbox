/*
Description:
The following script generates a command that creates a dedicated PF/PS for a desired table.
Notes:
TBA
*/
USE MyDB
GO

--User parameters:

DECLARE  @PartitionFunctionName sysname = 'PF_MyPartitionFunction'
	,@PartitionSchemeName sysname = 'PS_MyPartitionScheme'
	,@PartitionFunctionType sysname = 'datetime' 
	,@PartitionFunctionRange nvarchar(10) = 'RIGHT' 
	,@StartTime datetime = '20210101'
	,@EndTime datetime = '20210201'
	,@FG sysname = 'ARCHIVE_FG'
	,@ExecuteCommands BIT = 0

--------------------------------------------------------------------

DECLARE @CreatePFPS nvarchar(max)

SET @CreatePFPS = N'IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = N''' + @PartitionFunctionName + N''')' + NCHAR(13) 
		+ N'	CREATE PARTITION FUNCTION ' + @PartitionFunctionName + N' (' + @PartitionFunctionType + N')' + NCHAR(13)  
		+ N'		AS RANGE ' + @PartitionFunctionRange + N' FOR VALUES (' 

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
				+ '		ALL TO (' + QUOTENAME(@FG) + ');'

IF @ExecuteCommands = 0
BEGIN
	PRINT @CreatePFPS
END
ELSE 
BEGIN
	EXEC sp_executesql @CreatePFPS;   
END
GO