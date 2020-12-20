DECLARE
	@DatabaseName		SYSNAME		= 'MyDB',
	@TableName		SYSNAME		= 'MyTable',
	@DateTimeColumnName	SYSNAME		= 'MyColumn',
	@ThresholdDateTime	DATETIME	= DATEADD(DAY, -14, GETDATE()),
	@BatchSize		INT		= 10000,
	@SleepBetweenBatches	VARCHAR(17)	= '00:00:00.6'

SET NOCOUNT ON;

DECLARE @CMD NVARCHAR(MAX), @Executor NVARCHAR(1000);
SET @DatabaseName = ISNULL(@DatabaseName, DB_NAME());

IF DB_ID(@DatabaseName) IS NULL OR DATABASEPROPERTYEX(@DatabaseName, 'Updateability') <> 'READ_WRITE' OR DATABASEPROPERTYEX(@DatabaseName, 'Status') <> 'ONLINE'
	RAISERROR(N'Database "%s" is not found or not accessible or not writeable.', 16, 1, @DatabaseName);
ELSE
BEGIN
SET @CMD = N'
IF NOT EXISTS (SELECT NULL FROM sys.columns WHERE object_id = OBJECT_ID(@TableName) AND [name] = @ColumnName)
	RAISERROR(N''Column "%s" was not found for table "%s"!'',16,1, @ColumnName, @TableName);
ELSE
BEGIN
WHILE 1=1
BEGIN
	DELETE TOP (@BatchSize)
	FROM ' + @TableName + N'
	WHERE ' + @DateTimeColumnName + N' < @ThresholdDateTime

	IF @@ROWCOUNT = 0
		BREAK;

	IF @SleepBetweenBatches IS NOT NULL
		WAITFOR DELAY @SleepBetweenBatches;
END
END'

SET @Executor = QUOTENAME(@DatabaseName) + N'..sp_executesql'

PRINT N'Database: ' + @DatabaseName
PRINT @CMD;

EXEC @Executor @CMD
	, N'@TableName SYSNAME, @ColumnName SYSNAME, @BatchSize INT, @ThresholdDateTime DATETIME, @SleepBetweenBatches VARCHAR(17)'
	, @TableName, @DateTimeColumnName,@BatchSize, @ThresholdDateTime, @SleepBetweenBatches
END