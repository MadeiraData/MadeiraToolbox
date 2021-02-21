DECLARE
	@DatabaseName			SYSNAME		= 'MyDB',
	@TableName			SYSNAME		= 'dbo.MyTable',
	@DateTimeColumnName		SYSNAME		= 'MyColumn',
	@ThresholdFilterExpression	NVARCHAR(MAX)	= N'DATEADD(DAY, -14, GETDATE())',
	@BatchSize			INT		= 10000,
	@SleepBetweenBatches		VARCHAR(17)	= '00:00:00.6',
	@WhatIf				BIT		= 1

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;

DECLARE @CMD NVARCHAR(MAX), @Executor NVARCHAR(1000);
SET @DatabaseName = ISNULL(@DatabaseName, DB_NAME());

IF DB_ID(@DatabaseName) IS NULL OR DATABASEPROPERTYEX(@DatabaseName, 'Updateability') <> 'READ_WRITE' OR DATABASEPROPERTYEX(@DatabaseName, 'Status') <> 'ONLINE'
	RAISERROR(N'Database "%s" is not found or not accessible or not writeable.', 16, 1, @DatabaseName);
ELSE
BEGIN
    SET @Executor = QUOTENAME(@DatabaseName) + N'..sp_executesql'
    PRINT N'USE ' + QUOTENAME(@DatabaseName)

    DECLARE @Validator BIT = 0;
    EXEC @Executor N'SELECT @Validator = 1 FROM sys.columns WHERE object_id = OBJECT_ID(@TableName) AND [name] = @DateTimeColumnName'
		, N'@TableName SYSNAME, @DateTimeColumnName SYSNAME, @Validator BIT OUTPUT', @TableName, @DateTimeColumnName, @Validator OUTPUT

    IF @Validator = 0
	RAISERROR(N'Column "%s" was not found for table "%s"!',16,1, @DateTimeColumnName, @TableName);
    ELSE
    BEGIN
        SET @CMD = N'DECLARE @ThresholdDateTime DATETIME = ' + @ThresholdFilterExpression + N';

WHILE 1=1
BEGIN
        DELETE TOP (' + CONVERT(nvarchar(max), @BatchSize) + N')
        FROM ' + @TableName + N'
        WHERE ' + @DateTimeColumnName + N' < @ThresholdDateTime
        
        IF @@ROWCOUNT = 0
        	BREAK;
        ' + CASE WHEN @SleepBetweenBatches IS NOT NULL THEN N'
        WAITFOR DELAY ' + QUOTENAME(@SleepBetweenBatches, N'''') + N';'
		ELSE N''
		END + N'
END'
        
        PRINT @CMD;
        
	IF @WhatIf = 0 EXEC @Executor @CMD
    END
END