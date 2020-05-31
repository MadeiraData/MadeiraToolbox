/*
----------------------------------------------------------------------------
		Grow a Database File in Specified Increments
----------------------------------------------------------------------------
Author: Eitan Blumin | https://www.eitanblumin.com
Creation Date: 2020-03-30
----------------------------------------------------------------------------
Description:
	This script uses small intervals to grow a file (in the current database)

	This can be useful when growing data files without IFI,
	or transaction log files in controlled increments.

	Change the parameter values below to customize the behavior.
	
----------------------------------------------------------------------------
	!!! DON'T FORGET TO SET THE CORRECT DATABASE NAME !!!
----------------------------------------------------------------------------

Change log:
	2020-03-30 - First version
----------------------------------------------------------------------------

Parameters:
----------------------------------------------------------------------------
*/
DECLARE
	 @DatabaseName		SYSNAME = NULL		-- Leave NULL to use current database context
	,@FileName		SYSNAME	= NULL		-- Leave NULL to grow the file with the least free space percentage
	,@TargetSizeMB		INT	= 20000		-- Leave NULL to rely on @MinPercentFree exclusively.
	,@MinPercentFree	INT	= 80		-- Leave NULL to rely on @TargetSizeMB exclusively.
								-- Either @TargetSizeMB or @MinPercentFree must be specified.
								-- If both @TargetSizeMB and @MinPercentFree are provided, the largest of them will be used.
	,@IntervalMB		INT	= 1024	-- Leave NULL to grow using a single operation

----------------------------------------------------------------------------
		-- DON'T CHANGE ANYTHING BELOW THIS LINE --
----------------------------------------------------------------------------

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET ANSI_WARNINGS OFF;
DECLARE @CurrSizeMB INT, @StartTime DATETIME, @sp_executesql NVARCHAR(1000), @CMD NVARCHAR(MAX), @SpaceUsedMB INT, @SpaceUsedPct VARCHAR(10), @TargetPct VARCHAR(10);

SET @DatabaseName = ISNULL(@DatabaseName, DB_NAME());

IF @DatabaseName IS NULL
BEGIN
	RAISERROR(N'Database "%s" was not found on this server.',16,1,@DatabaseName);
	GOTO Quit;
END

IF DATABASEPROPERTYEX(@DatabaseName, 'Updateability') <> 'READ_WRITE'
BEGIN
	RAISERROR(N'Database "%s" is not writeable.',16,1,@DatabaseName);
	GOTO Quit;
END

IF @TargetSizeMB IS NULL AND @MinPercentFree IS NULL
BEGIN
	RAISERROR(N'Either @TargetSizeMB or @MinPercentFree must be specified!', 16, 1);
	GOTO Quit;
END

IF @IntervalMB < 1
BEGIN
	RAISERROR(N'@IntervalMB must be an integer value of 1 or higher (or NULL if you want to grow using a single interval)', 16,1)
	GOTO Quit;
END

SET @sp_executesql = QUOTENAME(@DatabaseName) + '..sp_executesql'

SET @CMD = N'
SELECT TOP 1
	 @FileName = [name]
	,@CurrSizeMB = size / 128
	,@SpaceUsedMB = CAST(FILEPROPERTY([name], ''SpaceUsed'') AS int) / 128.0
FROM sys.database_files
WHERE ([name] = @FileName OR @FileName IS NULL)
AND ([size] / 128 < @TargetSizeMB)
AND type IN (0,1) -- data and log files only
ORDER BY CAST(FILEPROPERTY([name], ''SpaceUsed'') AS float) / size DESC;'

EXEC @sp_executesql @CMD, N'@FileName SYSNAME OUTPUT, @CurrSizeMB INT OUTPUT, @SpaceUsedMB INT OUTPUT, @TargetSizeMB INT'
			, @FileName OUTPUT, @CurrSizeMB OUTPUT, @SpaceUsedMB OUTPUT, @TargetSizeMB
			
SET @TargetSizeMB = (
			SELECT MAX(val)
			FROM (VALUES
				(@TargetSizeMB),(CEILING(@SpaceUsedMB / (CAST(@MinPercentFree as float) / 100.0)))
				) AS v(val)
			)

SET @SpaceUsedPct = CAST( CEILING(@SpaceUsedMB * 100.0 / @CurrSizeMB) as varchar(10)) + '%'
SET @TargetPct = CAST( CEILING(@SpaceUsedMB * 100.0 / @TargetSizeMB) as varchar(10)) + '%'

IF @CurrSizeMB < @TargetSizeMB
BEGIN
	RAISERROR(N'-- File "%s" current size: %d MB, used space: %d MB (%s), target size: %d MB (%s)',0,1,@FileName,@CurrSizeMB,@SpaceUsedMB,@SpaceUsedPct,@TargetSizeMB,@TargetPct) WITH NOWAIT;
END
ELSE
BEGIN
	PRINT N'-- Nothing to grow'
	GOTO Quit;
END

SET @sp_executesql = 'master..sp_executesql'

WHILE @CurrSizeMB < @TargetSizeMB
BEGIN
	SET @CurrSizeMB = @CurrSizeMB+@IntervalMB
	IF @CurrSizeMB > @TargetSizeMB OR @IntervalMB IS NULL SET @CurrSizeMB = @TargetSizeMB
	
	SET @CMD = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' MODIFY FILE (NAME = ' + QUOTENAME(@FileName, N'''') + N', SIZE = ' + CONVERT(nvarchar, @CurrSizeMB) + N'MB); -- ' + CONVERT(nvarchar(25),GETDATE(),121)
	RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
	EXEC @sp_executesql @CMD
END

PRINT N'-- Done - ' + CONVERT(nvarchar(25),GETDATE(),121)
Quit: