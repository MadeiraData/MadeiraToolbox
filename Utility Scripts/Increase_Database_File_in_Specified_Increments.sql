/*
----------------------------------------------------------------------------
		Increase a Database File in Specified Increments
----------------------------------------------------------------------------
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Creation Date: 2021-07-14
----------------------------------------------------------------------------
Description:
	This script uses small intervals to grow a file (in the current database)
	up to a specific size or percentage (of used space).

	This can be useful when growing transaction log files 
	while minimizing VLF count.

	Change the parameter values below to customize the behavior.
	
----------------------------------------------------------------------------
	!!! DON'T FORGET TO SET THE CORRECT DATABASE NAME !!!
----------------------------------------------------------------------------

Change log:
	2021-07-14 - First version
----------------------------------------------------------------------------

Parameters:
----------------------------------------------------------------------------
*/
DECLARE
	 @DatabaseName		SYSNAME = NULL		-- Leave NULL to use current database context
	,@FileName		SYSNAME	= NULL		-- Leave NULL to grow the file with the lowest % free space
	,@FileType		SYSNAME	= 'LOG'		-- If @FileName is NULL, use this to filter for a specific file type (ROWS | LOG | NULL).
	,@TargetSizeMB		INT	= 1024 * 8		-- Leave NULL to rely on @MinPercentFree exclusively.
	,@MinPercentFree	INT	= 50		-- Leave NULL to rely on @TargetSizeMB exclusively.
								-- Either @TargetSizeMB or @MinPercentFree must be specified.
								-- If both @TargetSizeMB and @MinPercentFree are provided, the largest of them will be used.
	,@IntervalMB		INT	= 1024		-- Leave NULL to grow the file in a single interval
	,@DelayBetweenGrowths	VARCHAR(12) = '00:00:01' -- Delay to wait between growth iterations (in 'hh:mm[[:ss].mss]' format). Leave NULL to disable delay. For more info, see the 'time_to_execute' argument of WAITFOR DELAY: https://docs.microsoft.com/en-us/sql/t-sql/language-elements/waitfor-transact-sql?view=sql-server-ver15#arguments

	,@WhatIf		BIT	= 1		-- Set to 1 to only print the commands but not run them.

----------------------------------------------------------------------------
		-- DON'T CHANGE ANYTHING BELOW THIS LINE --
----------------------------------------------------------------------------

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET ANSI_WARNINGS OFF;
DECLARE @CurrSizeMB INT, @StartTime DATETIME, @sp_executesql NVARCHAR(1000);
DECLARE @CMD NVARCHAR(MAX), @SpaceUsedMB INT;
DECLARE @SpaceUsedPct VARCHAR(10), @TargetPct VARCHAR(10), @NewSizeMB INT;

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
AND ([size] / 128 < @TargetSizeMB OR @TargetSizeMB IS NULL OR [name] = @FileName)
AND type IN (0,1) -- data and log files only'
+ CASE WHEN @FileType IS NOT NULL THEN N'
AND type_desc = @FileType'
 ELSE N'' END
+ N'
ORDER BY CAST(FILEPROPERTY([name], ''SpaceUsed'') AS float) / size DESC;'

IF @WhatIf = 1 PRINT @CMD;
EXEC @sp_executesql @CMD, N'@FileType SYSNAME, @FileName SYSNAME OUTPUT, @CurrSizeMB INT OUTPUT, @SpaceUsedMB INT OUTPUT, @TargetSizeMB INT'
			, @FileType, @FileName OUTPUT, @CurrSizeMB OUTPUT, @SpaceUsedMB OUTPUT, @TargetSizeMB

SET @TargetSizeMB = (
			SELECT MAX(val)
			FROM (VALUES
				(@TargetSizeMB),(CEILING(@SpaceUsedMB / (CAST(@MinPercentFree as float) / 100.0)))
				) AS v(val)
			)

SET @SpaceUsedPct = CAST( CEILING(@SpaceUsedMB * 100.0 / @CurrSizeMB) as varchar(10)) + '%'
SET @TargetPct = CAST( CEILING(@SpaceUsedMB * 100.0 / @TargetSizeMB) as varchar(10)) + '%'

IF @SpaceUsedMB IS NOT NULL
	RAISERROR(N'-- File "%s" current size: %d MB, used space: %d MB (%s), target size: %d MB (%s)',0,1,@FileName,@CurrSizeMB,@SpaceUsedMB,@SpaceUsedPct,@TargetSizeMB,@TargetPct) WITH NOWAIT;

IF @SpaceUsedMB IS NULL OR @CurrSizeMB >= @TargetSizeMB
BEGIN
	PRINT N'-- Nothing to grow'
	GOTO Quit;
END

WHILE @CurrSizeMB < @TargetSizeMB
BEGIN
	SET @CurrSizeMB = @CurrSizeMB+@IntervalMB
	IF @CurrSizeMB > @TargetSizeMB OR @IntervalMB IS NULL SET @CurrSizeMB = @TargetSizeMB
	
	SET @CMD = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' MODIFY FILE (NAME = N' + QUOTENAME(@FileName, N'''') + N' , SIZE = ' + CONVERT(nvarchar, @CurrSizeMB) + N'MB); -- ' + CONVERT(nvarchar(25),GETDATE(),121)
	RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;

	IF @WhatIf = 1
		PRINT N'-- @WhatIf was set to 1. Skipping execution.'
	ELSE
	BEGIN
		EXEC @sp_executesql @CMD

		-- Re-check new file size
		EXEC @sp_executesql N'SELECT @NewSizeInMB = [size]/128 FROM sys.database_files WHERE [name] = @FileName;'
					, N'@FileName SYSNAME, @NewSizeInMB FLOAT OUTPUT', @FileName, @NewSizeMB OUTPUT
	
		-- See if target size was successfully reached
		IF @NewSizeMB < @CurrSizeMB
		BEGIN
			RAISERROR(N'-- Unable to grow beyond %d MB. Stopping operation.', 12, 1, @NewSizeMB) WITH NOWAIT;
			BREAK;
		END
	
		-- Sleep between iterations
		IF @DelayBetweenGrowths IS NOT NULL
			WAITFOR DELAY @DelayBetweenGrowths;
	END
END

PRINT N'-- Done - ' + CONVERT(nvarchar(25),GETDATE(),121)
Quit: