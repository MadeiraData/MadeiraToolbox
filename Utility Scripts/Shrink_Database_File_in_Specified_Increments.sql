/*
----------------------------------------------------------------------------
		Shrink a Database File in Specified Increments
----------------------------------------------------------------------------
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Creation Date: 2020-01-05
Last Update: 2020-06-21
----------------------------------------------------------------------------
Description:
	This script uses small intervals to shrink a file (in the current database)
	down to a specific size or percentage (of used space).

	This can be useful when shrinking files with large heaps and/or LOBs
	that can cause regular shrink operations to get stuck.

	Change the parameter values below to customize the behavior.
	
----------------------------------------------------------------------------
	!!! DON'T FORGET TO SET THE CORRECT DATABASE NAME !!!
----------------------------------------------------------------------------

Change log:
	2020-06-21 - Added @DelayBetweenShrinks and @IterationMaxRetries parameters
	2020-03-18 - Added @DatabaseName parameter
	2020-01-30 - Added @MinPercentFree, and made all parameters optional
	2020-01-05 - First version
----------------------------------------------------------------------------

Parameters:
----------------------------------------------------------------------------
*/
DECLARE
	 @DatabaseName		SYSNAME = NULL		-- Leave NULL to use current database context
	,@FileName		SYSNAME	= NULL		-- Leave NULL to shrink the file with the highest % free space
	,@TargetSizeMB		INT	= 20000		-- Leave NULL to rely on @MinPercentFree exclusively.
	,@MinPercentFree	INT	= 80		-- Leave NULL to rely on @TargetSizeMB exclusively.
								-- Either @TargetSizeMB or @MinPercentFree must be specified.
								-- If both @TargetSizeMB and @MinPercentFree are provided, the largest of them will be used.
	,@IntervalMB		INT	= 1		-- Leave NULL to shrink the file in a single interval
	,@DelayBetweenShrinks	VARCHAR(12) = '00:00:01' -- Delay to wait between shrink iterations (in 'hh:mm[[:ss].mss]' format). Leave NULL to disable delay. For more info, see the 'time_to_execute' argument of WAITFOR DELAY: https://docs.microsoft.com/en-us/sql/t-sql/language-elements/waitfor-transact-sql?view=sql-server-ver15#arguments
	,@IterationMaxRetries	INT	= 3		-- Maximum number of attempts per iteration to shrink a file, when cannot successfuly shrink to desired target size

----------------------------------------------------------------------------
		-- DON'T CHANGE ANYTHING BELOW THIS LINE --
----------------------------------------------------------------------------

SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET ANSI_WARNINGS OFF;
DECLARE @CurrSizeMB INT, @StartTime DATETIME, @sp_executesql NVARCHAR(1000), @CMD NVARCHAR(MAX), @SpaceUsedMB INT, @SpaceUsedPct VARCHAR(10), @TargetPct VARCHAR(10);
DECLARE @NewSizeMB INT, @RetryNum INT
SET @DatabaseName = ISNULL(@DatabaseName, DB_NAME());
SET @RetryNum = 0;

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
	RAISERROR(N'-- Either @TargetSizeMB or @MinPercentFree must be specified!', 16, 1);
	GOTO Quit;
END

IF @IntervalMB < 1
BEGIN
	RAISERROR(N'-- @IntervalMB must be an integer value of 1 or higher (or NULL if you want to shrink using a single interval)', 16,1)
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
AND ([size] / 128 > @TargetSizeMB OR @TargetSizeMB IS NULL)
AND type IN (0,1) -- data and log files only
ORDER BY CAST(FILEPROPERTY([name], ''SpaceUsed'') AS float) / size ASC;'

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

IF @CurrSizeMB > @TargetSizeMB
BEGIN
	RAISERROR(N'-- File "%s" current size: %d MB, used space: %d MB (%s), target size: %d MB (%s)',0,1,@FileName,@CurrSizeMB,@SpaceUsedMB,@SpaceUsedPct,@TargetSizeMB,@TargetPct) WITH NOWAIT;
END
ELSE
BEGIN
	PRINT N'-- Nothing to shrink'
	GOTO Quit;
END

WHILE @CurrSizeMB > @TargetSizeMB
BEGIN
	SET @CurrSizeMB = @CurrSizeMB-@IntervalMB
	IF @CurrSizeMB < @TargetSizeMB OR @IntervalMB IS NULL SET @CurrSizeMB = @TargetSizeMB
	
	SET @CMD = N'DBCC SHRINKFILE (N' + QUOTENAME(@FileName, N'''') + N' , ' + CONVERT(nvarchar, @CurrSizeMB) + N') WITH NO_INFOMSGS; -- ' + CONVERT(nvarchar(25),GETDATE(),121)
	RAISERROR(N'%s',0,1,@CMD) WITH NOWAIT;
	EXEC @sp_executesql @CMD
	
	-- Re-check new file size
	EXEC @sp_executesql N'SELECT @NewSizeInMB = [size]/128 FROM sys.database_files WHERE [name] = @FileName;'
				, N'@FileName SYSNAME, @NewSizeInMB FLOAT OUTPUT', @FileName, @NewSizeMB OUTPUT
	
	-- See if target size was successfully reached
	IF @NewSizeMB > @CurrSizeMB
	BEGIN
		IF @RetryNum >= @IterationMaxRetries
		BEGIN
			RAISERROR(N'Unable to shrink below %d MB. Stopping operation after %d retries.', 12, 1, @NewSizeMB, @RetryNum) WITH NOWAIT;
			BREAK;
		END
		ELSE
		BEGIN
			SET @RetryNum = @RetryNum + 1;
			SET @CurrSizeMB = @NewSizeMB;
			RAISERROR(N'-- Unable to shrink below %d MB. Retry attempt %d...', 0, 1, @NewSizeMB, @RetryNum) WITH NOWAIT;
		END
	END
	ELSE
		SET @RetryNum = 0;
	
	-- Sleep between iterations
	IF @DelayBetweenShrinks IS NOT NULL
		WAITFOR DELAY @DelayBetweenShrinks;
END

PRINT N'-- Done - ' + CONVERT(nvarchar(25),GETDATE(),121)
Quit: