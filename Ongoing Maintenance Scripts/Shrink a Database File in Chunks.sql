/*========================================================================================================================

Description:	Shrink a single database file in small chunks to minimize load on the server
Scope:			Database File
Author:			Yossi Hakikat, Madeira
Created:		04/07/2013
Last Updated:	04/07/2013
Notes:			Shrinking a file is a very bad habit, and it should be avoided as much as possible.
				When there is no choice, it is better to do it in small chunks and during low activity.
				The ideal chunk size is different for each system,
				so it might be a good idea to start with small chunks and increase them gradually.

=========================================================================================================================*/


USE
	DatabaseName;
GO


SET NOCOUNT ON;
GO


-- User input variables

DECLARE
	@intTargetSize_MB		AS INT		= 100 ,			-- Enter the target file size in MB
	@intChuckSize_MB		AS INT		= 100 ,			-- Enter the size of each iteration in MB
	@sysFileName			AS SYSNAME	= N'FileName' ,	-- Enter the name of the file to shrink
	@intNumberOfIIterations	AS INT		= 0 ,			-- Enter the number of iterations
														-- If the target size is reached before the requested number of iterations, then the process will stop
														-- Set to 0 for an unlimited number of iterations
	@bitDebug				AS BIT		= 0;			-- Set to 1 to print debugging messages during the process


-- Internal use variables

DECLARE
	@intInitialFileSize_MB		AS INT ,			-- The initial size of the file in MB
	@intCurrentFileSize_MB		AS INT ,			-- The current size of the file (in MB) in each iteration
	@intPreviousFileSize_MB		AS INT ,			-- The size of the file (in MB) in the previous iteration
	@intIterationNumber			AS INT ,			-- The current iteration number (starting with 1)
	@intNextTargetFileSize_MB	AS INT ,			-- The target size of the file (in MB) in the next iteration
	@intFinalFileSize_MB		AS INT ,			-- The final size of the file (in MB) after shrinking
	@dt2Now						AS DATETIME2(7) ,	-- Used to calculate the duration of each iteration
	@intDuration_MS				AS INT ,			-- The duration of each iteration in milliseconds
	@intDuration_Sec			AS INT ,			-- The duration of each iteration in seconds
	@intMoreToGo_MB				AS INT;				-- The remaining size in MB still left to free up


-- Verify that the file exists

IF
	NOT EXISTS
		(
			SELECT
				NULL
			FROM
				sys.database_files
			WHERE
				name = @sysFileName
		)
BEGIN

	RAISERROR (N'The requested database file doesn''t exist' , 16 , 1);

	RETURN;

END;


-- Measure the current file size before any shrinking

SELECT
	@intInitialFileSize_MB = CAST (ROUND (size / 128.0 , 0) AS INT)
FROM
	sys.database_files
WHERE
	name = @sysFileName;


-- Verify that the target size is actually smaller than the initial size

IF
	@intTargetSize_MB >= @intInitialFileSize_MB
BEGIN

	RAISERROR (N'The target file size is larger or equal to the current file size' , 16 , 1);

	RETURN;

END;


-- If debugging is enabled, print the initial file size

IF
	@bitDebug = 1
BEGIN

	RAISERROR (N'The initial file size is %dMB' , 0 , 0 , @intInitialFileSize_MB) WITH NOWAIT;

END;


-- Initialize variables before the loop

SET @intCurrentFileSize_MB	= @intInitialFileSize_MB;
SET @intPreviousFileSize_MB	= 0;
SET @intIterationNumber		= 1;


-- Continue to shrink the file in chunks as long as all of the following conditions are met:
-- 1. The current size of the file is still larger than the target size
-- 2. The size of the file has changed in the last iteration (this is important in order to avoid an infinite loop)
-- 3. The requseted number of itearations has not been reached yet, or there is no limit on the number of iterations (@intNumberOfIIterations = 0)

WHILE
	@intCurrentFileSize_MB > @intTargetSize_MB
AND
	@intCurrentFileSize_MB != @intPreviousFileSize_MB
AND
	(@intIterationNumber <= @intNumberOfIIterations OR @intNumberOfIIterations = 0)
BEGIN


	-- Set the next target file size and the current date & time

	SET @intNextTargetFileSize_MB	= @intCurrentFileSize_MB - @intChuckSize_MB;
	SET @dt2Now						= SYSDATETIME ();


	-- Shrink the file (but don't make the file smaller than the target size)

	IF
		@intNextTargetFileSize_MB >= @intTargetSize_MB
	BEGIN

		DBCC SHRINKFILE (@sysFileName , @intNextTargetFileSize_MB) WITH NO_INFOMSGS;

	END
	ELSE
	BEGIN

		DBCC SHRINKFILE (@sysFileName , @intTargetSize_MB) WITH NO_INFOMSGS;

	END;


	-- Calculate the duration of the iteration

	SET @intDuration_MS = DATEDIFF (MILLISECOND , @dt2Now , SYSDATETIME ());


	-- Measure the current file size at the end of each iteration

	SET @intPreviousFileSize_MB = @intCurrentFileSize_MB;

	SELECT
		@intCurrentFileSize_MB = CAST (ROUND (size / 128.0 , 0) AS INT)
	FROM
		sys.database_files
	WHERE
		name = @sysFileName;


	-- Calculate the file space still left to release

	SET @intMoreToGo_MB = @intCurrentFileSize_MB - @intTargetSize_MB;


	-- You can add this delay between iterations in order to allow better concurrency

	-- WAITFOR DELAY '00:00:02';


	-- If debugging is enabled, display the current status

	IF
		@bitDebug = 1
	BEGIN

		IF
			@intDuration_MS > 10000
		BEGIN

			SET @intDuration_Sec = CAST (ROUND (@intDuration_MS / 1000.0 , 0) AS INT);

			RAISERROR (N'Iteration #%d: The file has been shrunk from %dMB to %dMB in %d seconds. There are still %dMB to go...' , 0 , 0 , @intIterationNumber , @intPreviousFileSize_MB , @intCurrentFileSize_MB , @intDuration_Sec , @intMoreToGo_MB) WITH NOWAIT;

		END
		ELSE
		BEGIN

			RAISERROR (N'Iteration #%d: The file has been shrunk from %dMB to %dMB in %d milliseconds. There are still %dMB to go...' , 0 , 0 , @intIterationNumber , @intPreviousFileSize_MB , @intCurrentFileSize_MB , @intDuration_MS , @intMoreToGo_MB) WITH NOWAIT;

		END;

	END;


	-- Increase the iteration number by 1

	SET @intIterationNumber += 1;


END;


-- If debugging is enabled, print the final file size

IF
	@bitDebug = 1
BEGIN

	RAISERROR (N'The final file size is %dMB' , 0 , 0 , @intCurrentFileSize_MB) WITH NOWAIT;

END;
GO
