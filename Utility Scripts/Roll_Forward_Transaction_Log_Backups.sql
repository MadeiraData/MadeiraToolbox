/*
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Date: August, 2015
Description:
Use this script to easily roll forward multiple transaction log backups from a given folder.

More info:
https://eitanblumin.com/2018/10/28/t-sql-script-to-roll-forward-transaction-log-backups
*/
DECLARE
	  @TransactionLogBackupFolder	VARCHAR(4000)	= 'C:\SqlDBBackupsMyDB'
	, @FileNameQualifier			VARCHAR(4000)	= 'MyDB_%.trn'
	, @DatabaseName					SYSNAME			= 'MyDB'
	, @PerformRecovery				BIT				= 0

SET NOCOUNT ON;

DECLARE @Output AS TABLE (Msg NVARCHAR(MAX));
DECLARE @CMD VARCHAR(4000)

-- Add backslash at end of path if doesn't exist already
IF RIGHT(@TransactionLogBackupFolder, 1) <> '\'
	SET @TransactionLogBackupFolder = @TransactionLogBackupFolder + '\'

-- Prepare and execute dir command
SET @CMD = 'dir /b "' + @TransactionLogBackupFolder + N'"'

INSERT INTO @Output
EXEC xp_cmdshell @CMD

-- Loop through all files that comply with the specified qualifier
DECLARE @CurrPath NVARCHAR(MAX)
DECLARE CM CURSOR FOR
SELECT *
FROM @Output
WHERE Msg LIKE @FileNameQualifier
ORDER BY Msg

OPEN CM
FETCH NEXT FROM CM INTO @CurrPath

WHILE @@FETCH_STATUS = 0
BEGIN
	-- Prepare and execute RESTORE LOG command
	SET @CMD = N'RESTORE LOG ' + QUOTENAME(@DatabaseName) + N' FROM  
DISK = N''' + @TransactionLogBackupFolder + @CurrPath + N''' WITH  
FILE = 1,  NORECOVERY,  NOUNLOAD,  STATS = 10'
	
	RAISERROR(@CMD,0,1) WITH NOWAIT;
	EXEC(@CMD);
	
	FETCH NEXT FROM CM INTO @CurrPath
END

CLOSE CM
DEALLOCATE CM

-- Perform final recovery if needed
IF @PerformRecovery = 1
BEGIN
	SET @CMD = N'RESTORE LOG ' + QUOTENAME(@DatabaseName) + N' WITH RECOVERY'
	RAISERROR(@CMD,0,1) WITH NOWAIT;
	EXEC(@CMD);
END

RAISERROR(N'Done.',0,1) WITH NOWAIT;

GO