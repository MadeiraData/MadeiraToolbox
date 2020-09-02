use tempdb
go
DECLARE
	@SourceFolderPath NVARCHAR(4000) = N'C:\Temp\Passwords\'

SET NOCOUNT ON;
DECLARE @CMD NVARCHAR(4000), @CurrFile NVARCHAR(4000), @NumVal INT
SET @CMD = N'dir "' + @SourceFolderPath + N'" /S /B'
PRINT @SourceFolderPath
IF OBJECT_ID('tempdb..##AllPasswords') IS NOT NULL DROP TABLE ##AllPasswords;
CREATE TABLE ##AllPasswords (pwd NVARCHAR(4000), UNIQUE CLUSTERED (pwd) WITH (IGNORE_DUP_KEY = ON));

DECLARE @FileList AS TABLE
(FilePath NVARCHAR(4000));

INSERT @FileList
EXEC xp_cmdshell @CMD

SELECT TOP 1 @CurrFile = FilePath FROM @FileList WHERE FilePath NOT LIKE '%.txt' AND FilePath NOT LIKE '%.tar.gz' AND FilePath NOT LIKE '%.csv';

WHILE @@ROWCOUNT = 1
BEGIN
	SET @CMD = N'dir "' + @CurrFile + N'" /S /B'
	PRINT @CurrFile
	
	INSERT @FileList
	EXEC xp_cmdshell @CMD

	DELETE FROM @FileList WHERE FilePath = @CurrFile OR FilePath IS NULL;
	SELECT TOP 1 @CurrFile = FilePath FROM @FileList WHERE FilePath NOT LIKE '%.txt' AND FilePath NOT LIKE '%.tar.gz' AND FilePath NOT LIKE '%.csv';
END

SELECT @NumVal = COUNT(*) FROM @FileList WHERE FilePath LIKE '%.txt';
RAISERROR(N'Found %d password files in total',0,1, @NumVal);
SET @NumVal = 0;

DECLARE FilesCur CURSOR
LOCAL FAST_FORWARD
FOR
SELECT FilePath
FROM @FileList
WHERE FilePath LIKE '%.txt'

OPEN FilesCur

FETCH NEXT FROM FilesCur INTO @CurrFile

WHILE @@FETCH_STATUS = 0
BEGIN
	RAISERROR(N'Loading: %s',0,1,@CurrFile) WITH NOWAIT;

	EXEC (N'BULK INSERT ##AllPasswords FROM ''' + @CurrFile + ''' WITH (ROWTERMINATOR = ''0x0a'')');
	SET @NumVal = @NumVal + @@ROWCOUNT;

	FETCH NEXT FROM FilesCur INTO @CurrFile
END

CLOSE FilesCur
DEALLOCATE FilesCur

RAISERROR(N'Finished loading %d passwords from password files.',0,1,@NumVal) WITH NOWAIT;

--SELECT DISTINCT pwd FROM ##AllPasswords

DECLARE @deviations AS TABLE (Deviation NVARCHAR(100), LoginName NVARCHAR(1000));

INSERT INTO @deviations
SELECT 'Empty Password' AS Deviation, RTRIM(name) AS [Name]
FROM master.sys.sql_logins
WHERE ([password_hash] IS NULL OR PWDCOMPARE('', [password_hash]) = 1)
AND name NOT IN ('MSCRMSqlClrLogin')
AND name NOT LIKE '##MS_%##'
AND is_disabled = 0
UNION ALL
SELECT DISTINCT 'Login Name is the same as Password' AS Deviation, RTRIM(s.name) AS [Name] 
FROM master.sys.sql_logins s 
WHERE PWDCOMPARE(RTRIM(RTRIM(s.name)), s.[password_hash]) = 1
AND s.is_disabled = 0

-- Do the comparisons by chunks
IF OBJECT_ID('tempdb..#chunk') IS NOT NULL DROP TABLE #chunk;
CREATE TABLE #chunk (pwd NVARCHAR(4000));

WHILE 1 = 1
BEGIN
	
	DELETE TOP (1000) T
	OUTPUT deleted.pwd INTO #chunk
	FROM ##AllPasswords AS T;

	IF @@ROWCOUNT = 0
		BREAK;
		
	INSERT INTO @deviations
	SELECT DISTINCT N'Weak Password' AS Deviation, RTRIM(s.name) AS [LoginName]
	FROM #chunk d
	INNER JOIN master.sys.sql_logins s ON PWDCOMPARE(RTRIM(RTRIM(d.pwd)), s.[password_hash]) = 1
	WHERE s.is_disabled = 0

	TRUNCATE TABLE #chunk;
END

SELECT *
FROM @deviations

RAISERROR(N'Found %d logins with weak passwords.',0,1,@@ROWCOUNT);
