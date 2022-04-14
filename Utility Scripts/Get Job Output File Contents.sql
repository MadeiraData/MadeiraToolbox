/*
Get Job Output File Contents
============================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2022-04-13
*/
DECLARE @JobName sysname = N'DatabaseBackup - USER_DATABASES - FULL'

SET NOCOUNT ON;

IF NOT EXISTS
(
SELECT NULL
FROM msdb.dbo.sysjobs AS j
INNER JOIN msdb.dbo.sysjobsteps AS js ON j.job_id = js.job_id
WHERE j.name = @JobName AND js.output_file_name <> N''
)
BEGIN
	RAISERROR(N'Job "%s" is not a valid job with output.', 16,1,@JobName);
END

DECLARE @LogDirectory nvarchar(4000), @cmd nvarchar(MAX), @OutputFilePath nvarchar(MAX);
DECLARE @FilesList table (id int IDENTITY(1,1) NOT NULL, subdir nvarchar(MAX) NULL, depth smallint NOT NULL, isFile tinyint NOT NULL);

IF CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff) >= 11
BEGIN
	SELECT @LogDirectory = [path]
	FROM sys.dm_os_server_diagnostics_log_configurations
	OPTION(RECOMPILE)
END
ELSE
BEGIN
	SET @LogDirectory = LEFT(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)),LEN(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max))) - CHARINDEX('\',REVERSE(CAST(SERVERPROPERTY('ErrorLogFileName') AS nvarchar(max)))))
END

RAISERROR(N'Job Name: %s', 0, 1, @JobName);
RAISERROR(N'Log Directory: %s', 0, 1, @LogDirectory);

INSERT INTO @FilesList (subdir, depth, isFile)
EXEC sys.xp_dirtree @LogDirectory, 0, 1

SELECT TOP (1) @OutputFilePath = @LogDirectory + subdir
FROM @FilesList
WHERE subdir LIKE @JobName + N'%[_]%[_]%.txt'
ORDER BY subdir DESC

SET @cmd = N'SELECT BulkColumn AS OutputFileContents, @OutputFilePath AS OutputFilePath FROM OPENROWSET(BULK N' + QUOTENAME(@OutputFilePath, N'''') + N', SINGLE_NCLOB) AS d'

RAISERROR(N'Output File Path: %s', 0, 1, @OutputFilePath);
RAISERROR(N'Query Command: %s', 0, 1, @cmd);

IF @OutputFilePath IS NOT NULL AND @cmd IS NOT NULL
EXEC sp_executesql @cmd, N'@OutputFilePath nvarchar(max)', @OutputFilePath;
