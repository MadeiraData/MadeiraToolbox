/*
Get Job Output File Contents
============================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2022-04-13
*/
DECLARE
	  @JobName sysname = N'Maintenance.IntegrityAndIndex'
	, @jobStepName sysname
	, @jobStepId int

SET NOCOUNT ON;
DECLARE @cmd nvarchar(MAX), @LogDirectory nvarchar(4000), @OutputFilePath nvarchar(MAX), @jobId varbinary(64)

SELECT @JobName = j.name
	, @jobId = CONVERT(varbinary(256), js.job_id)
	, @jobStepId = js.step_id
	, @jobStepName = js.step_name
	, @OutputFilePath = js.output_file_name
FROM msdb.dbo.sysjobs AS j
INNER JOIN msdb.dbo.sysjobsteps AS js ON j.job_id = js.job_id
WHERE j.name = @JobName AND js.output_file_name <> N''
AND (@jobStepName IS NULL OR js.step_name = @jobStepName)
AND (@jobStepId IS NULL OR js.step_id = @jobStepId)

IF @OutputFilePath IS NULL
BEGIN
	RAISERROR(N'Job "%s" is not a valid job with output.', 16,1,@JobName);
	SET NOEXEC ON;
END

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

--SELECT @OutputFilePath
SET @OutputFilePath =
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		@OutputFilePath
		, N'$(ESCAPE_SQUOTE(SQLLOGDIR))\', @LogDirectory)
		, N'$(ESCAPE_SQUOTE(JOBNAME))', @JobName)
		, N'$(ESCAPE_SQUOTE(JOBID))', CONVERT(nvarchar(4000), @jobId, 1))
		, N'$(ESCAPE_SQUOTE(STEPNAME))', @jobStepName)
		, N'$(ESCAPE_SQUOTE(STEPID))', @jobStepId)
		, N'$(ESCAPE_SQUOTE(DATE))', N'%')
		, N'$(ESCAPE_SQUOTE(TIME))', N'%')
		, N'$(ESCAPE_SQUOTE(STRTDT))', N'%')
		, N'$(ESCAPE_SQUOTE(STRTTM))', N'%')
		, N'[', N'_'), N']', N'_')

RAISERROR(N'Job Name: %s, Step Name: %s', 0, 1, @JobName, @jobStepName);
RAISERROR(N'Log Directory: %s', 0, 1, @LogDirectory);
RAISERROR(N'Output File Path Qualifier: %s', 0, 1, @OutputFilePath);

-- If there are wildcards in the path, we'll need to parse the folder contents to find the exact file
IF CHARINDEX('%', @OutputFilePath) > 0
BEGIN
	SET @LogDirectory = LEFT(@OutputFilePath, LEN(@OutputFilePath) - ISNULL(NULLIF(CHARINDEX(N'\', REVERSE(@OutputFilePath)),0),CHARINDEX(N'/', REVERSE(@OutputFilePath))) + 1)
	SET @OutputFilePath = REPLACE(@OutputFilePath, @LogDirectory, N'')
	
	RAISERROR(N'Searching in log folder: %s',0,1,@LogDirectory) WITH NOWAIT;

	DECLARE @FilesList table (id int IDENTITY(1,1) NOT NULL, subdir nvarchar(MAX) NULL, depth smallint NOT NULL, isFile tinyint NOT NULL);
	INSERT INTO @FilesList (subdir, depth, isFile)
	EXEC sys.xp_dirtree @LogDirectory, 0, 1

	SELECT TOP (1) @OutputFilePath = @LogDirectory + subdir
	FROM @FilesList
	WHERE subdir LIKE @OutputFilePath
	ORDER BY subdir DESC

	--SELECT @OutputFilePath AS [@OutputFilePath], @LogDirectory AS [@LogDirectory]
END


SET @cmd = N'SELECT BulkColumn AS OutputFileContents, @OutputFilePath AS OutputFilePath FROM OPENROWSET(BULK N'''
		+ REPLACE(@OutputFilePath, N'''', N'''''') + N''', SINGLE_NCLOB) AS d'

RAISERROR(N'Output File Path: %s', 0, 1, @OutputFilePath);
RAISERROR(N'Query Command: %s', 0, 1, @cmd);

IF @OutputFilePath IS NOT NULL AND @cmd IS NOT NULL
EXEC sp_executesql @cmd, N'@OutputFilePath nvarchar(max)', @OutputFilePath;

SET NOEXEC OFF;