/*
Remove transaction log files from databases with more than one transaction log file
===================================================================================
Authors: Evyatar Karni and Eitan Blumin | https://www.madeiradata.com
Date Created: 2020-09-02
*/
DECLARE
	  @DBname		SYSNAME		= NULL
	, @TrnBackupFolderPath	NVARCHAR(500)	= NULL



SET ANSI_NULLS, QUOTED_IDENTIFIER, NOCOUNT ON;
DECLARE
	@Database_id INT = DB_ID(@DBname),
	@backup_file_name VARCHAR(250) = NULL,
	@Default_bck_path VARCHAR(1000) = NULL

IF OBJECT_ID('tempdb..#tmp_data') IS NOT NULL DROP TABLE #tmp_data;
CREATE TABLE #tmp_data(
	[database_id] INT,
	[database_name] SYSNAME,
	[Command] nvarchar(max)
)

/* ## Here the DEFAULT backup path is saved */
EXEC master.dbo.xp_instance_regread 
        N'HKEY_LOCAL_MACHINE', 
        N'Software\Microsoft\MSSQLServer\MSSQLServer',N'BackupDirectory', 
        @Default_bck_path OUTPUT,  
        'no_output' 


--### For testing ###
--SET @DBname = 'MyDB'
--SET @DBname = NULL
--SET @backup_file_name = @DBname +'_' + convert(varchar(500),GetDate(),112)

IF @TrnBackupFolderPath IS NULL
BEGIN
	SET @TrnBackupFolderPath = @Default_bck_path
END

IF @TrnBackupFolderPath <> 'NUL'
BEGIN
	SET @TrnBackupFolderPath = @TrnBackupFolderPath + '{BackupFileName}_log.trn'
END

-- ## Here we SET the BACKUP PATH for the coming backup ##
-- ### OPTIONAL ###
--SET @TrnBackupFolderPath  = 'D:/Log backup/{BackupFileName}_log.trn'

IF @DBname IS NOT NULL
BEGIN
	SET @backup_file_name = @DBname + '_' + convert(varchar(500),GetDate(),112)
	INSERT #tmp_data
	SELECT DB_ID(@DBname),@DBname, 'BACKUP LOG ' + QUOTENAME(@DBname) + ' TO DISK = N''' + REPLACE(@TrnBackupFolderPath, N'{BackupFileName}', @backup_file_name)  + ''' WITH COMPRESSION'
END

IF @DBname IS NULL
BEGIN
		
	DECLARE db_cntr CURSOR
	LOCAL FAST_FORWARD
	FOR	
	SELECT 
		database_id,
		DB_NAME(database_id)
	FROM sys.master_files mf WITH(NOLOCK)
	WHERE [type] = 1
	AND DATABASEPROPERTYEX(DB_NAME(database_id),'Status') = 'ONLINE'
	AND DATABASEPROPERTYEX(DB_NAME(database_id),'Updateability') = 'READ_WRITE'
	AND CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(database_id),'recovery')) <> 'SIMPLE'
	GROUP BY database_id
	HAVING COUNT(*) > 1
	
	OPEN db_cntr
	FETCH NEXT FROM db_cntr INTO @Database_id,@DBname

	WHILE @@FETCH_STATUS = 0
	BEGIN 
		SET @backup_file_name = @DBname + '_' + convert(varchar(500),GetDate(),112)

		INSERT #tmp_data
		SELECT @Database_id,@DBname, 'BACKUP LOG ' + QUOTENAME(@DBname) + ' TO DISK = N''' + REPLACE(@TrnBackupFolderPath, N'{BackupFileName}', @backup_file_name) + ''';'

		DECLARE @FileName NVARCHAR(4000)
		DECLARE scnd_crsr CURSOR
		LOCAL FAST_FORWARD
		FOR 
		SELECT [name]
		FROM sys.master_files
		WHERE [type] = 1 -- transaction log
		AND [database_id] = @Database_id 
		AND [file_id] > 2

		OPEN scnd_crsr
		FETCH NEXT FROM scnd_crsr INTO @FileName

		WHILE @@FETCH_STATUS = 0
		BEGIN
			IF NOT EXISTS(SELECT vlf_active FROM sys.dm_db_log_info(@Database_id) WHERE [file_id] > 2 AND vlf_active = 1) 
			BEGIN
				INSERT #tmp_data
				SELECT @Database_id,@DBname, 'ALTER DATABASE ' + QUOTENAME(@DBname) + ' REMOVE FILE ' + QUOTENAME(@FileName)
			END
			ELSE
			BEGIN
				PRINT 'The file ' + QUOTENAME(@FileName) + ' from Database ' + QUOTENAME(@DBname) + ' is still in use'
			END

			FETCH NEXT FROM scnd_crsr INTO @FileName
		END

		CLOSE scnd_crsr;
		DEALLOCATE scnd_crsr;

		FETCH NEXT FROM db_cntr INTO @Database_id,@DBname
	END

	CLOSE db_cntr;
	DEALLOCATE db_cntr;
END


SELECT *
FROM #tmp_data
