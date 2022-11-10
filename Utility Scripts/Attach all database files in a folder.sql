/*
====================================================
Attach all database files in a folder
====================================================
Author: Eitan Blumin
Date: 2022-08-22
Description:
	Scan all .mdf files in a folder and attach all un-attached files.
	It also performs some validations:
		Check for existing database with the same name.
		Check for the existence of all database files.
	This script is a variation of the script provided by SQLUndercover here:
	https://sqlundercover.com/2022/08/15/attach-all-sql-datafiles-in-a-directory/
====================================================
*/
DECLARE
	  @WhatIf bit = 1
	, @SourceMdfFolderPath VARCHAR(4000) = 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\DATA'
	, @NewDestinationDataFolderPath varchar(4000) = 'F:\MSSQL\DATA'
	, @NewDestinationLogsFolderPath varchar(4000) = 'E:\MSSQL\DATA'

SET NOCOUNT ON;

IF OBJECT_ID('tempdb.dbo.#DataFiles') IS NOT NULL DROP TABLE #DataFiles;
CREATE TABLE #DataFiles (filepath VARCHAR(4000), depth int, isfile tinyint)
 
IF OBJECT_ID('tempdb.dbo.#DBFiles') IS NOT NULL DROP TABLE #DBFiles;
CREATE TABLE #DBFiles(status INT,fileid INT,name sysname,filename VARCHAR(4000),newfilename varchar(4000) NULL)
 
IF OBJECT_ID('tempdb.dbo.#DBProperties') IS NOT NULL DROP TABLE #DBProperties;
CREATE TABLE #DBProperties(property sysname,[value] sql_variant)
 
DECLARE @DataFileNames TABLE (mdfFile nvarchar(4000))
DECLARE @MdfFilePath VARCHAR(4000), @DBName SYSNAME, @CMD VARCHAR(4000)
 
INSERT INTO #DataFiles 
EXEC sys.xp_dirtree @SourceMdfFolderPath,0,1;

DELETE FROM #DataFiles WHERE filepath NOT LIKE '%.mdf' OR isfile = 0;
 
UPDATE #DataFiles SET filepath = @SourceMdfFolderPath + '\' + filepath;

RAISERROR(N'Found %d MDF files in total',0,1,@@ROWCOUNT) WITH NOWAIT;

-- get .mdf files not associatated with any existing database
DECLARE curMdfFiles CURSOR
STATIC LOCAL FORWARD_ONLY
FOR
    SELECT filepath
    FROM #DataFiles
    WHERE filepath != 'null'
    AND filepath NOT IN (SELECT physical_name FROM sys.master_files)

OPEN curMdfFiles;

WHILE 1=1
BEGIN
    FETCH NEXT FROM curMdfFiles INTO @MdfFilePath;
    IF @@FETCH_STATUS <> 0 BREAK;

    TRUNCATE TABLE #DBFiles
    TRUNCATE TABLE #DBProperties
 
    -- return all files associated with the database
    INSERT INTO #DBFiles(status, fileid, name, filename)
    EXEC ('DBCC CHECKPRIMARYFILE(''' + @MdfFilePath + ''', 3) WITH NO_INFOMSGS')
 
    -- get database name
    INSERT INTO #DBProperties (property, value)
    EXEC ('DBCC CHECKPRIMARYFILE(''' + @MdfFilePath + ''', 2) WITH NO_INFOMSGS')
 
    SELECT @DBName = CAST(value AS SYSNAME)
    FROM #DBProperties
    WHERE property = 'Database name'

    IF DB_ID(@DBName) IS NOT NULL
    BEGIN
	RAISERROR(N'**** Cannot attach file "%s" because database "%s" already exists.',0,1,@MdfFilePath,@DBName) WITH NOWAIT;
	CONTINUE; -- skip this database
    END
    ELSE
    BEGIN
	RAISERROR(N'**** Attaching database "%s" from file "%s"...',0,1,@DBName,@MdfFilePath) WITH NOWAIT;
    END

    -- calculate new file paths
    UPDATE #DBFiles
	SET newfilename =
	CASE
		WHEN fileid = 1 THEN @MdfFilePath
		WHEN @NewDestinationDataFolderPath IS NOT NULL AND filename LIKE '%.[mn]df' THEN
			@NewDestinationDataFolderPath + '\' + REVERSE(SUBSTRING(REVERSE(RTRIM(filename)),0,CHARINDEX('\',REVERSE(RTRIM(filename)))))
		WHEN @NewDestinationLogsFolderPath IS NOT NULL AND filename LIKE '%.ldf' THEN
			@NewDestinationLogsFolderPath + '\' + REVERSE(SUBSTRING(REVERSE(RTRIM(filename)),0,CHARINDEX('\',REVERSE(RTRIM(filename)))))
		ELSE LTRIM(RTRIM(filename))
	END

    -- construct file attachment command while checking for the existence of each file
    DECLARE @CurrFileName sysname, @OriginalFilePath varchar(4000), @NewFilePath varchar(4000);
    DECLARE @FileExistenceCheck table (fileExists tinyint, isDirectory tinyint, parentDirectoryExists tinyint);
    DECLARE @HasMissingFiles bit
    SET @CMD = NULL;
    SET @HasMissingFiles = 0;

    DECLARE curFiles CURSOR
    LOCAL STATIC FORWARD_ONLY
    FOR
        SELECT name, filename, newfilename
        FROM #DBFiles

    OPEN curFiles

    WHILE 1=1
    BEGIN
	FETCH NEXT FROM curFiles INTO @CurrFileName, @OriginalFilePath, @NewFilePath;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @CMD = ISNULL(@CMD + ', ', 'CREATE DATABASE ' + QUOTENAME(@DBName) + ' ON ') + '(FILENAME = N''' + @NewFilePath + ''')' + char(13)

	-- check for file existence
	DELETE @FileExistenceCheck;

	INSERT INTO @FileExistenceCheck
	EXEC sys.xp_fileexist @NewFilePath;

	IF EXISTS (SELECT NULL FROM @FileExistenceCheck WHERE fileExists = 0)
	BEGIN
		RAISERROR(N'       File "%s" does not exist.',0,1,@NewFilePath) WITH NOWAIT;
		SET @HasMissingFiles = 1;
	END

    END

    CLOSE curFiles;
    DEALLOCATE curFiles;

    IF @HasMissingFiles = 1
    BEGIN
	RAISERROR(N'!!!! Cannot attach database "%s" due to missing file(s).',0,1,@DBName);
	CONTINUE; -- skip this database
    END

    SET @CMD = @CMD + ' FOR ATTACH'
 
    IF @WhatIf = 1
    BEGIN
	PRINT @CMD
    END
    ELSE
    BEGIN
	EXEC (@CMD)
    END
END
 
CLOSE curMdfFiles
DEALLOCATE curMdfFiles