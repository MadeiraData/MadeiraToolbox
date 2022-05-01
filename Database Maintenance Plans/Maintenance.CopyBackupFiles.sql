/*========================================================================================================================
Description:	Manage copy backup files to secondary backup location following backup process.
Author:			Reut Almog Talmi
Created:		2022-05-01
Last Updated:	2022-05-01
Notes:			Use this table and procedure to manage a copy backup files solution executed from within SQL server.
				This solutiuon is based on system stored procedure master.sys.xp_copy_file introduced in SQL server 2017 CU(18)
				Supports Windows and Linux OS platforms
=========================================================================================================================*/


USE MyDB				/*  Specify the database in which the objects will be created.	*/

SET NOCOUNT ON


DECLARE
	@DestinationRootDirectory NVARCHAR(MAX) = '/s3disk/'        /*	Specify the Destination root directory where backup files will be copied to. 
																	cannot be NULL	*/

IF @DestinationRootDirectory IS NULL
	RAISERROR('@DestinationRootDirectory Must Be Specified', 16, 1)

IF @DestinationRootDirectory IS NOT NULL AND RIGHT(@DestinationRootDirectory,1) NOT IN ('/' ,'\')
	SET @DestinationRootDirectory = @DestinationRootDirectory + CAST(SERVERPROPERTY('PathSeparator') AS NVARCHAR(2))


--Create a management table 
CREATE TABLE DBA_CopyFiles (
backup_set_id INT NOT NULL,
DatabaseName SYSNAME NULL,
BackupType	CHAR(1) NULL,
BackupStartDate DATETIME NULL,
BackupEndDate DATETIME NULL,
SourceFilePath NVARCHAR(1000) NULL,
DestinationFilePath NVARCHAR(1000) NULL,
Is_Copied BIT CONSTRAINT C_CopyFilesToS3_Is_Copied DEFAULT (0),
CopyStartTime DATETIME NULL, 
CopyEndTime DATETIME NULL,
CopyDurationInSec AS (DATEDIFF(SECOND, BackupStartDate, BackupEndDate)),
ErrorNumber INT NULL, 
ErrorMessage NVARCHAR(MAX) NULL
)
GO
--Create a Stored procedure that manages the copy files process
CREATE OR ALTER PROCEDURE sp_DBA_CopyBackupFiles

	@DatabaseName			NVARCHAR(1000) = NULL,		/*Accepts values of one or more databases with ',' separator or NULL */
	@BackupType				NVARCHAR(100) = NULL,		/*Accepts values: full ; diff ; log ; 'full','diff' ; 'full','log' ; 'diff','log'; 'full','diff','log' ; NULL */
	@MinDateToCopy			DATETIME = NULL,
	@DestinationRootFolder	NVARCHAR(100) = @DestinationRootDirectory,
	@Debug					BIT = 0

AS

BEGIN

  SET NOCOUNT ON;  

	DECLARE 
		@Delimiter				NVARCHAR(2),
		@SourceFilePath			NVARCHAR(1000), 
		@DestinationFilePath	NVARCHAR(1000),
		@backup_set_id			INT,
		@Type					NVARCHAR(5),
		@Message				NVARCHAR(4000),
		@CopyCommand			NVARCHAR(MAX),
		@SourceFile_Exists		INT = 0,
		@DestFile_Exists		INT = 0


	SET @Delimiter = CAST(SERVERPROPERTY('PathSeparator') AS NVARCHAR(2))


	-- check if @BackupType is passed and set @type variable accordingly
	IF @BackupType IS NOT NULL 
	BEGIN
		IF LOWER(@BackupType) LIKE '%full%'
			SET @Type = REPLACE(@BackupType, 'full', 'D')
		ELSE IF LOWER(@BackupType) LIKE '%diff%'
			SET @Type = REPLACE(@BackupType, 'diff', 'I')
		ELSE IF LOWER(@BackupType) LIKE '%log%'
			SET @Type = REPLACE(@BackupType, 'log', 'L')
		ELSE
		BEGIN
			SET @Message =	N'Invalid value passed to @BackupType parameter.'	+CHAR(10)+
							'@BackupType Accepts the following values:'			+CHAR(10)+
							'''full'''											+CHAR(10)+
							'''diff'''											+CHAR(10)+
							'''log'''											+CHAR(10)+
							'''full'',''diff'''									+CHAR(10)+
							'''full'',''log'''									+CHAR(10)+
							'''diff'',''log'''									+CHAR(10)+
							'''full'',''diff'''									+CHAR(10)+
							'''log'''											+CHAR(10)+ 
							'NULL'
			RAISERROR(@Message, 16,1)
		END
	END
	


	--set default value for @MinDateToCopy if it was not passed 
	IF @MinDateToCopy IS NULL
		SET @MinDateToCopy = DATEADD(DAY,-3,GETDATE())


	--get databases list
	DROP TABLE IF EXISTS #Databases
	CREATE TABLE #Databases (name SYSNAME)

	IF @DatabaseName IS NULL
	BEGIN
		INSERT INTO #Databases SELECT DISTINCT database_name AS name FROM msdb.dbo.backupset
	END
	ELSE
	BEGIN
		INSERT INTO #Databases SELECT value AS name FROM STRING_SPLIT(REPLACE(@DatabaseName, ' ','') , ',') 
	END


	--get types list
	DROP TABLE IF EXISTS #Types
	CREATE TABLE #Types (type SYSNAME)

	IF @BackupType IS NULL
	BEGIN
		INSERT INTO #Types SELECT DISTINCT type FROM msdb.dbo.backupset
	END
	ELSE
	BEGIN
		INSERT INTO #Types SELECT value AS type FROM STRING_SPLIT(REPLACE(@Type, ' ','') , ',') 
	END




	--insert only new records into DBA_CopyFiles based on backup_set_id
	MERGE DBA_CopyFiles AS TARGET
	USING	(
			SELECT
				bs.backup_set_id,
				bs.database_name AS DatabaseName,
				bs.type AS BackupType,
				bs.backup_start_date AS BackupStartDate,
				bs.backup_finish_date AS BackupEndDate,
				bmf.physical_device_name AS SourceFilePath,
				CONCAT(@DestinationRootFolder, SUBSTRING(bmf.physical_device_name, CHARINDEX( @Delimiter, SUBSTRING(bmf.physical_device_name, 2, LEN(bmf.physical_device_name) ))+2, LEN(bmf.physical_device_name))) AS DestinationFilePath
			FROM
				msdb.dbo.backupset bs
			INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id	
			WHERE 1 = 1
			AND bs.type IN (SELECT type FROM #Types)
			AND bs.database_name IN (SELECT name FROM #databases)
			AND bs.server_name = @@SERVERNAME
			AND bs.backup_start_date >= @MinDateToCopy
			) AS SOURCE 
	ON (SOURCE.backup_set_id = TARGET.backup_set_id ) 	
	
	WHEN MATCHED THEN UPDATE SET 
	TARGET.DestinationFilePath	= SOURCE.DestinationFilePath

	WHEN NOT MATCHED BY TARGET THEN 
	INSERT ([backup_set_id], [DatabaseName], [BackupType], [BackupStartDate], [BackupEndDate], [SourceFilePath], [DestinationFilePath]) 
	VALUES (SOURCE.backup_set_id, SOURCE.DatabaseName, SOURCE.BackupType, SOURCE.BackupStartDate, SOURCE.BackupEndDate, SOURCE.SourceFilePath, SOURCE.DestinationFilePath);
	


	--retrieve the list of files that are going to be copied
	IF @Debug = 1
	BEGIN
		SELECT 
			cf.backup_set_id, 
			cf.SourceFilePath, 
			cf.DestinationFilePath, 
			cf.Is_Copied,
			CONVERT(decimal(10, 2), bs.compressed_backup_size/1024./1024.) AS CompressedBackupSize_MB
		FROM DBA_CopyFiles cf
		INNER JOIN msdb.dbo.backupset bs ON cf.backup_set_id = bs.backup_set_id
		WHERE 1 = 1
		AND BackupStartDate >= @MinDateToCopy
		AND BackupType IN (SELECT type FROM #Types)
		AND DatabaseName IN (SELECT name FROM #databases)
		AND Is_Copied = 0
		ORDER BY cf.backup_set_id DESC
	END


	-- Copy each file from source to destination
	DECLARE CopyFiles CURSOR READ_ONLY FOR 
		SELECT backup_set_id, SourceFilePath, DestinationFilePath
		FROM DBA_CopyFiles
		WHERE 1 = 1
		AND BackupStartDate >= @MinDateToCopy
		AND BackupType IN (SELECT type FROM #Types)
		AND DatabaseName IN (SELECT name FROM #databases)
		ORDER BY backup_set_id DESC

	OPEN CopyFiles

	FETCH NEXT FROM CopyFiles INTO @backup_set_id, @SourceFilePath, @DestinationFilePath
	WHILE @@FETCH_STATUS = 0	   
	BEGIN

		--check for file existence in destination - in case it has been copied manualy or by any other method
		EXEC master.dbo.xp_fileexist @DestinationFilePath, @DestFile_Exists OUTPUT
			
		IF @DestFile_Exists = 1
		BEGIN
			UPDATE DBA_CopyFiles
			SET Is_Copied = 1
			WHERE backup_set_id = @backup_set_id
		END		
		ELSE
				
		BEGIN TRY
			--check for source file existence
			EXEC master.dbo.xp_fileexist @SourceFilePath, @SourceFile_Exists OUTPUT
				
			IF @SourceFile_Exists = 0
			BEGIN
				SET @Message = N'File '+ @SourceFilePath + ' Does Not exists in source folder therefore cannot be copied'
				RAISERROR(@Message, 16,1) WITH SETERROR 
			END
			ELSE	--File exists in source 
			BEGIN
				SELECT @CopyCommand =	N'EXEC master.sys.xp_copy_file ''' + @SourceFilePath + ''', ''' + @DestinationFilePath + ''';'	
				PRINT @CopyCommand	

				IF @Debug = 0 
				BEGIN

					UPDATE DBA_CopyFiles
					SET CopyStartTime = GETDATE()
					WHERE backup_set_id = @backup_set_id

					EXEC sp_executesql @CopyCommand 
					
					--verify file existence in destination
					EXEC master.dbo.xp_fileexist @DestinationFilePath, @DestFile_Exists OUTPUT

					IF @DestFile_Exists = 1
					BEGIN
						UPDATE DBA_CopyFiles
						SET Is_Copied = 1,
						CopyEndTime = GETDATE()
						WHERE backup_set_id = @backup_set_id
					END
					ELSE
					BEGIN
						SET @Message = N'File '+ @DestinationFilePath + ' Failed to be copied into destination folder'
						RAISERROR(@Message, 16,1) WITH SETERROR 
							
						UPDATE DBA_CopyFiles
						SET Is_Copied = 0,
						CopyEndTime = NULL
						WHERE backup_set_id = @backup_set_id
					END
				END
			END
		END TRY
		BEGIN CATCH

			UPDATE DBA_CopyFiles
			SET ErrorNumber = ERROR_NUMBER(),
			ErrorMessage = ERROR_MESSAGE() 
			WHERE backup_set_id = @backup_set_id

		END CATCH
		

		FETCH NEXT FROM CopyFiles INTO @backup_set_id, @SourceFilePath, @DestinationFilePath

	END

		


	CLOSE CopyFiles
	DEALLOCATE CopyFiles



END



GO



