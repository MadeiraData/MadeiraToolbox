/*========================================================================================================================
Description:	Manage copy process of backup files, or any other file defined by file extention, to destination location.
Author:			Reut Almog Talmi @ Madeira Data Solutions
Created:		2022-05-01
Last Updated:	2023-02-22
Notes:			Use the following table and procedures to manage a copy backup files solution executed from within SQL server.
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
CREATE TABLE dbo.DBA_CopyFilesLog (
	id					BIGINT IDENTITY(1,1)	NOT NULL,
	SourceFilePath		NVARCHAR(1000)			NULL,
	DestinationFilePath NVARCHAR(1000)			NULL,
	Is_Copied			BIT						NULL CONSTRAINT C_CopyFilesLog_Is_Copied DEFAULT (0),
	CopyStartTime		DATETIME				NULL,
	CopyEndTime			DATETIME				NULL,
	CopyDurationInSec	AS (DATEDIFF(SECOND,CopyStartTime,CopyEndTime)),
	ErrorNumber			INT						NULL,
	ErrorMessage		NVARCHAR(MAX)			NULL
)
GO


ALTER TABLE DBA_CopyFiles 
ADD CONSTRAINT PK_DBA_CopyFilesLog_id PRIMARY KEY CLUSTERED (id);





--Adding custom errors to sys.messages used by sp_DBA_CopyFiles
EXEC sp_addmessage 
    @msgnum = 50100, 
    @severity = 1, 
    @msgtext = 'File %s already exists in destination folder',
	@replace = replace;

EXEC sp_addmessage 
    @msgnum = 50200, 
    @severity = 15, 
    @msgtext = 'File failed to be copied to destination folder';

EXEC sp_addmessage 
    @msgnum = 50300, 
    @severity = 15, 
    @msgtext = 'File Does Not exists in source folder therefore cannot be copied';


SELECT *
FROM sys.messages
WHERE message_id >= 50000



--Create the Stored procedure to manages the copy files process
/*******************************************************************
example:
	exec [dbo].[sp_DBA_CopyFiles] 
	@SourceFolder = '/sql_backup/', 
	@DestinationFolder = '/s3disk/',
	@FileExtention = 'log',
	@NoOfDays = 8,
	@Debug = 1
********************************************************************/


CREATE OR ALTER PROCEDURE [dbo].[sp_DBA_CopyFiles]
	@SourceFolder		NVARCHAR(1000),
	@DestinationFolder	NVARCHAR(1000),
	@FileExtention		NVARCHAR(10),
	@PrefixFileName		NVARCHAR(1000) = NULL,
	@NoOfDays			SMALLINT,
	@Debug				BIT = 0,
	@GenerateDestFolder BIT = 0
AS

SET NOCOUNT ON

DECLARE 
	@FileName				NVARCHAR(1000),
	@DestinationFileName	NVARCHAR(1000),
	@SearchPattern			NVARCHAR(1024),
	@CutOffDateTime			DATETIME2,
	@CopyCommand			NVARCHAR(MAX),
	@MaxLevel				SMALLINT,
	@CuurentLevel			SMALLINT = 0,
	@NumberOfRows			INT,
	@RowCounter				INT = 1,
	@Id						BIGINT,
	@DestFile_Exists		INT = 0,
	@SourceFile_Exists		INT = 0,
	@Message				NVARCHAR(1000),
	@CopyStartTime			DATETIME,
	@DestDirStructure		NVARCHAR(1000)


-- set @CutOffDateTime in UTC date time and set Min Date according to @NoOfDays parameter
SET @CutOffDateTime = DATEADD(hh, DATEDIFF(hh, GETDATE(), GETUTCDATE()), DATEADD(DAY, -@NoOfDays , GETDATE()))

--normalize directory path 
IF RIGHT(TRIM(@SourceFolder) ,1) NOT IN ('/' ,'\')
	SET @SourceFolder = @SourceFolder  + CAST(SERVERPROPERTY('PathSeparator') AS NVARCHAR(2))

IF RIGHT(TRIM(@DestinationFolder) ,1) NOT IN ('/' ,'\')
	SET @DestinationFolder = @DestinationFolder  + CAST(SERVERPROPERTY('PathSeparator') AS NVARCHAR(2))


-- add '.' character to file extention if it has not specified
IF CHARINDEX('.', @FileExtention) = 0
BEGIN
	IF @Debug = 1 
	BEGIN
		PRINT 'Adding ''.'' character to file extention'
	END

	SET @FileExtention = '.' + TRIM(@FileExtention)
END


-- set SearchPattern 
SET @SearchPattern = CONCAT(ISNULL(@PrefixFileName, ''), '*', @FileExtention)

IF @Debug = 1 
	SELECT @SearchPattern AS SearchPattern



--Get directory structure
SELECT @MaxLevel = MAX(level) FROM sys.dm_os_enumerate_filesystem (@SourceFolder, '*') WHERE is_directory = 1

DROP TABLE IF EXISTS #DirStructure 
CREATE TABLE #DirStructure (level SMALLINT, DirectoryPath NVARCHAR(256))

WHILE @CuurentLevel <= @MaxLevel
BEGIN

	INSERT INTO #DirStructure  (level,  DirectoryPath)
 	SELECT level, full_filesystem_path AS DirectoryPath
	FROM sys.dm_os_enumerate_filesystem (@SourceFolder, '*')
	WHERE 1 = 1
	AND is_directory = 1
	AND level = @CuurentLevel
	
	SET @CuurentLevel = @CuurentLevel + 1

END


UPDATE #DirStructure 
SET DirectoryPath = CONCAT(DirectoryPath, CAST(SERVERPROPERTY('PathSeparator') AS NVARCHAR(2)))
WHERE RIGHT(DirectoryPath, 1) NOT IN ('/' ,'\')



IF @Debug = 1 
	SELECT * FROM #DirStructure ORDER BY DirectoryPath ASC


--Generate destination structure Path
IF @GenerateDestFolder = 1 
BEGIN
	SELECT
		ROW_NUMBER() OVER (ORDER BY DirectoryPath) AS RN,
		DirectoryPath,
		level,
		REPLACE(DirectoryPath, @SourceFolder, @DestinationFolder) AS DestinationDriStructure
		INTO #DstDirStructure
	FROM
		#DirStructure


	DECLARE @maxRN INT = (SELECT MAX(RN) FROM #DstDirStructure)
	DECLARE @i INT = 1

	WHILE @maxRN >= @i	
	BEGIN	
		SELECT @DestDirStructure =  DestinationDriStructure FROM #DstDirStructure WHERE RN = @i
		IF @Debug = 1 
		BEGIN
			PRINT 'xp_create_subdir ' + @DestDirStructure + char(10)
		END
		
		EXEC   xp_create_subdir @DestDirStructure	
		SET @i = @i + 1
	END	
END


DROP TABLE IF EXISTS #FileList
CREATE TABLE #FileList (
	Id					INT IDENTITY NOT NULL,
    SourceFileName		NVARCHAR(255) NOT NULL, 
	DestinationFileName NVARCHAR(2000),
	IsCopied			BIT
);


-- Get list of files 
INSERT INTO #FileList (SourceFileName, DestinationFileName, IsCopied)
SELECT 
full_filesystem_path AS SourceFileName,
REPLACE(full_filesystem_path, @SourceFolder, @DestinationFolder) AS DestinationFileName,
0 AS IsCopied
FROM sys.dm_os_enumerate_filesystem (@SourceFolder, @SearchPattern) ef
LEFT JOIN #DirStructure ds ON ef.parent_directory = ds.DirectoryPath
WHERE ef.is_directory = 0
AND ef.last_write_time >= @CutOffDateTime
ORDER BY ef.parent_directory ASC



IF @Debug = 1
    BEGIN
	    SELECT SourceFileName FROM #FileList ORDER BY Id ASC
    END;

IF NOT EXISTS (SELECT 1 FROM #FileList)
BEGIN
	PRINT N'No files with the specified file extention and between the defined dates were returned in path ' + @SourceFolder;
	RETURN;
END;


SET @NumberOfRows = (SELECT COUNT(Id) FROM #FileList)

WHILE @RowCounter <= @NumberOfRows
BEGIN
	
	SELECT 
	@FileName = SourceFileName,
	@DestinationFileName = DestinationFileName 
	FROM #FileList
	WHERE Id = @RowCounter
	ORDER BY Id ASC

	--check for file existence in destination - in case it has been copied manualy or by any other method
	EXEC master.dbo.xp_fileexist @DestinationFileName, @DestFile_Exists OUTPUT
			
	IF @DestFile_Exists = 1
	BEGIN
		
		SET @Message =  CONCAT(N'File: ', @DestinationFileName ,' Already exists in Destination folder') + CHAR(10) + CHAR(13)
		
		IF @Debug = 1
		BEGIN
			RAISERROR (50100,-1,-1, @DestinationFileName) WITH SETERROR
		END

		IF @Debug = 0
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM [dbo].[DBA_CopyFilesLog] WHERE SourceFilePath = @FileName)
			BEGIN
				INSERT INTO [dbo].[DBA_CopyFilesLog]
				SELECT 
					SourceFilePath = @FileName,
					DestinationFilePath = @DestinationFileName,
					Is_Copied = 1,
					CopyStartTime = GETDATE(),
					CopyEndTime = NULL,
					ErrorNumber = 50100,
					ErrorMessage = @Message 
			END
		END
	END

	ELSE	-- File does not exists in destination folder. start copy
		
	BEGIN TRY
		
		IF @Debug = 0
		BEGIN
			INSERT INTO [dbo].[DBA_CopyFilesLog]
			SELECT 
				SourceFilePath = @FileName,
				DestinationFilePath = @DestinationFileName,
				Is_Copied = 0,
				CopyStartTime = NULL,
				CopyEndTime = NULL,
				ErrorNumber = NULL,
				ErrorMessage = NULL
			
			SELECT @Id = SCOPE_IDENTITY()
			
		END

		--check for source file existence
		EXEC master.dbo.xp_fileexist @FileName, @SourceFile_Exists OUTPUT
				
		IF @SourceFile_Exists = 0
		BEGIN
			RAISERROR(50300,15,1)
		END
		ELSE	--File exists in source 
		BEGIN
					
			SELECT @CopyCommand =	N'EXEC master.sys.xp_copy_file ''' + @FileName + ''', ''' + @DestinationFileName + ''';'	
			
			PRINT N'Beginig copy file: '+ @FileName 
			
			IF @Debug = 1
			BEGIN
				PRINT @CopyCommand	+ CHAR(10)
			END
			
			IF @Debug = 0		
			BEGIN
				SELECT @CopyStartTime = GETDATE()

				EXEC sp_executesql @CopyCommand 
					
				--verify file existence in destination
				EXEC master.dbo.xp_fileexist @DestinationFileName, @DestFile_Exists OUTPUT

				IF @DestFile_Exists = 1
				BEGIN
					UPDATE [DBA_CopyFilesLog]
					SET Is_Copied = 1,
					CopyStartTime = @CopyStartTime,
					CopyEndTime = GETDATE(),
					ErrorNumber = NULL,
					ErrorMessage = NULL
					WHERE Id = @Id

					PRINT N'File: ' + @FileName + ' Copied successfully to destination folder' + CHAR(10)
				END
				ELSE	-- file does not exists in Destination folder
				BEGIN
									
					UPDATE [DBA_CopyFilesLog]
					SET Is_Copied = 0,
					CopyStartTime = @CopyStartTime
					WHERE Id = @Id

					RAISERROR(50200,15,1) 
											
				END
			END
		END
	END TRY
	BEGIN CATCH
		
		PRINT N'Catch block: updating DBA_CopyFilesLog table. Id = ' + @Id + CHAR(10)

		UPDATE [DBA_CopyFilesLog]
		SET ErrorNumber = ERROR_NUMBER(),
		ErrorMessage = ERROR_MESSAGE() 
		WHERE Id = @Id

	END CATCH

	
	SET @RowCounter  = @RowCounter + 1

END



GO




-- Create a Stored procedure to clean CopyFilesLog table 
CREATE OR ALTER PROCEDURE [dbo].[sp_DBA_Clean_CopyFilesLog]
@DaysToKeep SMALLINT,
@ThresholdRecords SMALLINT = NULL

AS

SET NOCOUNT ON;

IF @DaysToKeep IS NULL
BEGIN
	RAISERROR ('Must declare number of days to keep in table DBA_CopyFilesLog', 16,1)
	RETURN
END


IF NOT EXISTS (	SELECT TOP 1 1 
			FROM DBA_CopyFilesLog 
			WHERE CopyStartTime <= DATEADD(DAY,-(@DaysToKeep),GETDATE())
			ORDER BY Id ASC)
BEGIN
	RAISERROR ('No records to delete', 1,1)	
	RETURN
END
ELSE
BEGIN

	DECLARE 
		@Rows INT,
		@CutOffDate DATETIME

	IF @ThresholdRecords IS NULL
	BEGIN
		SET @ThresholdRecords = 100
	END
		
	SET @Rows = (SELECT COUNT(*) FROM DBA..DBA_CopyFilesLog)
		
	SELECT @CutOffDate = DATEADD(DAY,-@DaysToKeep,GETDATE())
		
	WHILE @Rows > 0
	BEGIN
		DELETE TOP (@ThresholdRecords)
		FROM DBA..DBA_CopyFilesLog
		WHERE CopyStartTime <= @CutOffDate

		SET @Rows = @@ROWCOUNT;
		
	END
END


GO


