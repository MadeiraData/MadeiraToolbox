USE MyDB
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*******************************************************************************************************************
 Title:		A procedure to create Database Snapshot
 Author:	Reut Almog Talmi @Madeira
********************************************************************************************************************/
CREATE OR ALTER PROCEDURE [dbo].[sp_DBA_CreateDatabaseSnapshot]
	@SourceDBName		SYSNAME,
	@Suffix				NVARCHAR(20) = NULL,
	@DestFilePath		NVARCHAR(200) = NULL, -- Edit when you want the destination snapshot file to reside somewhere(Example: 'Z:\Path\')
	@IgnoreReplicaRole	BIT = 0,			/*By default, the snapshot will be created on the secondary replica only. 
											in case desired to create on primary - set @IgnoreReplicaRole to 1
											For databases not involved in AG -  @IgnoreReplicaRole can be 0 or 1 or NULL*/
	@Debug BIT = 1

AS

SET NOCOUNT ON


DECLARE  	
	@FilePath NVARCHAR(3000) = '',
	@SQLCommand NVARCHAR(4000) = '',
	@SnapshotDatabaseName SYSNAME
	

IF DB_ID(@SourceDBName) IS NULL OR NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @SourceDBName) 
BEGIN
	RAISERROR('Database %s doesn''t exist',16,1,@SourceDBName)
END


-- Set default suffix with timestamp if it is not defined
IF @Suffix IS NULL OR TRIM(@Suffix) = ''
BEGIN
	SELECT @Suffix = REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(20), GETDATE(), 120), ':', ''), '-','_'), ' ', '_')
END


-- Set the file path location of the snapshot data files.

IF TRIM(@DestFilePath ) = ''
BEGIN
	SET @DestFilePath = NULL
END
ELSE
BEGIN
	SET @DestFilePath += @SourceDBName
END


-- build list of files for the database snapshot.
IF ISNULL((SELECT sys.fn_hadr_is_primary_replica (@SourceDBName)), 0) = 0 OR  @IgnoreReplicaRole = 1
BEGIN

	SELECT @FilePath += N'' + mf.name + ', FILENAME = ''' + ISNULL(@DestFilePath, LEFT(mf.physical_name,LEN(mf.physical_name)- 4 ) ) + '_' + @Suffix + '.ss'')'
	FROM sys.master_files AS mf
	INNER JOIN sys.databases AS db ON db.database_id = mf.database_id
	WHERE db.state = 0
	AND mf.type = 0 -- Only include data files
	AND db.name = @SourceDBName




	SET @SQLCommand += 
	N'CREATE DATABASE ' + QUOTENAME(@SourceDBName + '_' + @Suffix ) + CHAR(10)+ 
	' ON ( NAME = ' + @FilePath + ' AS SNAPSHOT OF '+ QUOTENAME(@SourceDBName) + ';' + CHAR(30)

END


PRINT @SQLCommand

IF @Debug != 1
BEGIN
	EXEC sp_executesql @SQLCommand
	
	SELECT TOP 1 @SnapshotDatabaseName = name 
	FROM sys.databases 
	WHERE  source_database_id = DB_ID(@SourceDBName) 
	ORDER BY create_date DESC

	RAISERROR ('Database [%s] has been created',1,1,@SnapshotDatabaseName)
END
GO


