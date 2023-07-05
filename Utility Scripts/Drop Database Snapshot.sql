USE MyDB
GO

/********************************************************************************************************************************
 Title:		A procedure to Drop a specified Database Snapshot or all snapshots if not specified
 Author:	Reut Almog Talmi @Madeira Data Solutions
 Notes:		@SourceDBName is the source database name, meaning the origin database which the database snapshot was created from. 
			NOT the snapshot database name that you wish to drop.
			Leave as NULL if you desire to drop ALL database snapshots that currently exist on the instance.
********************************************************************************************************************************/
CREATE OR ALTER PROCEDURE sp_DBA_DropDatabaseSnapshot
	@SourceDBName SYSNAME = NULL,
	@Debug BIT = 1

AS

SET NOCOUNT ON


DECLARE
	@CMD NVARCHAR(1000),
	@SnapshotDatabaseName SYSNAME


IF @SourceDBName IS NOT NULL 
BEGIN
	-- validate source DB name 
	IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @SourceDBName) 
	BEGIN
		RAISERROR('Database: %s doesn''t exist',16,1,@SourceDBName)
	END

	-- validate snapshot database existance for source DB name
	IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE source_database_id = DB_ID(@SourceDBName))
	BEGIN
		RAISERROR ('A Database snapshot does not exists for the source database: %s',16,1,@SourceDBName)
	END
END



-- Drop all snapshot databases for a specified source DB or all snapshots in the SQL instance if no source DB has been specified
BEGIN
	DECLARE DropSnapshot CURSOR READ_ONLY FOR 
		SELECT name AS SnapshotDatabaseName
		FROM sys.databases 
		WHERE 1 = 1 
		AND source_database_id IS NOT NULL
		AND COALESCE( DB_ID(@SourceDBName), source_database_id) = source_database_id


	OPEN DropSnapshot

	FETCH NEXT FROM DropSnapshot INTO @SnapshotDatabaseName
	WHILE @@FETCH_STATUS = 0
		BEGIN
		
			SELECT @CMD =	N'DROP DATABASE ' + QUOTENAME(@SnapshotDatabaseName)+ N';'			+CHAR(10)	

			PRINT @CMD
			IF @Debug != 1
			BEGIN
				EXEC sp_executesql @CMD

				RAISERROR ('Database [%s] has been dropped',1,1,@SnapshotDatabaseName)
			END

			FETCH NEXT FROM DropSnapshot INTO @SnapshotDatabaseName
		END



	CLOSE DropSnapshot
	DEALLOCATE DropSnapshot


END



GO


