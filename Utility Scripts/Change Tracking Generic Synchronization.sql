IF OBJECT_ID('tempdb..#GetChangeTrackingDelta') IS NOT NULL DROP PROCEDURE #GetChangeTrackingDelta
GO
/*
===========================
 Get Change Tracking Delta
===========================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2022-06-22
Description:
	This procedure gets as a parameter the name of a table and a database, and a last change-tracking version number,
	and constructs and executes the necessary SQL command to retrieve all data changed since the specified version number.

Arguments:
	@TableName nvarchar(1000)		-- The name of the source table (both schema and table name)
	@DBName sysname			-- The name of the database containing @TableName. If NULL will use current database context.
	@CTLastPulledVersion bigint	-- The Change-Tracking version number last sychronized, and the baseline for the delta changes. Default 0.
	@VersionNotValidSeverity int	-- If @CTLastPulledVersion is older than the minimum available change-tracking version,
						this means this delta is not valid and may have missing data.
						If this happens, an error will be raised, and this parameter determines its severity.
						Default is 16, which will also stop the execution of this procedure.
	@Debug bit			-- If set to 1, will output an additional resultset with intermediate data for debugging.
	@OutputCommand nvarchar(max)	-- Optional output variable, returns the SQL command implementing the change tracking delta query.

Result:
	The result will be in the following structure:
	SYS_CHANGE_OPERATION (I/U/D = Insert/Update/Delete)
	SYS_CHANGE_VERSION (the Change-Tracking version number responsible for this operation)
	SYS_CHANGE_TRACKING_CURRENT_VERSION (the current change tracking version. same for all records. use this for next execution with the @CTLastPulledVersion parameter)
	<... Primary Key Columns ...>
	<... all other source table columns ...>
*/
CREATE PROCEDURE #GetChangeTrackingDelta
	 @TableName nvarchar(1000)
	,@DBName sysname = NULL
	,@CTLastPulledVersion bigint = 0
	,@VersionNotValidSeverity int = 16
	,@Debug bit = 0
	,@OutputCommand nvarchar(MAX) = NULL OUTPUT
AS
BEGIN
	SET NOCOUNT, XACT_ABORT ON;

	DECLARE @SpExecuteSql nvarchar(1000), @TableObjId int
	DECLARE @PKColumns nvarchar(MAX), @PKJoinExpr nvarchar(MAX), @DataColumns nvarchar(MAX)
	DECLARE @CTMinValidVersion bigint, @CTCurrentVersion bigint

	SET @DBName = ISNULL(@DBName, DB_NAME());

	IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID(@DBName))
	BEGIN
		RAISERROR(N'Specified database "%s" is not enabled for change tracking', 16, 1, @DBName);
		RETURN -1;
	END

	SET @SpExecuteSql = QUOTENAME(@DBName) + N'..sp_executesql'

	EXEC @SpExecuteSql N'
	select @TableObjId = object_id, @CTMinValidVersion = CHANGE_TRACKING_MIN_VALID_VERSION(object_id)
	, @TableName = QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N''.'' + QUOTENAME(OBJECT_NAME(object_id))
	from sys.change_tracking_tables
	where object_id = OBJECT_ID(@TableName)
	'
		, N'@TableName nvarchar(1000) OUTPUT, @TableObjId int OUTPUT, @CTMinValidVersion bigint OUTPUT'
		, @TableName OUTPUT, @TableObjId OUTPUT, @CTMinValidVersion OUTPUT;

	IF @TableObjId IS NULL
	BEGIN
		RAISERROR(N'Change tracking is not enabled for table: "%s" in database "%s"', 16,1,@TableName,@DBName);
		RETURN -2;
	END

	IF @CTMinValidVersion > @CTLastPulledVersion
	BEGIN
		RAISERROR(N'Last pulled version requested (%I64d) is older than the minimum valid version (%I64d). Table "%s" in database "%s" must be fully re-synchronized.'
			, @VersionNotValidSeverity,1,@CTLastPulledVersion,@CTMinValidVersion,@TableName,@DBName);
		IF @VersionNotValidSeverity > 15 RETURN -3;
	END

	EXEC @SpExecuteSql N'
	SELECT
	  @PKColumns = CASE WHEN ixc.column_id = c.column_id THEN ISNULL(@PKColumns + N'', '', N'''') + N''ct.'' + QUOTENAME(c.name) ELSE @PKColumns END
	, @PKJoinExpr = CASE WHEN ixc.column_id = c.column_id THEN ISNULL(@PKJoinExpr + CHAR(10) + N''AND '', N'''') + N''src.'' + QUOTENAME(c.name) + N'' = ct.'' + QUOTENAME(c.name) ELSE @PKJoinExpr END
	, @DataColumns = CASE WHEN ixc.column_id = c.column_id THEN @DataColumns ELSE ISNULL(@DataColumns, N'''') + N'', src.'' + QUOTENAME(c.name) END
	FROM sys.columns AS c 
	LEFT JOIN sys.indexes AS ix ON ix.object_id = c.object_id AND ix.is_primary_key = 1
	LEFT JOIN sys.index_columns AS ixc ON ix.object_id = ixc.object_id AND ix.index_id = ixc.index_id AND ixc.column_id = c.column_id
	WHERE c.object_id = @TableObjId
	AND c.is_computed = 0

	SET @CTCurrentVersion = CHANGE_TRACKING_CURRENT_VERSION();'
		, N'@TableObjId int, @PKColumns nvarchar(max) OUTPUT, @PKJoinExpr nvarchar(max) OUTPUT, @DataColumns nvarchar(max) OUTPUT, @CTCurrentVersion bigint OUTPUT'
		, @TableObjId, @PKColumns OUTPUT, @PKJoinExpr OUTPUT, @DataColumns OUTPUT, @CTCurrentVersion OUTPUT;

	SET @OutputCommand = N'SELECT ct.SYS_CHANGE_OPERATION, ct.SYS_CHANGE_VERSION
	, ' + CONVERT(nvarchar(max), @CTCurrentVersion) + N' AS SYS_CHANGE_TRACKING_CURRENT_VERSION /* @CTCurrentVersion */
	, ' + @PKColumns + N'
	' + ISNULL(@DataColumns, N'') + N'
	FROM ' + @TableName + N' AS src
	RIGHT JOIN CHANGETABLE (CHANGES ' + @TableName + N', ' + CONVERT(nvarchar(max), @CTLastPulledVersion) + N') AS ct /* @CTLastPulledVersion */
	ON ' + @PKJoinExpr + N'
	WHERE ct.SYS_CHANGE_VERSION <= @CTCurrentVersion'

	IF @Debug = 1
	SELECT @DBName AS DBName, @TableName AS TableName, @CTLastPulledVersion AS LastPulledVersion
	, @TableObjId AS TableObjId, @CTCurrentVersion AS CTCurrentVersion, @CTMinValidVersion AS CTMinValidVersion
	, @OutputCommand AS OutputCommand, @PKJoinExpr AS PKJoinExpr, @PKColumns AS PKColumns

	PRINT @OutputCommand
	EXEC @SpExecuteSql @OutputCommand, N' @CTLastPulledVersion bigint, @CTCurrentVersion bigint',  @CTLastPulledVersion, @CTCurrentVersion;

	SET @CTLastPulledVersion = @CTCurrentVersion
END
GO

-- Example execution (randomly picks one database and one table with CT enabled)
-- You can also replace the NULLs with actual values
/*
-- get all tables with change tracking in current database:

SELECT *
, DBName = DB_NAME()
, TableName = QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N'.' + QUOTENAME(OBJECT_NAME(object_id))
FROM sys.change_tracking_tables

*/

DECLARE
	  @DBName sysname		= NULL
	, @TableName nvarchar(1000)	= NULL
	, @CTLastPulledVersion bigint	= NULL

SELECT TOP (1) @DBName = DB_NAME(database_id)
FROM sys.change_tracking_databases
WHERE @DBName IS NULL

IF @DBName IS NULL
BEGIN
	RAISERROR(N'No databases found with change tracking enabled on server "%s"',16,1,@@SERVERNAME);
END
ELSE
BEGIN
	RAISERROR(N'Found database with change tracking enabled: %s',0,1,@DBName) WITH NOWAIT;

	DECLARE @SpExecuteSql nvarchar(1000), @OutputCommand nvarchar(MAX)
	SET @SpExecuteSql = QUOTENAME(@DBName) + N'..sp_executesql'

	EXEC @SpExecuteSql N'
	SELECT TOP(1)
		@TableName = QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + N''.'' + QUOTENAME(OBJECT_NAME(object_id)),
		@CTLastPulledVersion = ISNULL(@CTLastPulledVersion, CHANGE_TRACKING_MIN_VALID_VERSION(object_id))
	FROM sys.change_tracking_tables
	WHERE @TableName IS NULL OR OBJECT_ID(@TableName) = object_id'
		, N'@TableName nvarchar(1000) OUTPUT, @CTLastPulledVersion bigint OUTPUT'
		, @TableName OUTPUT, @CTLastPulledVersion OUTPUT

	IF @TableName IS NULL
	BEGIN
		RAISERROR(N'No tables found with change tracking enabled in database "%s"',16,1,@DBName);
	END
	ELSE
	BEGIN
		EXEC #GetChangeTrackingDelta
			  @TableName = @TableName
			, @DBName = @DBName
			, @CTLastPulledVersion = @CTLastPulledVersion
			, @Debug = 1
			, @OutputCommand = @OutputCommand OUTPUT
		
		SELECT @OutputCommand AS [@OutputCommand]
	END
END
GO