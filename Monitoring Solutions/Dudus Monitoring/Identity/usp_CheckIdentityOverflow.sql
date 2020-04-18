USE [DB_DBA]
GO

IF SCHEMA_ID('Report') IS NULL 
	BEGIN 
		DECLARE @Command NVARCHAR(MAX) = N'CREATE SCHEMA [Report]';
		EXEC (@Command);
	END;

GO
-- =============================================
-- Author:		David Sinai
-- Create date: 11-11-2014
-- Description:	the procedure Checks for Identity Overflow
-- =============================================
CREATE PROCEDURE [Report].[usp_CheckIdentityOverflow]
	@MaxPercent INT = 85,
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#IdentityColumns') IS NOT NULL 
			BEGIN 
				DROP TABLE #IdentityColumns
				RAISERROR('the #IdentityColumns table already exist',0,0) WITH NOWAIT;
			END 

		CREATE TABLE #IdentityColumns
		(
			Id				INT IDENTITY PRIMARY KEY CLUSTERED,
			DatabaseName	SYSNAME,
			SchemaName		SYSNAME,
			TableName		SYSNAME,
			ColumnName		SYSNAME,
			ColumnType		SYSNAME,
			LastValue		SQL_VARIANT,
			MaxValue		BIGINT,
			PercentUsed		DECIMAL(10, 2)
		)
		---------------------------------------------------------------------------------

		------------------Generate the Insert command for all databases------------------
		
		DECLARE @Command NVARCHAR(MAX)
		SELECT 
			@Command = COALESCE (@Command+CHAR(10)+CHAR(10)+CHAR(10),'')+
					'-----------------------['+databases.name+']-----------------------'																																+CHAR(10)+
					'	INSERT INTO  #IdentityColumns(DatabaseName,SchemaName,TableName,ColumnName,ColumnType,LastValue,MaxValue,PercentUsed)'																			+CHAR(10)+
					'	SELECT '																																														+CHAR(10)+
					'		N'''+databases.name+''' DatabaseName,schemas.name SchemaName, tables.name TableName, columns.name ColumnName,'																				+CHAR(10)+
					'		types.name ColumnType, Last_Value LastValue,Calc1.MaxValue,Calc2.Percent_Used'																												+CHAR(10)+
					'	FROM		['+databases.name+'].sys.identity_columns WITH (NOLOCK)'																															+CHAR(10)+
					'	INNER JOIN	['+databases.name+'].sys.columns WITH (NOLOCK) ON columns.column_id = identity_columns.column_id AND columns.object_id = identity_columns.object_id'								+CHAR(10)+
					'	INNER JOIN	['+databases.name+'].sys.tables WITH (NOLOCK) ON columns.object_id = tables.object_id'																								+CHAR(10)+
					'	INNER JOIN	['+databases.name+'].sys.schemas WITH (NOLOCK) ON schemas.schema_id = tables.schema_id'																								+CHAR(10)+
					'	INNER JOIN	['+databases.name+'].sys.types ON types.system_type_id = columns.system_type_id'																									+CHAR(10)+
					'	CROSS APPLY (SELECT MaxValue = POWER(CAST(256 AS BIGINT), identity_columns.max_length - 1) * CAST(127 AS BIGINT) + (POWER(CAST(256 AS BIGINT), identity_columns.max_length - 1) - 1)) Calc1'	+CHAR(10)+
					'	CROSS APPLY (SELECT Percent_Used = CAST(CAST(Last_Value AS BIGINT) *100./MaxValue AS DECIMAL(10, 2))) Calc2'																					+CHAR(10)+
					'	WHERE Percent_Used >= @MaxPercent'																																								+CHAR(10)+
					'----------------------------------------------------------------------------------'																												+CHAR(10)
		FROM [master].[sys].[databases]
		LEFT JOIN	[master].[sys].[availability_databases_cluster]			ON [databases].[name] = [availability_databases_cluster].[database_name]
		LEFT JOIN	[master].[sys].[dm_hadr_name_id_map]					ON [dm_hadr_name_id_map].[ag_id] = [availability_databases_cluster].[group_id]
		LEFT JOIN	[master].[sys].[dm_hadr_availability_replica_states]	ON [dm_hadr_availability_replica_states].[group_id] = [availability_databases_cluster].[group_id]
		WHERE 
			(([dm_hadr_availability_replica_states].[role] != 2 --SECONDARY 
			AND [dm_hadr_availability_replica_states].[is_local] = 1)
			OR [dm_hadr_availability_replica_states].[role]  IS NULL )
			AND [databases].[source_database_id] IS NULL 

		IF @DebugMode = 1 
			SELECT @Command
		---------------------------------------------------------------------------------

		---------------------------------Execute command---------------------------------
		EXEC SP_EXECUTESQL @Command,N'@MaxPercent int',@MaxPercent = @MaxPercent
		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			DatabaseName, SchemaName, TableName, ColumnName, ColumnType, LastValue, MaxValue,PercentUsed	
		FROM #IdentityColumns																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#IdentityColumns',
			@Header =N'Identity Overflow columns',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = 0
		---------------------------------------------------------------------------------

		DROP TABLE #IdentityColumns

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#IdentityColumns') IS NOT NULL 
				BEGIN 
					DROP TABLE #IdentityColumns;
				END;
			DECLARE @ErrorMessage NVARCHAR(4000);
			DECLARE @ErrorSeverity INT;
			DECLARE @ErrorState INT;
  
			SELECT 
				@ErrorMessage = ERROR_MESSAGE(),
				@ErrorSeverity = ERROR_SEVERITY(),
				@ErrorState = ERROR_STATE();
  
			RAISERROR (@ErrorMessage,@ErrorSeverity, @ErrorState );
	END CATCH 
END 