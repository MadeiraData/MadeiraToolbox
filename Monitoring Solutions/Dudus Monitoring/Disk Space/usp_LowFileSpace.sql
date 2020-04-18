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
-- Description:	the procedure Checks for Low File space
-- =============================================
CREATE PROCEDURE [Report].[usp_LowFileSpace]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT,
	@FreeSpacePercentThreshold INT = 20  -- free percentage
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#FileSpace') IS NOT NULL 
			BEGIN 
				DROP TABLE #FileSpace
				RAISERROR('the #FileSpace table already exist',0,0) WITH NOWAIT;
			END 

		IF @FreeSpacePercentThreshold > 100 
			SET @FreeSpacePercentThreshold = 100
		
		IF @FreeSpacePercentThreshold < 0 
			SET @FreeSpacePercentThreshold = 0

		DECLARE @Command NVARCHAR(MAX)
		
		CREATE TABLE #FileSpace ([DB Name] SYSNAME,[File Name] NVARCHAR(1000), [Current Size MB] INT, [Free Space MB] INT, [Free Space Percent] NVARCHAR(100) )

		SELECT 
			@Command = COALESCE (@Command+CHAR(10)+CHAR(10),N'')+
			N'USE ['+name+N']'																																			+CHAR(10)+
			N'INSERT INTO #FileSpace([DB Name],[File Name],[Current Size MB],[Free Space MB],[Free Space Percent])'														+CHAR(10)+
			N'SELECT DBName = '''+name+''',FileName = name, CurrentSizeMB,FreeSpaceMB, N''<font color="red">''+CAST(FreeSpacePercent AS NVARCHAR(100))+N''%</font>'''	+CHAR(10)+
			N'FROM sys.database_files'																																	+CHAR(10)+
			N'CROSS APPLY (SELECT CurrentSizeMB = size/128.0, FreeSpaceMB = size/128.0 - CAST(FILEPROPERTY(name, ''SpaceUsed'') AS INT)/128.0) T1'						+CHAR(10)+
			N'CROSS APPLY (SELECT FreeSpacePercent = CAST(FreeSpaceMB/CurrentSizeMB*100 AS DECIMAL (5,2))) T2'															+CHAR(10)+
			N'WHERE FreeSpacePercent < @FreeSpacePercentThreshold'
		FROM [master].[sys].[databases]
		LEFT JOIN	[master].[sys].[availability_databases_cluster]			ON [databases].[name] = [availability_databases_cluster].[database_name]
		LEFT JOIN	[master].[sys].[dm_hadr_name_id_map]					ON [dm_hadr_name_id_map].[ag_id] = [availability_databases_cluster].[group_id]
		LEFT JOIN	[master].[sys].[dm_hadr_availability_replica_states]	ON [dm_hadr_availability_replica_states].[group_id] = [availability_databases_cluster].[group_id]
		WHERE 
			(([dm_hadr_availability_replica_states].[role] != 2 --SECONDARY 
			AND [dm_hadr_availability_replica_states].[is_local] = 1)
			OR [dm_hadr_availability_replica_states].[role]  IS NULL )
			AND [databases].[source_database_id] IS NULL 

		EXEC SP_EXECUTESQL @Command,N'@FreeSpacePercentThreshold INT',@FreeSpacePercentThreshold=@FreeSpacePercentThreshold
		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[DB Name],[File Name],[Current Size MB],[Free Space MB],[Free Space Percent]
		FROM #FileSpace																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#FileSpace',
			@Header =N'Low File Space',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------
		DROP TABLE #FileSpace

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#FileSpace') IS NOT NULL 
				BEGIN 
					DROP TABLE #FileSpace;
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
