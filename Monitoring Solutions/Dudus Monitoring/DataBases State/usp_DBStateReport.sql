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
-- Description:	the procedure Checks for DB State other than online
-- =============================================
CREATE PROCEDURE [Report].[usp_DBStateReport]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#DBState') IS NOT NULL 
			BEGIN 
				DROP TABLE #DBState
				RAISERROR('the #DBState table already exist',0,0) WITH NOWAIT;
			END 

		SELECT 
			[DB Name] =name, [State]=N'<font color="red">'+state_desc+N'</font>'
		INTO #DBState
		FROM sys.databases 
		WHERE state != 0

		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[DB Name],[State]
		FROM #DBState																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#DBState',
			@Header =N'DB State Report',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------
		DROP TABLE #DBState

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#DBState') IS NOT NULL 
				BEGIN 
					DROP TABLE #DBState;
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