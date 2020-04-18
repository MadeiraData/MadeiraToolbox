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
-- Description:	the procedure Checks for Non Working Services
-- =============================================
CREATE PROCEDURE [Report].[usp_NonWorkingServices]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#ServerServices') IS NOT NULL 
			BEGIN 
				DROP TABLE #ServerServices
				RAISERROR('the #ServerServices table already exist',0,0) WITH NOWAIT;
			END 

		SELECT [ServiceName] = servicename, [Status] = status_desc
		INTO #ServerServices
		FROM sys.dm_server_services
		WHERE 
			[status] != 4 AND startup_type = 2 

		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[ServiceName],[Status]
		FROM #ServerServices																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#ServerServices',
			@Header =N'Non Working Services',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = 0
		---------------------------------------------------------------------------------
		DROP TABLE #ServerServices

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#ServerServices') IS NOT NULL 
				BEGIN 
					DROP TABLE #ServerServices;
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