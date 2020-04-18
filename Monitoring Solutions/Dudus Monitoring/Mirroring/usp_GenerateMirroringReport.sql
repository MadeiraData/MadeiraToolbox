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
-- Description:	the procedure returns mirroring events from the last 24 hours   
-- =============================================
CREATE PROCEDURE [Report].[usp_GenerateMirroringReport]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		IF OBJECT_ID ('tempdb..#Result') IS NOT NULL 
			BEGIN 
				DROP TABLE #Result
				RAISERROR('the #Result table already exist',0,0) WITH NOWAIT;
			END;
		
		DECLARE @LastNMin INT

		SELECT 
			@LastNMin=CASE WHEN CAST (DATEADD(MINUTE,DATEDIFF(MINUTE,0,SYSDATETIME() ),0) AS TIME) = CAST (DailyReport.[Value] AS TIME) THEN 24*60 ELSE CAST(SendInterval.[Value] AS INT)*1.1 END		
		FROM [Report].[Configuration] DailyReport
		CROSS JOIN [Report].[Configuration] SendInterval
		WHERE	DailyReport.[Key] = N'Daily Report Time'
				AND SendInterval.[Key] = N'Continuous Alerts Send Interval (MINUTE)'

		SELECT 
			[Database],[EventTime],[NewState],[EventDescription]
		INTO #Result
		FROM [Mirroring].[DBMirroringStateChanges]
		WHERE	[EventTime] >= DATEADD(MINUTE, -1*@LastNMin ,GETDATE()) 
				AND	[EventTime] <= SYSDATETIME()
		OPTION (RECOMPILE)

		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[Database],[EventTime],[NewState],[EventDescription]
		FROM #Result																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#Result',
			@Header =N'Mirroring Events',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------

		DROP TABLE #Result

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			
			IF OBJECT_ID ('tempdb..#Result') IS NOT NULL 
				BEGIN 
					DROP TABLE #Result;
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