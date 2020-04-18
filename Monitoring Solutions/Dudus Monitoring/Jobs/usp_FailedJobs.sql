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
-- Description:	the procedure Checks for Failed Jobs
-- =============================================
CREATE PROCEDURE [Report].[usp_FailedJobs]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#FailedJobs') IS NOT NULL 
			BEGIN 
				DROP TABLE #FailedJobs
				RAISERROR('the #FailedJobs table already exist',0,0) WITH NOWAIT;
			END 

		SELECT      DISTINCT 
					[JobName]   = JOB.name,
					[Step]      = HIST.step_id,
					[StepName]  = HIST.step_name,
					[Message]   = HIST.message,
					[Status]    = HIST.run_status,
					[RunDate]   = HIST.run_date
		INTO #FailedJobs
		FROM        msdb..sysjobs JOB
			INNER JOIN  msdb..sysjobhistory HIST 
			ON HIST.job_id = JOB.job_id
		WHERE    HIST.run_status = 0
			AND enabled = 1
			AND HIST.step_id <> 0
			AND HIST.run_date = CONVERT(nvarchar(30), GETDATE(), 112)
			AND HIST.run_status = 
						(
							SELECT TOP 1 run_status 
							FROM msdb..sysjobhistory
							WHERE job_id = HIST.job_id
							ORDER BY run_date DESC, run_time desc
						)
		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[JobName],[Step],[StepName],[Message],[Status],[RunDate]
		FROM #FailedJobs																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#FailedJobs',
			@Header =N'Failed Jobs',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------
		DROP TABLE #FailedJobs

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#FailedJobs') IS NOT NULL 
				BEGIN 
					DROP TABLE #FailedJobs;
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