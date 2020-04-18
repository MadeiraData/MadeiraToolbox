USE [DB_DBA]
GO

IF SCHEMA_ID('Report') IS NULL 
BEGIN 
	EXEC ('CREATE SCHEMA [Report]')
END 
GO
-- =============================================
-- Author:		David Sinai
-- Create date: 11-11-2014
-- Description:	the procedure Checks for Failed Jobs
-- =============================================
CREATE PROCEDURE [Report].[usp_AvailabilityGroupHealthIssues]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#HealthCheck') IS NOT NULL 
			BEGIN 
				DROP TABLE #HealthCheck
				RAISERROR('the #HealthCheck table already exist',0,0) WITH NOWAIT;
			END; 

		SELECT 
			[DBName] = [availability_databases_cluster].[database_name]
			,[SynchronizationState] = [dm_hadr_database_replica_states].[synchronization_state_desc]
			,[SuspendReason] = [dm_hadr_database_replica_states].[suspend_reason_desc]
			,[SynchronizationHealth] = [dm_hadr_database_replica_states].[synchronization_health_desc]
		INTO #HealthCheck
		FROM		[sys].[dm_hadr_database_replica_states]
		INNER JOIN	[sys].[availability_databases_cluster] ON [dm_hadr_database_replica_states].[group_database_id] = [availability_databases_cluster].[group_database_id]
		WHERE	[dm_hadr_database_replica_states].[is_local] = 1
				AND 
				(	[dm_hadr_database_replica_states].[synchronization_state] != 2 --SYNCHRONIZED
					OR [dm_hadr_database_replica_states].[suspend_reason] IS NOT NULL
					OR [dm_hadr_database_replica_states].[synchronization_health] != 2
				)

		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[DBName],[SynchronizationState],[SuspendReason],[SynchronizationHealth]
		FROM #HealthCheck																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#HealthCheck',
			@Header =N'Availability Group Health Issues',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------
		DROP TABLE #HealthCheck

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#HealthCheck') IS NOT NULL 
				BEGIN 
					DROP TABLE #HealthCheck;
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
GO


