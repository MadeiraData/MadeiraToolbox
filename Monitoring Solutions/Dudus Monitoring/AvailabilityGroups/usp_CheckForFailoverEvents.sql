USE [DB_DBA]
GO

IF SCHEMA_ID('Audit') IS NULL 
BEGIN 
	EXEC ('CREATE SCHEMA [Audit]')
END 

IF OBJECT_ID('Audit.FailoverEvents') IS NULL 
BEGIN 
	CREATE TABLE [Audit].[FailoverEvents]
	(
		[Id]					INT IDENTITY(1,1) CONSTRAINT [PK_Audit_FailoverEvents] PRIMARY KEY CLUSTERED,
		[AvailabilityGroupId]	UNIQUEIDENTIFIER	NOT NULL,
		[PreviousRole]			INT					NULL,
		[PreviousDescription]	NVARCHAR(200)		NULL,
		[Role]					INT					NOT NULL,
		[Description]			NVARCHAR(200)		NOT NULL,
		[ChangeDateTime]		DATETIME2			NOT NULL

	)

	CREATE INDEX [IX_nc_nu_Audit_FailoverEvents_ChangeDateTime]  ON [Audit].[FailoverEvents] ([AvailabilityGroupId],[ChangeDateTime]) INCLUDE ([Role],[Description])
END 

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
CREATE PROCEDURE [Report].[usp_CheckForFailoverEvents]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#RoleChanges') IS NOT NULL 
			BEGIN 
				DROP TABLE #RoleChanges
				RAISERROR('the #RoleChanges table already exist',0,0) WITH NOWAIT;
			END; 

		WITH CurrentStatus AS 
		(
			SELECT 
				[availability_groups].[group_id],
				[availability_groups].[name],
				[dm_hadr_availability_replica_states].[role],
				[dm_hadr_availability_replica_states].[role_desc]
			FROM [master].[sys].[availability_groups]
			INNER JOIN [master].[sys].[dm_hadr_availability_replica_states] ON 	[availability_groups].[group_id] = [dm_hadr_availability_replica_states].[group_id]
			WHERE [dm_hadr_availability_replica_states].[is_local] = 1
		),
		PreviousStatus AS 
		(
			SELECT * 
			FROM	(
						SELECT [FailoverEvents].[Id] ,
							   [FailoverEvents].[AvailabilityGroupId] ,
							   [FailoverEvents].[Role] ,
							   [FailoverEvents].[Description] ,
							   [FailoverEvents].[ChangeDateTime],
							   [Rnum] = ROW_NUMBER() OVER (PARTITION BY [FailoverEvents].[AvailabilityGroupId] ORDER BY [FailoverEvents].[ChangeDateTime] DESC) 
						FROM [Audit].[FailoverEvents]
					)T
			WHERE [Rnum] = 1  
		)
		SELECT  
			[AvailabilityGroupId]	= [CurrentStatus].[group_id],
			[AvailabilityGroupName]	= [CurrentStatus].[name],
			[Role]					= [CurrentStatus].[role],
			[Description]			= [CurrentStatus].[role_desc],
			[PreviousRole]			= [PreviousStatus].[Role],
			[PreviousDescription]	= [PreviousStatus].[Description]
		INTO #RoleChanges
		FROM		[CurrentStatus] 
		LEFT JOIN	[PreviousStatus]  ON [CurrentStatus].[group_id] =  [PreviousStatus].[AvailabilityGroupId]
		WHERE [CurrentStatus].[role]!=[PreviousStatus].[role] OR [PreviousStatus].[role] IS NULL

		---------------------------------------------------------------------------------

		----------------------Insert the change into the audit table---------------------
		INSERT INTO [Audit].[FailoverEvents] ([AvailabilityGroupId],[PreviousRole],[PreviousDescription],[Role],[Description],[ChangeDateTime])
		SELECT [AvailabilityGroupId],[PreviousRole],[PreviousDescription],[Role],[Description],SYSDATETIME()
		FROM #RoleChanges
		---------------------------------------------------------------------------------

		---------------------------remove unnecessary columns----------------------------
		DECLARE @Command NVARCHAR(MAX)
		
		SELECT	@Command =	COALESCE(@Command+NCHAR(10),N'')+
							N'ALTER TABLE #RoleChanges DROP COLUMN'+NCHAR(9)+QUOTENAME(name)
		FROM [tempdb].[sys].[columns]
		WHERE name NOT IN ('AvailabilityGroupName','PreviousDescription','Description')
		AND object_id = OBJECT_ID(N'tempdb..#RoleChanges')

		EXEC (@Command)
		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[AvailabilityGroupName],[PreviousDescription],[Description]
		FROM #RoleChanges																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#RoleChanges',
			@Header =N'Availability Group Role Switch',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------
		DROP TABLE #RoleChanges

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#RoleChanges') IS NOT NULL 
				BEGIN 
					DROP TABLE #RoleChanges;
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


