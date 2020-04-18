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
-- Description:	the procedure Checks for DB Blocked Requests
-- =============================================
CREATE PROCEDURE [Report].[usp_DBBlockedRequestsReport]
	@DebugMode BIT = 0,
	@HTMLTable NVARCHAR(MAX) OUTPUT
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#BlockedRequests') IS NOT NULL 
			BEGIN 
				DROP TABLE #BlockedRequests
				RAISERROR('the #BlockedRequests table already exist',0,0) WITH NOWAIT;
			END 

		SELECT          
			[Blocker SPID]			=B.[SPID] ,          
			[Blocker Last Batch]	=B.[Last_Batch] ,          
			[Blocker Open Tran]		=B.[Open_Tran] ,          
			[Blocker Wait Time]		=B.[WaitTime] ,          
			[Blocker Login Name]	=B.[LogiName] ,          
			[Blocker Host Name]		=B.[HostName] ,          
			[Blocker Program Name]	=B.[Program_Name] ,          
			[Blocker status]		=B.[Status] ,                  
			[Blocker Db Name]		=DB_NAME(B.[DBID]),
			[Blocker Command Text]	=BlockerText.[text],         
			[Victim SPID]			=V.[SPID] ,          
			[Victim Last Batch]		=V.[Last_Batch] ,          
			[Victim Open Tran]		=V.[Open_Tran] ,          
			[Victim Wait Time]		=V.[WaitTime] ,          
			[Victim Login Name]		=V.[LogiName] ,          
			[Victim Host Name]		=V.[HostName] ,          
			[Victim Program Name]	=V.[Program_Name] ,          
			[Victim status]			=V.[Status] ,                   
			[Victim Db Name]		=DB_NAME(V.[DBID]),
			[Victim Command Text]	=VictimText.[text]
		INTO #BlockedRequests         
		FROM		[master].[dbo].[sysprocesses] B (NOLOCK)  
		JOIN		[master].[dbo].[sysprocesses] V (NOLOCK)  ON   V.Blocked = B.SPID  
		OUTER APPLY [sys].[dm_exec_sql_text] (B.sql_handle)  BlockerText
		OUTER APPLY [sys].[dm_exec_sql_text] (V.sql_handle) VictimText
		WHERE	V.waittime > 1000*60*10 --10 Min
				AND  V.Blocked != 0  
				AND  V.Blocked != V.SPID

		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[Blocker SPID],[Blocker Last Batch],[Blocker Open Tran],[Blocker Wait Time],[Blocker Login Name],[Blocker Host Name],[Blocker Program Name],
			[Blocker status],[Blocker Db Name],[Blocker Command Text],[Victim SPID],[Victim Last Batch],[Victim Open Tran],[Victim Wait Time],
			[Victim Login Name],[Victim Host Name],[Victim Program Name],[Victim status],[Victim Db Name],[Victim Command Text]
		FROM #BlockedRequests																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#BlockedRequests',
			@Header =N'DB State Report',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
		---------------------------------------------------------------------------------
		DROP TABLE #BlockedRequests

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#BlockedRequests') IS NOT NULL 
				BEGIN 
					DROP TABLE #BlockedRequests;
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