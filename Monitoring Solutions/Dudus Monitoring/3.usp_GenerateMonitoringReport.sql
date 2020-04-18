USE [DB_DBA]
GO

IF SCHEMA_ID('Report') IS NULL 
	BEGIN 
		DECLARE @Command NVARCHAR(MAX) = N'CREATE SCHEMA [Report]';
		EXEC (@Command);
	END;

GO

IF NOT EXISTS (SELECT TOP 1 1  FROM sys.types WHERE name = N'udt_IntTable')
BEGIN 
	CREATE TYPE [udt_IntTable] AS TABLE 
	(
		Number INT NOT NULL 
	)
END 
GO 
-- =============================================
-- Author:		David Sinai
-- Create date: 11-11-2014
-- Description:	the procedure Generate and sends Monitoring Report  
-- =============================================
CREATE PROCEDURE [Report].[usp_GenerateMonitoringReport] 
	@MonitoringAlerts [dbo].[udt_IntTable] READONLY,
	@IgnoreSendInterval BIT = 0,
	@SendTableOfContent BIT = 0,
	@DebugMode BIT = 0
AS 

BEGIN 
BEGIN TRY 
		----------------------------Declare Variables----------------------------
		DECLARE @HTMLTable NVARCHAR(MAX),
				@HTML NVARCHAR(MAX),
				@Command NVARCHAR(MAX),
				@IsDailyReport BIT = 0
		
		SELECT TOP 0 Number INTO #MonitoringAlerts FROM @MonitoringAlerts
		IF EXISTS (SELECT TOP 1 1 FROM @MonitoringAlerts)
			BEGIN 
				INSERT INTO #MonitoringAlerts (Number)
					SELECT Number FROM @MonitoringAlerts
			END 
		ELSE 
			BEGIN 
				INSERT INTO #MonitoringAlerts (Number)
				SELECT Id FROM [Report].[MonitoringAlerts]
			END;
		-------------------------------------------------------------------------
		
		------------------------Change config for the Daily Report------------------------
		
		SELECT @IgnoreSendInterval = 1, @SendTableOfContent = 1, @IsDailyReport = 1
		FROM [Report].[Configuration] 
		WHERE	[Key] = N'Daily Report Time'
				AND CAST (DATEADD(MINUTE,DATEDIFF(MINUTE,0,SYSDATETIME() ),0) AS TIME) = CAST ([Value] AS TIME);
		----------------------------------------------------------------------------------
		
		----------------------------Generate the Report Table of content----------------------------
		WITH SendInterval AS
				(
					SELECT 
						[SentAlerts].[MonitoringAlertsId],
						Rnum = ROW_NUMBER() OVER (PARTITION BY [SentAlerts].[MonitoringAlertsId] ORDER BY [SentAlerts].[EventDateTime] DESC ) 
					FROM [Report].[SentAlerts] 
					INNER JOIN [Report].[MonitoringAlerts] ON [MonitoringAlerts].[Id] = [SentAlerts].[MonitoringAlertsId]
					CROSS APPLY (SELECT DateTimeThreshold = DATEADD (MINUTE,-1*CAST([MonitoringAlerts].[ContinuousAlertsSendIntervalMin] AS INT),SYSDATETIME()) ) A
					WHERE [SentAlerts].[EventDateTime] > A.[DateTimeThreshold]
				),
			EvaluationInterval AS 
				(
					SELECT 
						Id
					FROM [Report].[MonitoringAlerts]
					WHERE DATEPART(MINUTE,SYSDATETIME()) % [EvaluationIntervalMin] = 0
				)
		SELECT 
			Id = CAST ([MonitoringAlerts].[Id] AS INT),
			[MonitoringAlerts].[Name],
			Status = CAST (1 AS BIT),
			[ExecutionCommand] = [MonitoringAlerts].[ExecutionCommand],
			HTMLTable = CAST (NULL AS NVARCHAR(MAX))
		INTO #TempTableOfContent
		FROM [Report].[MonitoringAlerts]
		JOIN #MonitoringAlerts ON #MonitoringAlerts.[Number] = [MonitoringAlerts].[Id]																			--Filter according to the @MonitoringAlerts Table
		LEFT JOIN	( SELECT [MonitoringAlertsId] FROM SendInterval WHERE Rnum = 1)[SentAlerts] ON [SentAlerts].[MonitoringAlertsId] = [MonitoringAlerts].[Id]	--Filter according to the SendInterval
		LEFT JOIN	EvaluationInterval ON [EvaluationInterval].[Id] = [MonitoringAlerts].[Id]																	--Filter according to the EvaluationInterval
		
		WHERE		[MonitoringAlerts].[Enabled] = 1  
					AND ([SentAlerts].[MonitoringAlertsId] IS NULL OR @IgnoreSendInterval = 1)
					AND (EvaluationInterval.[Id] IS NOT NULL OR @IgnoreSendInterval = 1)
					AND ([MonitoringAlerts].[EnableContinuesMonitoring] = 1 OR @IsDailyReport = 1)
		ORDER BY [MonitoringAlerts].[Id]

		--------------------------------------------------------------------------------------------

		----------------------------Generate the table HTML----------------------------
		SELECT @Command = COALESCE (@Command+CHAR(10)+CHAR(10),N'') + 
				N'----------------------'+Name+N'-----------------------'						+CHAR(10)+
				N'SET @HTMLTable = NULL'														+CHAR(10)+CHAR(10)+
				[MonitoringAlerts].[ExecutionCommand]											+CHAR(10)+CHAR(10)+
				N'IF @HTMLTable IS NOT NULL '													+CHAR(10)+
				N'	BEGIN '																		+CHAR(10)+
				N'		UPDATE #TempTableOfContent '											+CHAR(10)+
				N'			SET Status = 0 , '													+CHAR(10)+
				N'				HTMLTable = @HTMLTable '										+CHAR(10)+
				N'		WHERE Id = '+CAST ([MonitoringAlerts].[Id] AS NVARCHAR(100))			+CHAR(10)+
				N'	END '																		+CHAR(10)+CHAR(10)+
				N'IF @DebugMode = 1'															+CHAR(10)+
				N'	SELECT @HTMLTable AS N''@HTMLTable'''										+CHAR(10)+
				N'-----------------------------------------------------'						+CHAR(10)
			
		FROM		#TempTableOfContent [MonitoringAlerts]
		ORDER BY [MonitoringAlerts].[Id]

		IF @DebugMode = 1
			SELECT @Command

		EXEC SP_EXECUTESQL @Command, N'@HTML NVARCHAR(MAX) OUTPUT,@HTMLTable NVARCHAR(MAX),@DebugMode BIT',@HTML=@HTML OUTPUT,@HTMLTable = @HTMLTable,@DebugMode=@DebugMode
		
		SELECT @HTML = COALESCE (@HTML,N'')+HTMLTable
		FROM  #TempTableOfContent
		WHERE [Status] = 0

		INSERT INTO [Report].[SentAlerts]([MonitoringAlertsId], [Description],[EventDateTime])
			SELECT [Id],[HTMLTable],SYSDATETIME() FROM #TempTableOfContent WHERE [Status] = 0
		-------------------------------------------------------------------------------

		----------------------------Generate HTML for the Table of content----------------------------
			
		IF @SendTableOfContent = 1
		BEGIN 
			SELECT	
					Id = ROW_NUMBER() OVER (ORDER BY (SELECT Id)),
					Name,
					A.Status
			INTO #TableOfContent
			FROM	#TempTableOfContent TOC
			CROSS APPLY (SELECT Status = CASE WHEN TOC.Status = 1 THEN N'<font color="green">✔</font>' ELSE N'<font color="red">X</font>' END ) A
			
			IF @DebugMode = 1
				SELECT * FROM  #TableOfContent
			SET @HTMLTable = NULL 

			EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#TableOfContent',
			@Header =N'<p align="center">Table Of Content</p>',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode
			
			IF @DebugMode = 1 
				SELECT @HTMLTable AS N'@HTMLTable'

			SET @HTML = @HTMLTable+ISNULL(@HTML,N'')
		END
		----------------------------------------------------------------------------------------------

		----------------------------Generate the HTML Page----------------------------
		IF @DebugMode = 1
			SELECT @HTML AS N'@HTML'	

		IF @HTML != N'' AND @HTML IS NOT NULL
			BEGIN 
				SET @Command = ''
				--------------------------------Add HTML Header--------------------------------
				
				SELECT @HTML = 
				N'<div id = "Header"><img src="'+Company.[Value]+'" height="100" width="150" align = "left">'
				+N'<img src="'+Madeira.[Value]+'" height="100" width="150" align = "right">'
				+N'<h1><p align = "center">'+EmailSubject.[Value]+N'</p></h1><h5><p align = "center">'+@@SERVERNAME+N'</p><p align = "center">'+CONVERT(NVARCHAR(100),SYSDATETIME(),120)+N'</p></h5></div><div id = "Body"></br></br></br>'+@HTML+N'</div>'
				FROM 
					[Report].[Configuration] Madeira
					CROSS JOIN [Report].[Configuration] Company
					CROSS JOIN [Report].[Configuration] EmailSubject
				WHERE	Madeira.[Key] = N'Madeira Company Logo'
						AND Company.[Key] = N'Client Company Logo'
						AND EmailSubject.[Key] = CASE WHEN @IsDailyReport = 1 THEN  N'Daily Report Subject' ELSE N'Error Report Subject' END 
				-------------------------------------------------------------------------------
				
				SELECT 
					@Command = 
						N'EXEC msdb.dbo.sp_send_dbmail'					+CHAR(10)+
						N'@body = @HTML'								+CHAR(10)+
						N', @recipients = '''+REPLACE(Recipients.[Value],'''','''''')+''''	+CHAR(10)+
						N', @subject = '''+REPLACE(EmailSubject.[Value],'''','''''')+''''	+CHAR(10)+
						N', @profile_name = '''+REPLACE(ProfileName.[Value],'''','''''')+''''+CHAR(10)+
						N', @body_format = ''HTML'''					+CHAR(10)
				FROM 
					[Report].[Configuration] Recipients
					CROSS JOIN [Report].[Configuration] EmailSubject
					CROSS JOIN [Report].[Configuration] ProfileName
				WHERE 
					Recipients.[Key] = N'Email Recipients'
					AND EmailSubject.[Key] = CASE WHEN @IsDailyReport = 1 THEN  N'Daily Report Subject' ELSE N'Error Report Subject' END 
					AND ProfileName.[Key] = N'Send Profile Name'
				
				IF @DebugMode = 1
					SELECT @Command AS N'@Command'
				
				EXEC sp_executesql @Command,N'@HTML NVARCHAR(MAX)',@HTML=@HTML
			END 
		------------------------------------------------------------------------------

END TRY 
BEGIN CATCH 
	IF @@TRANCOUNT > 1 
		ROLLBACK; 
	IF OBJECT_ID(N'tempdb..#TableOfContent') IS NOT NULL 
		DROP TABLE #TableOfContent 
	IF OBJECT_ID(N'tempdb..#TempTableOfContent') IS NOT NULL 
		DROP TABLE #TempTableOfContent
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