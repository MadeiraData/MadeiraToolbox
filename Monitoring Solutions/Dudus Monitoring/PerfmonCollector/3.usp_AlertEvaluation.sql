USE [DB_DBA]
GO

IF SCHEMA_ID('Perfmon') IS NULL 
	BEGIN 
		DECLARE @Command NVARCHAR(MAX) = N'CREATE SCHEMA [Report]';
		EXEC (@Command);
	END;

GO
-- =============================================
-- Author:		David Sinai
-- Create date: 11-11-2014
-- Description:	the procedure evaluates Dynamic alerts
-- =============================================
CREATE PROCEDURE [Perfmon].[usp_AlertEvaluation]
	@HTMLTable NVARCHAR(MAX) OUTPUT,
	@DebugMode BIT = 0,
	@LastNMinutes INT,	--Last N minutes to test
	@CounterId INT,		
	@Threshold INT, 
	@ConditionId TINYINT, 
	@OperatorId TINYINT,
	@Header NVARCHAR(200)
AS
BEGIN
	BEGIN TRY 

		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#Evaluation') IS NOT NULL 
			BEGIN 
				DROP TABLE #Evaluation
				RAISERROR('the #Evaluation table already exist',0,0) WITH NOWAIT;
			END 

		CREATE TABLE #Evaluation
		(
			[Start Date Time]	DATETIME,
			[End Date Time]		DATETIME,
			[Counter Name]		NVARCHAR(4000),
			[Max Counter Value]	DECIMAL(23,2), 
			[Min Counter Value]	DECIMAL(23,2),
			[AVG Counter Value]	DECIMAL(23,2)
		)
		---------------------------------------------------------------------------------

		------------------Generate the Insert command for all databases------------------
		
		DECLARE @Command NVARCHAR(MAX)

		SELECT @Command = 
			N'INSERT INTO #Evaluation ([Start Date Time],[End Date Time],[Counter Name],[Max Counter Value],[Min Counter Value],[AVG Counter Value])'																			+CHAR(10)+
			N'SELECT '																																																			+CHAR(10)+
			N'	[Start Date Time]	= MIN([StartDateTime]),'																																									+CHAR(10)+
			N'	[End Date Time]		= MAX([StartDateTime]),'																																									+CHAR(10)+
			N'	[Counter Name]		= A.[CounterFullName],'																																										+CHAR(10)+
			N'	[Max Counter Value] = CAST(MAX([CounterValue]) AS DECIMAL(23,2)),'																																				+CHAR(10)+
			N'	[Min Counter Value] = CAST(MIN([CounterValue]) AS DECIMAL(23,2)),'																																				+CHAR(10)+
			N'	[AVG Counter Value] = CAST(AVG([CounterValue]) AS DECIMAL(23,2))'																																				+CHAR(10)+
			N'FROM		[Perfmon].[CounterCollector]'																																											+CHAR(10)+
			N'JOIN		[Perfmon].[CounterCollectorProcesses] ON [CounterCollectorProcesses].[Id] = [CounterCollector].[ProcesseId]'																							+CHAR(10)+
			N'JOIN		[Perfmon].[Counters] ON [Counters].[Id] =  [CounterCollector].[CounterId]'																																+CHAR(10)+
			N'CROSS APPLY (SELECT [CounterFullName] = [Counters].[DisplayName]+CASE WHEN [Counters].[InstanceName] = N'''' OR [Counters].[InstanceName] IS NULL THEN N'''' ELSE N''(''+[Counters].[InstanceName]+N'')'' END) A'	+CHAR(10)+
			N'WHERE	[CounterCollectorProcesses].[StartDateTime] > DATEADD(MINUTE,-1*@LastNMinutes,SYSDATETIME()) '																												+CHAR(10)+
			N'		AND [CounterCollector].[CounterId] =  @CounterId'																																							+CHAR(10)+
			N'GROUP BY [CounterCollector].[CounterId],A.[CounterFullName]'																																						+CHAR(10)+
			N'HAVING '+REPLACE([Conditions].[definition],N'$#@Expression$#@',N'[CounterValue]') + REPLACE([Operators].[definition],N'$#@[value]$#@',CAST(@Threshold AS NVARCHAR(10)))											+CHAR(10)
		FROM [Perfmon].[Operators]
		CROSS JOIN [Perfmon].[Conditions]
		WHERE	[Operators].[Id] = @OperatorId
				AND [Conditions].[Id] = @ConditionId

		IF @DebugMode = 1 
			SELECT @Command
		---------------------------------------------------------------------------------

		---------------------------------Execute command---------------------------------
		EXEC SP_EXECUTESQL @Command,N'@LastNMinutes INT, @CounterId INT',@LastNMinutes=@LastNMinutes,@CounterId=@CounterId
		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[Start Date Time],[End Date Time],[Counter Name],[Max Counter Value],[Min Counter Value],[AVG Counter Value] 
		FROM #Evaluation																				
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#Evaluation',
			@Header = @Header,
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = 0
		---------------------------------------------------------------------------------

		DROP TABLE #Evaluation

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#Evaluation') IS NOT NULL 
				BEGIN 
					DROP TABLE #Evaluation;
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