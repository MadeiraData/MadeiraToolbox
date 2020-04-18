USE [DB_DBA]
GO 

-- =============================================
-- Author:		David Sinai
-- Create date: 11-11-2014
-- Description:	the procedure generates the statistics report
-- =============================================
CREATE PROCEDURE [Perfmon].[usp_SendReport]
	@HTMLTable NVARCHAR(MAX) OUTPUT,
	@DebugMode BIT = 0,
	@SendReport BIT = 1
AS
BEGIN
	BEGIN TRY 
		DECLARE @html NVARCHAR(MAX);
		-----------------------------Create the result table-----------------------------
		
		IF OBJECT_ID ('tempdb..#Report') IS NOT NULL 
			BEGIN 
				DROP TABLE #Report
				RAISERROR('the #Report table already exist',0,0) WITH NOWAIT;
			END;



		WITH T AS (
		SELECT 
			DatesRangeId = DatesRange.Id,
			DatesRange = DatesRange.Name,
			[CounterCollector].[CounterId],
			A.[CounterFullName],
			MaxCounterValue = CAST(MAX([CounterValue]) AS DECIMAL(23,2)),
			MinCounterValue = CAST(MIN([CounterValue]) AS DECIMAL(23,2)),
			AVGCounterValue = CAST(AVG([CounterValue]) AS DECIMAL(23,2))
		FROM 
		[Perfmon].[CounterCollector]
		JOIN [Perfmon].[CounterCollectorProcesses] ON [CounterCollectorProcesses].[Id] = [CounterCollector].[ProcesseId]
		JOIN [Perfmon].[Counters] ON [Counters].[Id] =  [CounterCollector].[CounterId]
		JOIN	(
					VALUES	(1,N'Last 24 Hours',DATEADD(DAY,-1,SYSDATETIME()),SYSDATETIME()),
							(2,N'One Week Ago',DATEADD(DAY,-8,SYSDATETIME()),DATEADD(DAY,-7,SYSDATETIME())),
							(3,N'One Month Ago',DATEADD(DAY,-29,SYSDATETIME()),DATEADD(DAY,-28,SYSDATETIME())),
							(4,N'One Year Ago',DATEADD(DAY,-365,SYSDATETIME()),DATEADD(DAY,-364,SYSDATETIME()))
				)DatesRange(Id,Name,StartDateTime,EndDateTime) ON [CounterCollectorProcesses].StartDateTime >= DatesRange.StartDateTime AND [CounterCollectorProcesses].StartDateTime <= DatesRange.EndDateTime
		CROSS APPLY (SELECT [CounterFullName] = [Counters].[DisplayName]+CASE WHEN [Counters].[InstanceName] = N'' OR [Counters].[InstanceName] IS NULL THEN N'' ELSE N'('+[Counters].[InstanceName]+N')' END) A
		GROUP BY [CounterCollector].[CounterId],A.[CounterFullName],DatesRange.Name,DatesRange.Id) 

			SELECT 
				[Counter Id]		= ISNULL(CAST(Last24Hours.CounterId				AS NVARCHAR(MAX))	,''),		--[Counter Id]
				[Counter Name]		= ISNULL(CAST(Last24Hours.CounterFullName		AS NVARCHAR(MAX))	,''),		--[Counter Name]
				[AVG Counter Value]	= ISNULL(CAST(Last24Hours.AVGCounterValue		AS NVARCHAR(MAX))	,''),		--[AVG Counter Value]
				[Max Counter Value]	= ISNULL(CAST(Last24Hours.MaxCounterValue		AS NVARCHAR(MAX))	,''),		--[Max Counter Value]
				[Min Counter Value]	= ISNULL(CAST(Last24Hours.MinCounterValue		AS NVARCHAR(MAX))	,''),		--[Min Counter Value]
				[AVG Week Ago]		= ISNULL(CAST(Change.[AVGComparedToAWeekAgo]	AS NVARCHAR(MAX))	,''),
				[AVG Month Ago]		= ISNULL(CAST(Change.[AVGComparedToAMonthAgo]	AS NVARCHAR(MAX))	,''),	
				[AVG Year Ago]		= ISNULL(CAST(Change.[AVGComparedToAYearAgo]	AS NVARCHAR(MAX))	,''),
				[MAX Week Ago]		= ISNULL(CAST(Change.[MAXComparedToAWeekAgo]	AS NVARCHAR(MAX))	,''),
				[MAX Month Ago]		= ISNULL(CAST(Change.[MAXComparedToAMonthAgo]	AS NVARCHAR(MAX))	,''),	
				[MAX Year Ago]		= ISNULL(CAST(Change.[MAXComparedToAYearAgo]	AS NVARCHAR(MAX))	,''),
				[MIN Week Ago]		= ISNULL(CAST(Change.[MINComparedToAWeekAgo]	AS NVARCHAR(MAX))	,''),
				[MIN Month Ago]		= ISNULL(CAST(Change.[MINComparedToAMonthAgo]	AS NVARCHAR(MAX))	,''),	
				[MIN Year Ago]		= ISNULL(CAST(Change.[MINComparedToAYearAgo]	AS NVARCHAR(MAX)) 	,'')
			INTO #Report
			FROM T AS Last24Hours
			LEFT JOIN T AS OneWeekAgo ON Last24Hours.CounterId = OneWeekAgo.CounterId AND OneWeekAgo.DatesRangeId = 2 
			LEFT JOIN T AS OneMonthAgo ON Last24Hours.CounterId = OneMonthAgo.CounterId AND OneMonthAgo.DatesRangeId = 3
			LEFT JOIN T AS OneYearAgo ON Last24Hours.CounterId = OneYearAgo.CounterId AND OneYearAgo.DatesRangeId = 4
			CROSS APPLY (SELECT 
							[AVGComparedToAWeekAgo]		= FORMAT(CASE WHEN OneWeekAgo.AVGCounterValue = 0	THEN NULL ELSE  (Last24Hours.AVGCounterValue-OneWeekAgo.AVGCounterValue)/OneWeekAgo.AVGCounterValue/100		END ,'p'),
							[AVGComparedToAMonthAgo]	= FORMAT(CASE WHEN OneMonthAgo.AVGCounterValue = 0	THEN NULL ELSE  (Last24Hours.AVGCounterValue-OneMonthAgo.AVGCounterValue)/OneMonthAgo.AVGCounterValue/100	END ,'p'),
							[AVGComparedToAYearAgo]		= FORMAT(CASE WHEN OneYearAgo.AVGCounterValue = 0	THEN NULL ELSE  (Last24Hours.AVGCounterValue-OneYearAgo.AVGCounterValue)/OneYearAgo.AVGCounterValue/100		END ,'p'),
								  
							[MAXComparedToAWeekAgo]		= FORMAT(CASE WHEN OneWeekAgo.MaxCounterValue = 0	THEN NULL ELSE  (Last24Hours.MaxCounterValue-OneWeekAgo.MaxCounterValue)/OneWeekAgo.MaxCounterValue/100		END ,'p'),
							[MAXComparedToAMonthAgo]	= FORMAT(CASE WHEN OneMonthAgo.MaxCounterValue = 0	THEN NULL ELSE  (Last24Hours.MaxCounterValue-OneMonthAgo.MaxCounterValue)/OneMonthAgo.MaxCounterValue/100	END ,'p'),
							[MAXComparedToAYearAgo]		= FORMAT(CASE WHEN OneYearAgo.MaxCounterValue = 0	THEN NULL ELSE  (Last24Hours.MaxCounterValue-OneYearAgo.MaxCounterValue)/OneYearAgo.MaxCounterValue/100		END ,'p'),
								  
							[MINComparedToAWeekAgo]		= FORMAT(CASE WHEN OneWeekAgo.MinCounterValue = 0	THEN NULL ELSE  (Last24Hours.MinCounterValue-OneWeekAgo.MinCounterValue)/OneWeekAgo.MinCounterValue/100		END ,'p'),
							[MINComparedToAMonthAgo]	= FORMAT(CASE WHEN OneMonthAgo.MinCounterValue = 0	THEN NULL ELSE  (Last24Hours.MinCounterValue-OneMonthAgo.MinCounterValue)/OneMonthAgo.MinCounterValue/100	END ,'p'),
							[MINComparedToAYearAgo]		= FORMAT(CASE WHEN OneYearAgo.MinCounterValue = 0	THEN NULL ELSE  (Last24Hours.MinCounterValue-OneYearAgo.MinCounterValue)/OneYearAgo.MinCounterValue/100		END ,'p')
						)Change 
			WHERE Last24Hours.DatesRangeId = 1
			ORDER BY Last24Hours.CounterId ASC

		---------------------------------------------------------------------------------

		----------------------------------Return results---------------------------------
		SELECT 																								
			[Counter Id],[Counter Name],[AVG Counter Value],[Max Counter Value],[Min Counter Value],[AVG Week Ago],
			[AVG Month Ago],[AVG Year Ago],[MAX Week Ago],[MAX Month Ago],[MAX Year Ago],[MIN Week Ago],
			[MIN Month Ago],[MIN Year Ago]	
		FROM #Report	
																		
		---------------------------------------------------------------------------------

		----------------------------------Generate HTML----------------------------------
		EXEC[dbo].[usp_GenerateHTMLFromTable]
			@tableName =N'#Report',
			@Header =N'Statistics Report',
			@HTML = @HTMLTable OUTPUT,
			@DebugMode = @DebugMode

		IF @DebugMode = 1 
			SELECT @HTMLTable AS N'@HTMLTable'
		---------------------------------------------------------------------------------

		----------------------------------Send HTML----------------------------------
		IF @SendReport = 1 
		BEGIN 
			SET @html = '<html><header></header><body>'+@HTMLTable+'</body></html>'
		
			IF @DebugMode = 1 
				SELECT @html AS N'@html'

			EXEC msdb.dbo.sp_send_dbmail 
				@body = @html
				, @recipients = 'dudu@madeira.co.il'
				, @subject = 'Performance Report'
				, @profile_name = 'sales.neworder gmail'
				, @body_format = 'HTML';
		END 

		---------------------------------------------------------------------------------
		DROP TABLE #Report

	END TRY 
	BEGIN CATCH 
			IF @@TRANCOUNT > 1 
				ROLLBACK;
			IF OBJECT_ID ('tempdb..#Report') IS NOT NULL 
				BEGIN 
					DROP TABLE #Report;
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