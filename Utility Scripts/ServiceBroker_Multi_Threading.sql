/*
===================================================
	Service Broker Sample 1: Parallel Querying
===================================================
Copyright:	Eitan Blumin (C) 2012
Email:		eitan@madeira.co.il
Source:		www.madeira.co.il
Disclaimer:
	The author is not responsible for any damage this
	script or any of its variations may cause.
	Do not execute it or any variations of it on production
	environments without first verifying its validity
	on controlled testing and/or QA environments.
	You may use this script at your own risk and may change it
	to your liking, as long as you leave this disclaimer header
	fully intact and unchanged.
*/
USE
	[SB_PQ_Test]
GO
SET NOCOUNT ON;

-- init variables
DECLARE
	@SQL			NVARCHAR(MAX),		-- holding the SQL command
	@OutputParam	VARCHAR(128),		-- the name of the output parameter
	@Segment		INT,				-- index number of the current segment (thread)
	@ConvGroup		UNIQUEIDENTIFIER,	-- conversation group ID
	@Delay			VARCHAR(8),			-- delay representation HH:MM:SS
	@MinDelay		INT,				-- minimum delay in seconds
	@MaxDelay		INT,				-- maximum delay in seconds
	@StartTime		DATETIME,			-- start time of this script
	@NumOfSegments	INT					-- number of segments (threads) to use

SET @StartTime = GETDATE();
SET @OutputParam = 'SB_PQ_Result';

-- random workload simulation settings
SET @MinDelay = 10
SET @MaxDelay = 30

SET @ConvGroup = NEWID();	-- the messages will be grouped in a specific conversation group
SET @NumOfSegments = 3;		-- number of "threads" to use

-- create several segments
SET @Segment = 1;

WHILE @Segment <= @NumOfSegments
BEGIN

	-- random delay between @MinDelay and @MaxDelay seconds to simulate long execution time
	SET @Delay = '00:00:' + CONVERT(varchar(8), ROUND(RAND() * (@MaxDelay - @MinDelay),0) + @MinDelay)
	
	-- build our dynamic SQL command. note the use of XML as the result.
	SET @SQL = N'
	WAITFOR DELAY ''' + @Delay + ''';
	
	SET @SB_PQ_Result = 
	(
		SELECT
			Segment = ' + CONVERT(nvarchar(max), @Segment) + N',
			Delay = ''' + @Delay + N''',
			StartDate = GETDATE(),
			Name = QUOTENAME(name),
			object_id, type, modify_date
		FROM
			SB_PQ_Test.sys.tables AS Tab
		FOR XML AUTO, ELEMENTS
	);
	';
	
	-- Send request to queue
	EXEC SB_PQ_Start_Query @SQL, @OutputParam, @ConvGroup;
	
	RAISERROR(N'Sent segment %d (intended delay %s)',0,0,@Segment,@Delay) WITH NOWAIT;
	
	-- increment segment index
	SET @Segment = @Segment + 1;
END

-- init final result
DECLARE @TotalResult XML;
SET @TotalResult = '<Tables> </Tables>'

-- Get results
RAISERROR(N'Getting results...',0,0) WITH NOWAIT;

DECLARE @CurrentResult XML, @CurrSegment VARCHAR(100), @CurrDelay VARCHAR(8), @CurrStart DATETIME;

-- count based on number of segments that we created earlier
SET @Segment = 1;

WHILE @Segment <= @NumOfSegments
BEGIN
	-- Get segment from response queue
	EXEC SB_PQ_Get_Response_One @ConvGroup, @CurrentResult OUTPUT
	
	-- extract result values
	SET @CurrSegment = @CurrentResult.value('(Tab/Segment)[1]','varchar(100)');
	SET @CurrDelay = @CurrentResult.value('(Tab/Delay)[1]','varchar(8)');
	SET @CurrStart = @CurrentResult.value('(Tab/StartDate)[1]','datetime');
	
	PRINT 
		'Received segment '
		+ @CurrSegment + ' '
		+ CONVERT(nvarchar(max),DATEDIFF(ms,@StartTime,@CurrStart)) + ' ms delay end since start'
		+ ' (intended delay ' + ISNULL(@CurrDelay,'<none>') + ')'
	
	-- insert into TotalResults using XML DML (syntax for SQL2008 and newer)
	SET @TotalResult.modify('
insert sql:variable("@CurrentResult")
into (/Tables)[1] ');

	-- increment segment index
	SET @Segment = @Segment + 1;
END

-- return final result (as XML)
SELECT @TotalResult.query('.') AS FinalResult

-- return final result (as relational table)
SELECT
	Segment		= T.XRecord.query('.').value('(/Tab/Segment)[1]','varchar(100)'),
	Delay		= T.XRecord.query('.').value('(/Tab/Delay)[1]','varchar(8)'),
	StartDate	= T.XRecord.query('.').value('(/Tab/StartDate)[1]','datetime')
FROM
	@TotalResult.nodes('/Tables/Tab') AS T(XRecord)



-- check the SB logs to see how many unique sessions executed our requests
DECLARE @NumOfSessions INT;

SELECT @NumOfSessions = COUNT(DISTINCT SPID)
FROM SB_PQ_ServiceBrokerLogs
WHERE LogDate >= @StartTime

PRINT CONVERT(nvarchar(100),@NumOfSessions) + ' unique sessions participated in execution'

SELECT *
FROM SB_PQ_ServiceBrokerLogs
WHERE LogDate >= @StartTime

GO

SELECT *
FROM sys.conversation_endpoints

/* -- cleanup closed conversations (SQL Server eventually does this automatically)
declare @q uniqueidentifier;
select top 1 @q = conversation_handle from sys.conversation_endpoints where state='CD';
while @@rowcount > 0
begin
end conversation @q with cleanup
select top 1 @q = conversation_handle from sys.conversation_endpoints where state='CD';
end
*/