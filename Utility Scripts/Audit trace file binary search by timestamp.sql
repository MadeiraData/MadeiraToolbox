/*
Audit Trace File Binary Search
====================================
Author: Eitan Blumin | https://eitanblumin.com | https://madeiradata.com
Date: 2021-11-16
Description:
	Assuming that you have a folder with multiple audit trace files,
	and you want to find what happened around a certain point in time,
	but you don't know which trace file exactly is relevant to it.

	This script reads the list of all trace files in the folder,
	and performs a "binary search" on them (AKA "Lion in the Desert")
	to find which trace file should contain the relevant timestamp.
	It is based on the following algorithm: https://wiki.c2.com/?BinarySearchCodeOnly

	Don't forget to change the parameters as needed.

Note:
	This script does NOT require xp_cmdshell or CLR or Powershell.
	It is pure T-SQL only.
*/
SET NOEXEC OFF;
DECLARE
	 @AuditLogsPath		nvarchar(4000)	= CONVERT(nvarchar(4000), SERVERPROPERTY('InstanceDefaultDataPath')) -- C2 audit trace files are saved in the default data folder
	,@TargetTimeStamp	datetime	= /*'2021-11-15 11:14:26.163' --*/ (SELECT MAX(modify_date) FROM sys.server_principals) -- specify the target timestamp here
	,@BufferMinutesBefore	int		= 5
	,@BufferMinutesAfter	int		= 10
	,@Verbose		bit		= 0			-- change this to 1 to track the progress of the binary search

SET NOCOUNT, QUOTED_IDENTIFIER ON;
IF OBJECT_ID('tempdb..#FilesList') IS NOT NULL DROP TABLE #FilesList;
CREATE TABLE #FilesList (ID int NULL, itempath nvarchar(4000), depth int, isfile tinyint, INDEX IX_ID CLUSTERED (ID));

INSERT INTO #FilesList (itempath, depth, isfile)
exec xp_dirtree @AuditLogsPath, 0, 1

-- Remove all subdirectories and non-trace files
DELETE FROM #FilesList WHERE isfile = 0 OR itempath NOT LIKE '%.trc' OR depth > 1;

IF NOT EXISTS (SELECT * FROM #FilesList)
BEGIN
	RAISERROR(N'No trace files found in folder: %s', 16, 1, @AuditLogsPath);
	SET NOEXEC ON;
END

-- Numerize the table alphabetically
UPDATE t
	SET ID = RowRank
FROM (
	SELECT *, ROW_NUMBER() OVER (ORDER BY itempath ASC) AS RowRank
	FROM #FilesList
	) AS t;

DECLARE @LowerFileID int, @UpperFileID int, @MiddleFileID int, @LowerTimestamp datetime, @UpperTimestamp datetime, @MiddleTimestamp datetime
DECLARE @MiddleFilePath nvarchar(4000)

-- Get lower and upper bounds
SELECT @LowerFileID = MIN(ID), @UpperFileID = MAX(ID)
FROM #FilesList

IF RIGHT(@AuditLogsPath, 1) <> N'\' SET @AuditLogsPath = @AuditLogsPath + N'\';

-- Get lower bound value
SELECT @MiddleFilePath = @AuditLogsPath + itempath
FROM #FilesList
WHERE ID = @LowerFileID;

SELECT TOP (2) @LowerTimestamp = StartTime
FROM sys.fn_trace_gettable(@MiddleFilePath, 1) AS t

-- Get upper bound value
SELECT @MiddleFilePath = @AuditLogsPath + itempath
FROM #FilesList
WHERE ID = @UpperFileID;

SELECT TOP (2) @UpperTimestamp = StartTime
FROM sys.fn_trace_gettable(@MiddleFilePath, 1) AS t

IF @TargetTimeStamp >= @LowerTimestamp
BEGIN
	
	-- Following algorithm is a binary search.
	-- loop invariant: expects and maintains Array[lower] < value < Array[upper]
	-- exits loop when upper = lower + 1.
	-- exits function when Array[middle] = value.
	-- needs separate test for Array[lower]=value, Array[upper]=value

	WHILE @UpperFileID > @LowerFileID + 1
	BEGIN
		SET @MiddleFileID = (@UpperFileID + @LowerFileID) / 2;
		
		-- Get middle value
		SELECT @MiddleFilePath = @AuditLogsPath + itempath
		FROM #FilesList
		WHERE ID = @MiddleFileID;

		SELECT TOP (2) @MiddleTimestamp = StartTime
		FROM sys.fn_trace_gettable(@MiddleFilePath, 1) AS t;

		IF @TargetTimeStamp >= @MiddleTimestamp AND @MiddleFileID IN(@LowerFileID, @UpperFileID)
		BEGIN
			IF @Verbose = 1 RAISERROR(N'Found upper or lower bound -> @MiddleFileID: %d - %s',0,1,@MiddleFileID,@MiddleFilePath) WITH NOWAIT;
			BREAK;
		END
		ELSE IF @MiddleTimestamp < @TargetTimeStamp
		BEGIN
			SET @LowerFileID = @MiddleFileID;
			IF @Verbose = 1 RAISERROR(N'@MiddleTimestamp < @TargetTimeStamp -> @LowerFileID: %d - %s',0,1,@MiddleFileID,@MiddleFilePath) WITH NOWAIT;
		END
		ELSE
		BEGIN
			SET @UpperFileID = @MiddleFileID;
			IF @Verbose = 1 RAISERROR(N'@MiddleTimestamp >= @TargetTimeStamp -> @UpperFileID: %d - %s',0,1,@MiddleFileID,@MiddleFilePath) WITH NOWAIT;
		END
	END
	
	RAISERROR(N'Found possible target file path: %s',0,1,@MiddleFilePath);

END
ELSE
	RAISERROR(N'The specified target timestamp is not in range of available trace files.',15,1)

SELECT @MiddleFilePath AS FoundTargetFilePath, MIN(StartTime) AS MinStartTime, @TargetTimeStamp AS TargetTimestamp, MAX(StartTime) AS MaxStartTime
FROM sys.fn_trace_gettable(@MiddleFilePath, 1) AS t
WHERE @MiddleFilePath IS NOT NULL;

SELECT *
FROM sys.fn_trace_gettable(@MiddleFilePath, 1) AS t
WHERE @MiddleFilePath IS NOT NULL
AND StartTime BETWEEN DATEADD(minute, -@BufferMinutesBefore, @TargetTimeStamp) AND DATEADD(minute, @BufferMinutesAfter, @TargetTimeStamp)
AND ApplicationName NOT LIKE 'SolarWinds%'
AND ApplicationName NOT LIKE 'SentryOne%'
AND ApplicationName NOT IN ('Microsoft Dynamics NAV Service', 'Microsoft SQL Server IaaS Agent Query Service', 'Mashup Engine', N'Microsoft® Mashup Runtime')
