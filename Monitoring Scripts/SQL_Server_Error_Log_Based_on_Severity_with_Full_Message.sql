-- Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
-- Description: Get a single row per each high severity error from the SQL Server Error Log
DECLARE
  @SampleTime DATETIME = DATEADD(MINUTE,-30,SYSDATETIME())
, @MinimumSeverity INT = 17
, @MaximumSeverity INT = 25;

IF OBJECT_ID(N'tempdb..#errors') IS NOT NULL   
 DROP TABLE #errors;

SET NOCOUNT ON

-- Prepare severities list
DECLARE @Severities AS TABLE(Severity INT);
WHILE @MinimumSeverity <= @MaximumSeverity
BEGIN
	INSERT INTO @Severities
	VALUES(@MinimumSeverity);
	SET @MinimumSeverity = @MinimumSeverity + 1;
END

-- Prepare errors from error log
CREATE TABLE #errors
(
ID INT IDENTITY(1,1) NOT NULL,
LogDate DATETIME,
ProcessInfo NVARCHAR (10),
Error NVARCHAR(MAX)
);

INSERT INTO #errors(LogDate,ProcessInfo,Error)
EXEC master..XP_READERRORLOG 0, 1, NULL, NULL, @SampleTime, NULL, 'desc';

-- Use recursive query to construct full error messages
;WITH logs
AS
(
SELECT
head.ID AS RootID
,head.ID
,head.LogDate
,head.ProcessInfo
,CONVERT(nvarchar(max), head.Error) AS Error
, 1 AS Lvl
, sev.Severity
FROM #errors as head
INNER JOIN @Severities as sev
ON head.Error LIKE N'%Severity: ' + CONVERT(nvarchar,sev.Severity) + N'%'

UNION ALL

SELECT
head.RootID
,tail.ID
,tail.LogDate
,tail.ProcessInfo
,CONVERT(nvarchar(max), head.Error + N' ' + tail.Error)
,head.Lvl + 1
,head.Severity
FROM logs as head
INNER JOIN #errors as tail
ON head.ProcessInfo = tail.ProcessInfo
AND head.LogDate = tail.LogDate
AND head.Error <> tail.Error
AND head.ID > tail.ID
)
SELECT
	LogDate,
	ProcessInfo,
	Error
FROM
(
	SELECT *,
		RowRank = ROW_NUMBER() OVER (PARTITION BY RootID ORDER BY Lvl DESC)
	FROM logs
) AS d
WHERE RowRank = 1
OPTION (MAXRECURSION 0);

DROP TABLE #errors;