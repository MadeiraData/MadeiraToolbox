/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Date Created: 2021-04-22
Description:
Multi-Line Table Function to generate periods for time series, based on a datetime range and interval.

Example Usage:

SELECT *
FROM [dbo].[GeneratePeriodsByInterval](CONVERT(DATE, GETDATE()-1), GETDATE(), '00:10:00')

*/
CREATE OR ALTER FUNCTION [dbo].[GeneratePeriodsByInterval]
(
	@FromDate DATETIME,
	@EndDate DATETIME,
	@Interval DATETIME
)
RETURNS @T TABLE(PeriodNum INT, StartDate DATETIME, EndDate DATETIME)
WITH SCHEMABINDING
AS
BEGIN
	WITH Periods
	AS
	(
		SELECT 
			PeriodNum = 1,
			StartDate = @FromDate,
			EndDate = @FromDate + @Interval
		
		UNION ALL
		
		SELECT
			PeriodNum = PeriodNum + 1,
			StartDate = EndDate,
			EndDate = EndDate + @Interval
		FROM
			Periods
		WHERE
			EndDate < @EndDate - @Interval
	)
	INSERT INTO @T(PeriodNum, StartDate, EndDate)
	SELECT PeriodNum, StartDate, EndDate
	FROM Periods
	OPTION (MAXRECURSION 0);

	RETURN;
END
GO