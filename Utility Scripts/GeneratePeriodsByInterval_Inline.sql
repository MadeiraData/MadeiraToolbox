/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Date Created: 2021-04-22
Description:
CTE-based Inline Table Function to generate periods for time series, based on a datetime range and interval.

Example Usage:

SELECT *
FROM [dbo].[GeneratePeriodsByInterval](CONVERT(DATE, GETDATE()-1), GETDATE(), '00:10:00')
OPTION (MAXRECURSION 0)
*/
CREATE OR ALTER FUNCTION [dbo].[GeneratePeriodsByInterval]
(
	@FromDate DATETIME,
	@EndDate DATETIME,
	@Interval DATETIME
)
RETURNS TABLE
WITH SCHEMABINDING
AS RETURN
(
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
	SELECT PeriodNum, StartDate, EndDate
	FROM Periods
)
GO