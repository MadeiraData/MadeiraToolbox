/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Date Created: 2021-04-22
Description:
	Multi-Line Table Function to generate periods for time series, based on a range and period type.

Supported period types:
	MI - Minute
	H - Hour
	D - Day
	W - Week
	M - Month
	Q - Quarter
	T - Trimester
	HY - Half-Year
	Y - Year
	Else, the function will attempt to convert the period type parameter to a DATETIME interval.

Example Usage:

	-- Hourly periods
	SELECT *
	FROM [dbo].[GeneratePeriods](CONVERT(DATE, GETDATE()-1), GETDATE(), 'H')
	
	-- Using a datetime interval
	SELECT *
	FROM [dbo].[GeneratePeriods](CONVERT(DATE, GETDATE()-1), GETDATE(), '00:20:00')
	
	-- Based on time range:
	DECLARE @FromDate DATETIME = '2020-01-01', @ToDate DATETIME = '2020-05-01'

	SELECT *
	FROM [dbo].[GeneratePeriods](@FromDate, @ToDate, 'D')
	
	-- Joining with data from a table:
	SELECT
		per.StartDate,
		TotalCount = COUNT(dat.datetimeColumn),
		TotalSum = SUM(dat.amount)
	FROM dbo.MyTable AS dat
	RIGHT JOIN [dbo].[GeneratePeriods](@FromDate, @ToDate, 'D') AS per
	ON dat.datetimeColumn >= per.StartDate
	AND dat.datetimeColumn < per.EndDate
	GROUP BY
		per.StartDate
*/
CREATE OR ALTER FUNCTION [dbo].[GeneratePeriods]
(
	@FromDate DATETIME,
	@EndDate DATETIME,
	@PeriodType VARCHAR(36)
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
			EndDate = 
				CASE @PeriodType
					WHEN 'MI' THEN
						DATEADD(minute,1,@FromDate)
					WHEN 'H' THEN
						DATEADD(hh,1,@FromDate)
					WHEN 'D' THEN
						DATEADD(dd,1,@FromDate)
					WHEN 'W' THEN
						DATEADD(ww,1,@FromDate)
					WHEN 'M' THEN
						DATEADD(mm,1,@FromDate)
					WHEN 'Q' THEN
						DATEADD(Q,1,@FromDate)
					WHEN 'T' THEN
						DATEADD(mm,4,@FromDate)
					WHEN 'HY' THEN
						DATEADD(mm,6,@FromDate)
					WHEN 'Y' THEN
						DATEADD(yyyy,1,@FromDate)
					ELSE
						TRY_CONVERT(datetime, @PeriodType) + @FromDate
				END
		
		UNION ALL
		
		SELECT
			PeriodNum = PeriodNum + 1,
			StartDate = EndDate,
			EndDate = 
				CASE @PeriodType
					WHEN 'MI' THEN
						DATEADD(minute,1,EndDate)
					WHEN 'H' THEN
						DATEADD(hh,1,EndDate)
					WHEN 'D' THEN
						DATEADD(dd,1,EndDate)
					WHEN 'W' THEN
						DATEADD(ww,1,EndDate)
					WHEN 'M' THEN
						DATEADD(mm,1,EndDate)
					WHEN 'Q' THEN
						DATEADD(Q,1,EndDate)
					WHEN 'T' THEN
						DATEADD(mm,4,EndDate)
					WHEN 'HY' THEN
						DATEADD(mm,6,EndDate)
					WHEN 'Y' THEN
						DATEADD(yyyy,1,EndDate)
					ELSE
						TRY_CONVERT(datetime, @PeriodType) + EndDate
				END
		FROM
			Periods
		WHERE
			EndDate < @EndDate
	)
	INSERT INTO @T(PeriodNum, StartDate, EndDate)
	SELECT PeriodNum, StartDate, EndDate
	FROM Periods
	OPTION (MAXRECURSION 0);

	RETURN;
END