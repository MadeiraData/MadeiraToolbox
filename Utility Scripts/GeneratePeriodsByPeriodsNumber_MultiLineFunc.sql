/*
Author: Eitan Blumin (t: @EitanBlumin | b: https://eitanblumin.com)
Date Created: 2013-09-01
Last Update: 2020-07-28
Description:
	Multi-Line Table Function to generate periods for time series, based on an end date, period type, and number of periods back.

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

Example Usage:

	-- Based on number of periods:
	SELECT *
	FROM [dbo].[GeneratePeriods](GETDATE(), 'H', 14)
	
	-- Based on time range:
	DECLARE @FromDate DATETIME = '2020-01-01', @ToDate DATETIME = '2020-05-01'

	SELECT *
	FROM [dbo].[GeneratePeriods](@ToDate, 'D', DATEDIFF(dd, @FromDate, @ToDate) - 1)
	
	-- Joining with data from a table:
	SELECT
		per.StartDate,
		TotalCount = COUNT(dat.datetimeColumn),
		TotalSum = SUM(dat.amount)
	FROM dbo.MyTable AS dat
	RIGHT JOIN [dbo].[GeneratePeriods](GETDATE(), 'D', 14) AS per
	ON dat.datetimeColumn >= per.StartDate
	AND dat.datetimeColumn < per.EndDate
	GROUP BY
		per.StartDate
*/
CREATE OR ALTER FUNCTION [dbo].[GeneratePeriods]
(
	@EndDate DATETIME,
	@PeriodType CHAR(2),
	@NumOfPeriodsBack INT
)
RETURNS @T TABLE(
		PeriodNum INT,
		StartDate DATETIME,
		EndDate DATETIME)
WITH SCHEMABINDING
AS
BEGIN
	WITH FirstPeriod
	AS
	(
		SELECT 
			StartDate =
				CASE @PeriodType
					WHEN 'MI' THEN
						DATEADD(minute,DATEDIFF(minute,'2000-01-01',@EndDate),'2000-01-01')
					WHEN 'H' THEN
						DATEADD(hh,DATEDIFF(hh,'2000-01-01',@EndDate),'2000-01-01')
					WHEN 'D' THEN
						DATEADD(dd,DATEDIFF(dd,0,@EndDate),0)
					WHEN 'W' THEN
						DATEADD(day,-1,DATEADD(ww,DATEDIFF(ww,0,@EndDate),0))
					WHEN 'M' THEN
						DATEADD(mm,DATEDIFF(mm,0,@EndDate),0)
					WHEN 'Q' THEN
						DATEADD(Q,DATEDIFF(Q,0,@EndDate),0)
					WHEN 'T' THEN
						CASE
							WHEN DATEPART(mm,@EndDate) >= 9 THEN DATEADD(mm,8,DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0))
							WHEN DATEPART(mm,@EndDate) >= 5 THEN DATEADD(mm,4,DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0))
							ELSE DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0)
						END
					WHEN 'HY' THEN
						CASE WHEN DATEPART(mm,@EndDate) >= 7 THEN DATEADD(mm,6,DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0))
						ELSE DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0)
						END
					WHEN 'Y' THEN
						DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0)
				END,
			EndDate =
				CASE @PeriodType
					WHEN 'MI' THEN
						DATEADD(minute,DATEDIFF(minute,'2000-01-01',@EndDate)+1,'2000-01-01')
					WHEN 'H' THEN
						DATEADD(hh,DATEDIFF(hh,'2000-01-01',@EndDate)+1,'2000-01-01')
					WHEN 'D' THEN
						DATEADD(dd,DATEDIFF(dd,0,@EndDate)+1,0)
					WHEN 'W' THEN
						DATEADD(day,-1,DATEADD(ww,DATEDIFF(ww,0,@EndDate)+1,0))
					WHEN 'M' THEN
						DATEADD(mm,DATEDIFF(mm,0,@EndDate)+1,0)
					WHEN 'Q' THEN
						DATEADD(Q,DATEDIFF(Q,0,@EndDate)+1,0)
					WHEN 'T' THEN
						DATEADD(mm,4,
							CASE
								WHEN DATEPART(mm,@EndDate) >= 9 THEN DATEADD(mm,8,DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0))
								WHEN DATEPART(mm,@EndDate) >= 5 THEN DATEADD(mm,4,DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0))
								ELSE DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0)
							END)
					WHEN 'HY' THEN
						CASE WHEN DATEPART(mm,@EndDate) >= 7 THEN DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate)+1,0)
						ELSE DATEADD(mm,6,DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate),0))
						END
					WHEN 'Y' THEN
						DATEADD(yyyy,DATEDIFF(yyyy,0,@EndDate)+1,0)
				END
	), Periods
	AS
	(
		SELECT 
			PeriodNum = 1,
			StartDate,
			EndDate
		FROM FirstPeriod
		
		UNION ALL
		
		SELECT
			PeriodNum = PeriodNum + 1,
			StartDate = 
				CASE @PeriodType
					WHEN 'MI' THEN
						DATEADD(minute,-1,StartDate)
					WHEN 'H' THEN
						DATEADD(hh,-1,StartDate)
					WHEN 'D' THEN
						DATEADD(dd,-1,StartDate)
					WHEN 'W' THEN
						DATEADD(ww,-1,StartDate)
					WHEN 'M' THEN
						DATEADD(mm,-1,StartDate)
					WHEN 'Q' THEN
						DATEADD(Q,-1,StartDate)
					WHEN 'T' THEN
						DATEADD(mm,-4,StartDate)
					WHEN 'HY' THEN
						DATEADD(mm,-6,StartDate)
					WHEN 'Y' THEN
						DATEADD(yyyy,-1,StartDate)
				END,
			EndDate = StartDate
		FROM
			Periods
		WHERE
			PeriodNum < @NumOfPeriodsBack
	)
	INSERT INTO @T(PeriodNum, StartDate, EndDate)
	SELECT PeriodNum, StartDate, EndDate
	FROM Periods
	OPTION (MAXRECURSION 0);

	RETURN;
END