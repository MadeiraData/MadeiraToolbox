/*
Check for DBMail errors
=======================
Author: Or Issar, Madeira Data Solutions (https://madeiradata.com)
Date: 2023-01-15
*/
DECLARE
	 @HoursBackToCheck	int = 2
	,@ForceDetails		bit = 0

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

CREATE TABLE #tmp(account_name sysname, err_desc nvarchar(MAX) NULL, err_date datetime);

INSERT INTO #tmp
SELECT
ISNULL(acc.name, '(null)') as account_name,
el.[description] as err_desc,
el.log_date as err_date
FROM msdb.dbo.sysmail_event_log el
LEFT JOIN msdb.dbo.sysmail_account acc ON acc.account_id = el.account_id
WHERE el.event_type='error'
AND el.log_date > DATEADD(HOUR,-@HoursBackToCheck,GETDATE())

IF @@ROWCOUNT > 10 AND @ForceDetails = 0
	SELECT CONCAT(
	'There have been ', count(*),
	' errors for DBMail account ', account_name, N' since ', CONVERT(varchar(25),min(err_date),121)) as error_description,
	count(*) AS errCount
	FROM #tmp
	GROUP BY account_name
	ORDER BY count(*) DESC, max(err_date) DESC
ELSE
	SELECT CONCAT(
	'Account Name: ',ISNULL(account_name,'null'),
	' encountered error on ', CONVERT(nvarchar(25),err_date,121) ,' - ', err_desc) as error_description,
	1 as errCount
	FROM #tmp
	ORDER BY err_date DESC;

DROP TABLE #tmp