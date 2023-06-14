SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

DECLARE @results AS table (account_name sysname null, err_desc nvarchar(4000) NULL, err_date datetime NOT NULL);

INSERT INTO @results (account_name, err_desc, err_date)
SELECT 
acc.name,
el.description as err_desc,
el.log_date as err_date
from msdb.dbo.sysmail_event_log el
left join msdb.dbo.sysmail_account acc
ON acc.account_id = el.account_id
WHERE el.event_type='error'
and el.log_date > DATEADD(HOUR,-2,GETDATE())

if @@ROWCOUNT > 10
BEGIN
    SELECT msg = ISNULL(N'DBMail Account ' + QUOTENAME(account_name) + N': ',N'') + 'There have been ' + CONVERT(nvarchar(MAX), COUNT(*))
    + N' DBMail errors since ' + CONVERT(varchar(19),min(err_date),121)
	+ ISNULL(N'. Example: ' + MIN(err_desc), N'')
    , errcount = COUNT(*)
    FROM @results
    GROUP BY account_name
END
ELSE
BEGIN
    SELECT msg = ISNULL(N'DBMail Account ' + QUOTENAME(account_name) + N': ',N'')
    + CONVERT(varchar(19),MIN(err_date),121) + ISNULL(N' - ' + CONVERT(varchar(19),NULLIF(MAX(err_date),MIN(err_date)),121), N'')
    + ISNULL(N' - ' + err_desc, N'')
    , errcount = COUNT(*)
    FROM @results
    GROUP BY account_name, err_desc
END