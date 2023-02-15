/*
Check for CDC Errors
====================
Author: Eitan Blumin
Date: 2023-02-15
*/
DECLARE
	 @FilterByDBName		sysname		= NULL		-- Optionally filter by a specific database name. Leave NULL to check all accessible CDC-enabled databases.
	,@MinutesBackToCheck	int			= 60		-- Set how many minutes back to check for errors


/***** NO NEED TO CHANGE ANYTHING BELOW THIS LINE *****/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;

DECLARE @CurrDB sysname, @spExecuteSQL nvarchar(500);
DECLARE @Results AS TABLE ([database_id] int, entry_time datetime, phase_number tinyint, errors nvarchar(max))

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE is_cdc_enabled = 1
AND HAS_DBACCESS([name]) = 1
AND (@FilterByDBName IS NULL OR @FilterByDBName = [name])

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	SET @spExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql'

	INSERT INTO @Results
	EXEC @spExecuteSQL N'WITH err
AS
(
SELECT phase_number, entry_time
FROM sys.dm_cdc_errors
WHERE error_number NOT IN (22859)
AND entry_time >= DATEADD(MINUTE, -@MinutesBackToCheck, GETDATE())
GROUP BY phase_number, entry_time
)
SELECT DB_ID()
, err.entry_time
, err.phase_number
, errors =
STUFF((
SELECT CHAR(10) + N''Error '' + CONVERT(nvarchar(MAX), errDetail.error_number) + N'', Severity '' + CONVERT(nvarchar(MAX), errDetail.error_severity) + N'', State '' + CONVERT(nvarchar(MAX), errDetail.error_state) + N'': '' + errDetail.error_message
FROM sys.dm_cdc_errors AS errDetail
WHERE errDetail.error_number NOT IN (22859)
AND err.entry_time = errDetail.entry_time
AND err.phase_number = errDetail.phase_number
FOR XML PATH(''''), TYPE
).value(''(text())[1]'', ''nvarchar(max)''), 1, 1, N'''')
FROM err'
	, N'@MinutesBackToCheck int', @MinutesBackToCheck WITH RECOMPILE;

	RAISERROR(N'%s: %d error(s)',0,1,@CurrDB,@@ROWCOUNT) WITH NOWAIT;

END

CLOSE DBs;
DEALLOCATE DBs;


SELECT
Msg = N'In Server: ' + @@SERVERNAME + N', Database: ' + QUOTENAME(DB_NAME([database_id])) + N', Time: '
+ CONVERT(nvarchar(19), MIN(err.entry_time), 121)
+ ISNULL(N' - ' + CONVERT(nvarchar(19), NULLIF(MAX(err.entry_time), MIN(err.entry_time)), 121), N'')
+ N' ' + QUOTENAME(phases.phase_desc)
+ N' ' + REPLACE(errors, CHAR(10), N'<br/>')
, errCount = COUNT(*)
from @Results AS err
INNER JOIN
(VALUES
 (1,'Reading configuration')
,(2,'First scan, building hash table')
,(3,'Second scan')
,(4,'Second scan')
,(5,'Second scan')
,(6,'Schema versioning')
,(7,'Last scan')
,(8,'Done')
) AS phases(num,phase_desc)
ON err.phase_number = phases.num
GROUP BY [database_id], phases.phase_desc, errors
ORDER BY MAX(err.entry_time) DESC
OPTION (RECOMPILE);
