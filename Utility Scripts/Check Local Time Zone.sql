SET NOCOUNT, ARITHABORT, XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @LocalTimeZone VARCHAR(50) = NULL
DECLARE @sqlmajorver INT
SET @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)

IF CONVERT(varchar(100), SERVERPROPERTY('Edition')) = 'SQL Azure'
BEGIN
	DECLARE @cmd nvarchar(MAX) = N'SET @LocalTimeZone = CURRENT_TIMEZONE_ID()'
	EXEC sp_executesql @cmd, N'@LocalTimeZone varchar(50) output', @LocalTimeZone OUTPUT
END
ELSE IF @sqlmajorver >= 13
BEGIN
	EXEC master.dbo.xp_regread 'HKEY_LOCAL_MACHINE',
	'SYSTEM\CurrentControlSet\Control\TimeZoneInformation',
	'TimeZoneKeyName',@LocalTimeZone OUT
END

IF @LocalTimeZone IS NOT NULL AND CONVERT(int, SERVERPROPERTY('EngineEdition')) <> 5
BEGIN
	RAISERROR(N'Checking based on local time zone "%s"',0,1, @LocalTimeZone) WITH NOWAIT;
	
	DECLARE @CheckDate1 datetime, @CheckDate2 datetime;
	DECLARE @UtcDiff1 datetime, @UtcDiff2 datetime;

	SET @CheckDate1 = '2000-01-01'
	SET @CheckDate2 = '2000-06-01'

	SELECT
	  @UtcDiff1 = @CheckDate1 - CONVERT(datetime, @CheckDate1 AT TIME ZONE @LocalTimeZone AT TIME ZONE 'UTC')
	, @UtcDiff2 = @CheckDate2 - CONVERT(datetime, @CheckDate2 AT TIME ZONE @LocalTimeZone AT TIME ZONE 'UTC')
	
	IF @UtcDiff1 <> @UtcDiff2
	BEGIN
		PRINT N'DST Difference Found'
	END
	ELSE
	BEGIN
		PRINT N'No DST Difference Found'
	END
	
END
ELSE
BEGIN
	PRINT N'Time zone check is not supported on this server.'
END

SELECT
  @UtcDiff1 AS UTC_Offset_January
, @UtcDiff2 AS UTC_Offset_June
, @LocalTimeZone AS LocalTimeZone
