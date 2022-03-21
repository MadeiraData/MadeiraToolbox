DECLARE @PrintOnly bit = 1 -- Set to 0 to actually execute remediation

SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @CurrCmd nvarchar(max);

SET @CurrCmd = N'------ ' + CONVERT(nvarchar(max), SERVERPROPERTY('ServerName')) + N' ------'

SELECT @CurrCmd = @CurrCmd + CHAR(13) + CHAR(10)
+ N'ALTER DATABASE ' + QUOTENAME([name]) COLLATE database_default + N' SET PAGE_VERIFY CHECKSUM WITH NO_WAIT; -- previously: ' + page_verify_option_desc COLLATE database_default
FROM sys.databases
WHERE page_verify_option_desc <> 'CHECKSUM';

PRINT @CurrCmd
IF @PrintOnly = 0 EXEC(@CurrCmd)