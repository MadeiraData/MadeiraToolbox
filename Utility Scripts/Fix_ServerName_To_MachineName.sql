/*
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Date: November, 2018
Description:
Update @@SERVERNAME to Actual Machine Name.
Run as-is. The script is idempotent and requires no parameters.
SQL Service restart may be required in order to apply changes.

More info:
https://eitanblumin.com/2018/11/06/how-to-update-servername-to-actual-machine-name/
*/
DECLARE @MachineName NVARCHAR(60)
SET @MachineName = CONVERT(nvarchar,SERVERPROPERTY('ServerName'));

IF @MachineName IS NULL
BEGIN
	PRINT 'Could not retrieve machine name using SERVERPROPERTY!';
	GOTO Quit;
END

DECLARE @CurrSrv VARCHAR(MAX)
SELECT @CurrSrv = name FROM sys.servers WHERE server_id = 0;

IF @CurrSrv = @MachineName
BEGIN
	PRINT 'Server name already matches actual machine name.'
	GOTO Quit;
END

IF @CurrSrv IS NOT NULL
BEGIN
	PRINT 'Dropping local server name ' + @CurrSrv
	EXEC sp_dropserver @CurrSrv
END

IF EXISTS (SELECT 1 FROM sys.servers WHERE server_id <> 0 AND [name] = @MachineName)
BEGIN
	PRINT 'The local server is incorrectly configured as a remote server. Dropping server name ' + @MachineName
	EXEC sp_dropserver @MachineName
END

PRINT 'Creating local server name ' + @MachineName
EXEC sp_addserver @MachineName, local

Quit:

IF EXISTS (SELECT [name] FROM sys.servers WHERE server_id = 0 AND [name] <> @@SERVERNAME)
	OR (@MachineName IS NOT NULL AND (@@SERVERNAME <> CONVERT(NVARCHAR,SERVERPROPERTY('ServerName'))))
	PRINT 'Your server name was changed. Please restart the SQL Server service to apply changes.';