
DECLARE @DB sysname, @SpExecuteSQL nvarchar(4000), @CMD nvarchar(max);

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name],'Updateability') = 'READ_WRITE'

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @DB;
	IF @@FETCH_STATUS <> 0 BREAK;
	
	SET @SpExecuteSQL = QUOTENAME(@DB) + N'..sp_executesql'

	SET @CMD = N'SET @CMD = N'''';
	SELECT @CMD = @CMD + '';
DENY ALTER ANY DATABASE DDL TRIGGER TO '' + QUOTENAME([name])
	FROM sys.database_principals
	WHERE type = ''S'' AND [name] NOT LIKE ''##%##''
	AND principal_id > 4
	AND IS_SRVROLEMEMBER(''sysadmin'',[name]) <> 1'

	EXEC @SpExecuteSQL @CMD, N'@CMD nvarchar(max) OUTPUT', @CMD OUTPUT

	IF @CMD <> ''
	BEGIN
		RAISERROR(N'Database: %s. %s',0,1,@DB,@CMD) WITH NOWAIT;
		EXEC @SpExecuteSQL @CMD;
	END
END

CLOSE DBs;
DEALLOCATE DBs;
