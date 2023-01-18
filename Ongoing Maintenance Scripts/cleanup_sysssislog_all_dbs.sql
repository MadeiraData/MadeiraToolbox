/*
Cleanup dbo.sysssislog in all databases
=======================================
Author: Eitan Blumin | https://madeiradata.com
Date: 2023-01-18
*/
DECLARE
	@DaysBack int = 90,
	@ChunkSize int = 10000


SET NOCOUNT ON;

DECLARE @CurrDB sysname, @spExecuteSQL nvarchar(256)

DECLARE DBs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name]
FROM sys.databases
WHERE state = 0
AND HAS_DBACCESS([name]) = 1
AND DATABASEPROPERTYEX([name], 'Updateability') = 'READ_WRITE'
AND OBJECT_ID(QUOTENAME([name]) + N'.[dbo].[sysssislog]') IS NOT NULL

OPEN DBs;

WHILE 1=1
BEGIN
	FETCH NEXT FROM DBs INTO @CurrDB;
	IF @@FETCH_STATUS <> 0 BREAK;

	RAISERROR(N'%s',0,1,@CurrDB) WITH NOWAIT;

	SET @spExecuteSQL = QUOTENAME(@CurrDB) + N'..sp_executesql';

	WHILE 1=1
	BEGIN
		EXEC @spExecuteSQL N'
		DELETE TOP (@ChunkSize) T
		FROM [dbo].[sysssislog] AS T WITH(READPAST)
		WHERE starttime < DATEADD(dd, -@DaysBack, GETDATE())'
		, N'@DaysBack int, @ChunkSize int', @DaysBack, @ChunkSize
		WITH RECOMPILE;

		IF @@ROWCOUNT = 0 BREAK;

		WAITFOR DELAY '00:00:00.5'
	END
END

CLOSE DBs;
DEALLOCATE DBs;