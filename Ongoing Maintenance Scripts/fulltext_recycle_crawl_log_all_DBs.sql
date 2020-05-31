/*
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Date: 2020-05-31
Description:
This script iterates through every populated full-text catalog in every accessible database and recycles its crawl log.
Recommended to run this script as a weekly/monthly scheduled maintenance job.

More info:
https://www.sqlskills.com/blogs/jonathan/recycle-fulltext-catalog-log-files/
*/
SET NOCOUNT, XACT_ABORT, ARITHABORT ON;
IF CONVERT(bit, SERVERPROPERTY('IsFullTextInstalled')) = 1
BEGIN
	DECLARE @FTCatalogs AS TABLE
	(
		DBName SYSNAME,
		CatName SYSNAME
	);

	INSERT INTO @FTCatalogs
	EXEC sys.sp_MSforeachdb N'IF EXISTS (SELECT * FROM sys.databases WHERE name = ''?'' AND is_fulltext_enabled = 1 state_desc = ''ONLINE'' AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
BEGIN
	USE [?];
	SELECT DB_NAME(), [name] 
	FROM sys.fulltext_catalogs
	WHERE [path] IS NOT NULL 
	AND FULLTEXTCATALOGPROPERTY ([name] ,''ItemCount'') > 0
END'
	
	DECLARE @CurrDB SYSNAME, @CurrFTCat SYSNAME

	DECLARE FTCats CURSOR
	LOCAL FAST_FORWARD
	FOR
	SELECT DBName, CatName FROM @FTCatalogs

	OPEN FTCats
	FETCH NEXT FROM FTCats INTO @CurrDB, @CurrFTCat

	WHILE @@FETCH_STATUS = 0
	BEGIN
		DECLARE @sp_recycle_ft NVARCHAR(1000)
		SET @sp_recycle_ft = QUOTENAME(@CurrDB) + '.sys.sp_fulltext_recycle_crawl_log'

		RAISERROR(N'Cycling FT log [%s]:[%s]...',0,1,@CurrDB,@CurrFTCat) WITH NOWAIT;
		EXEC @sp_recycle_ft @ftcat = @CurrFTCat;

		FETCH NEXT FROM FTCats INTO @CurrDB, @CurrFTCat
	END

	CLOSE FTCats;
	DEALLOCATE FTCats;
END
ELSE
	RAISERROR(N'Full-Text is not installed on %s.',0,1,@@SERVERNAME);

	