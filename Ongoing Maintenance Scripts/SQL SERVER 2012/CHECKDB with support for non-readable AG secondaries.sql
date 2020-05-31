/*
Author: Eitan Blumin (t: @EitanBlumin | b: eitanblumin.com)
Date: March, 2020
Description:
Run DBCC CHECKDB on all databases which are either standalone, or SECONDARY in AG. 
Supports non-readable secondaries by creating DB snapshots.
*/
DECLARE @CurrDB SYSNAME, @IsInAG BIT, @CMD NVARCHAR(MAX);

-- Find all databases which are either standalone, or SECONDARY in AG
DECLARE dbs CURSOR
LOCAL FAST_FORWARD
FOR
SELECT [name], CASE WHEN replica_id IS NULL THEN 0 ELSE 1 END
FROM sys.databases
WHERE database_id <> 2
AND state_desc = 'ONLINE'
AND source_database_id IS NULL
AND (replica_id IS NULL
OR replica_id IN (
	SELECT ars.replica_id
	FROM sys.dm_hadr_availability_replica_states AS ars
	INNER JOIN sys.availability_groups AS ag
	ON ars.group_id = ag.group_id
	WHERE ars.is_local = 1
	AND ars.role_desc = 'SECONDARY'
	)
)

OPEN dbs

FETCH NEXT FROM dbs INTO @CurrDB, @IsInAG

WHILE @@FETCH_STATUS = 0
BEGIN
	IF @IsInAG = 1
	BEGIN
		SET @CMD = NULL;

		SELECT @CMD = ISNULL(@CMD + N',
		', N'') + N'(NAME = ' + QUOTENAME(name) + N'
		, FILENAME = ' + QUOTENAME(LEFT(physical_name, LEN(physical_name) - CHARINDEX('\', REVERSE(physical_name)) + 1)
		+ DB_NAME(database_id) + '_' + REPLACE(NEWID(),'-','') + '.ss', N'''')
		+ N')'
		FROM sys.master_files
		WHERE type <> 1
		AND database_id = DB_ID(@CurrDB)

		SELECT
		@CMD = N'CREATE DATABASE ' + QUOTENAME(SnapshotName) + N'
		ON ' + @CMD + N'
		AS SNAPSHOT OF ' + QUOTENAME(@CurrDB)
		, @CurrDB = SnapshotName
		FROM
		(VALUES (@CurrDB + '_snapshot_' + CONVERT(nvarchar,ABS(CHECKSUM(NEWID()))))) AS v(SnapshotName)

		PRINT @CMD
		EXEC(@CMD)
	END

	SET @CMD = N'DBCC CHECKDB(' + QUOTENAME(@CurrDB) + N') WITH NO_INFOMSGS, PHYSICAL_ONLY;'
	PRINT @CMD
	EXEC(@CMD)

	IF @IsInAG = 1
	BEGIN
		SET @CMD = N'DROP DATABASE ' + QUOTENAME(@CurrDB)
		PRINT @CMD;
		EXEC(@CMD)
	END

	FETCH NEXT FROM dbs INTO @CurrDB, @IsInAG
END

CLOSE dbs
DEALLOCATE dbs