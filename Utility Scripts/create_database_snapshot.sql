
DECLARE @CurrDB SYSNAME = 'MyDBName'

DECLARE @CMD NVARCHAR(MAX);

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