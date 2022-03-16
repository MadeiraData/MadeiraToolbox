SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @Sequences AS TABLE
(
 [Database] SYSNAME,
 [Schema] SYSNAME,
 [Sequence] SYSNAME,
 LastValue SQL_VARIANT,
 MaxValue SQL_VARIANT,
 PercentUsed DECIMAL(10, 2)
);

INSERT INTO @Sequences
exec sp_MSforeachdb 'IF EXISTS (SELECT * FROM sys.databases WHERE name = ''?'' AND state_desc = ''ONLINE'' AND DATABASEPROPERTYEX([name],''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
SELECT DB_NAME() AS DatabaseName,
OBJECT_SCHEMA_NAME(sequences.object_id) SchemaName, 
OBJECT_NAME(sequences.object_id) SequenceName, 
sequences.current_value LastValue, 
MaxValue = sequences.maximum_value, 
CAST(CAST(sequences.current_value AS FLOAT)/CAST(sequences.maximum_value AS FLOAT) * 100.0 AS DECIMAL(10, 2))
FROM sys.sequences WITH (NOLOCK)
WHERE is_cycling = 0 AND sequences.current_value IS NOT NULL
END'

SELECT *
FROM @Sequences
WHERE PercentUsed > 80