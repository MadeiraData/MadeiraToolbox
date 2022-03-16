SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @IdentityColumns AS TABLE
(
 [Database] SYSNAME,
 [Schema]  SYSNAME,
 [Table]  SYSNAME,
 [Column]  SYSNAME,
 LastValue  SQL_VARIANT,
 TypeName SYSNAME,
 MaxValue  SQL_VARIANT,
 PercentUsed  DECIMAL(10, 2),
 TotalRows BIGINT NULL,
 TotalDataMB BIGINT NULL,
 ReferencedForeignKeys INT NULL
);

INSERT INTO  @IdentityColumns 
exec sp_MSforeachdb 'IF EXISTS (SELECT * FROM sys.databases WHERE name = ''?'' AND state_desc = ''ONLINE'' AND DATABASEPROPERTYEX([name], ''Updateability'') = ''READ_WRITE'')
BEGIN
USE [?];
SELECT DB_NAME() DatabaseName,
 OBJECT_SCHEMA_NAME(identity_columns.object_id) SchemaName, OBJECT_NAME(identity_columns.object_id) TableName
 , columns.name ColumnName, Last_Value LastValue, types.name TypeName, Calc1.MaxValue, Calc2.Percent_Used
 , TotalRows = (SELECT SUM(rows) FROM sys.partitions AS p WHERE p.index_id <= 1 AND p.object_id = identity_columns.object_id)
 , TotalDataMB = (SELECT SUM(used_page_count) FROM sys.dm_db_partition_stats AS p WHERE p.index_id <= 1 AND p.object_id = identity_columns.object_id)
 , ReferencedForeignKeys = (SELECT COUNT(*) FROM sys.foreign_keys AS fk WHERE identity_columns.object_id = fk.referenced_object_id)
FROM sys.identity_columns WITH (NOLOCK)
INNER JOIN sys.columns WITH (NOLOCK) ON columns.column_id = identity_columns.column_id AND columns.object_id = identity_columns.object_id
INNER JOIN sys.types ON types.system_type_id = columns.system_type_id
CROSS APPLY (SELECT MaxValue = CASE WHEN identity_columns.max_length = 1 THEN 256 ELSE POWER(2.0, identity_columns.max_length * 8 - 1) - 1 END) Calc1
CROSS APPLY (SELECT Percent_Used = CAST(CAST(Last_Value AS FLOAT) *100.0/MaxValue AS DECIMAL(10, 2))) Calc2
END'

SELECT *
FROM @IdentityColumns
WHERE PercentUsed > 80
--Uncomment below and customize to add exceptions:
-- AND DatabaseName NOT IN ('tempdb')
-- AND TableName NOT IN ('Test')