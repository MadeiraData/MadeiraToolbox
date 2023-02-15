SELECT 
  name, 
  CASE is_cdc_enabled 
    WHEN 0 THEN 'CDC not enabled'
    WHEN 1 Then 'CDC enabled'
    ELSE 'Invalid value'
    END AS CDCstate
FROM sys.databases

GO

-- Enable for current database
IF EXISTS (SELECT * FROM sys.databases WHERE database_id = DB_ID() AND is_cdc_enabled = 0)
BEGIN
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	EXEC sys.sp_cdc_enable_db;
END

GO

SELECT 
  name, 
  CASE is_cdc_enabled 
    WHEN 0 THEN 'CDC not enabled'
    WHEN 1 Then 'CDC enabled'
    ELSE 'Invalid value'
    END AS CDCstate
FROM sys.databases

GO

-- Enable for all tables in current database

SELECT
  SCHEMA_NAME(schema_id) AS [schema], [name], is_tracked_by_cdc
, RemediationCmd = N'EXEC sys.sp_cdc_enable_table @source_schema = N' + QUOTENAME(SCHEMA_NAME(schema_id), '''') + N', @source_name = N' + QUOTENAME([name], '''') + N', @role_name = NULL, @filegroup_name = NULL, @supports_net_changes = 0;'
FROM sys.tables AS t
WHERE is_ms_shipped = 0 -- non-system
AND t.is_tracked_by_cdc = 0 -- not yet tracked by CDC
AND EXISTS (SELECT * FROM sys.indexes AS ix WHERE ix.is_primary_key = 1 AND ix.object_id = t.object_id) -- has PK
and [name] NOT LIKE '[_][_]%'
and [name] NOT IN ('sysdiagrams')
AND schema_id = 1 -- dbo schema only (comment this line to output for all schemas)
ORDER BY [name]