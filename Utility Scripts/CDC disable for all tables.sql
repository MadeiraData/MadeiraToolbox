-- Disable quickly at the database level:

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
EXEC sys.sp_cdc_disable_db;

GO

-- Generate disable commands for all tables:

SELECT
  SCHEMA_NAME(schema_id) AS [schema], [name], is_tracked_by_cdc
, RemediationCmd = N'EXEC sys.sp_cdc_disable_table @source_schema = N' + QUOTENAME(SCHEMA_NAME(t.schema_id), '''') + N', @source_name = N' + QUOTENAME(t.[name], '''') + N', @capture_instance = N' + QUOTENAME(ct.capture_instance, '''') + N';' 
FROM sys.tables AS t
INNER JOIN cdc.change_tables AS ct ON t.object_id = ct.source_object_id
WHERE is_ms_shipped = 0 -- non-system
AND t.is_tracked_by_cdc = 1 -- tracked by CDC
ORDER BY [name]